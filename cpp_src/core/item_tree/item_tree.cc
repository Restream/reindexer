#include "item_tree.h"
#include <type_traits>
#include <variant>
#include "core/cjson/tagsmatcher.h"
#include "core/cjson/tagspath.h"
#include "core/enums.h"
#include "core/index/index.h"
#include "core/item_tree/intermediate_indexes_data.h"
#include "core/key_value_type.h"
#include "core/keyvalue/variant.h"
#include "core/payload/payloadiface.h"
#include "core/payload/payloadtype.h"
#include "core/payload/payloadvalue.h"
#include "core/tag_name_index.h"
#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "estl/overloaded.h"
#include "tools/assertrx.h"
#include "tools/errors.h"
namespace reindexer {

TreeNode& TreeNode::AddObject(TagName key) {
	return std::visit(overloaded{
						  [&](ObjectNode* object) -> TreeNode& {
							  TreeNode* node = creator_.CreateObject(this, key.AsNumber(), data_.contextIndex_);
							  return object->Push(key, *node);
						  },
						  [&](ArrayNode* array) -> TreeNode& {
							  if (key != TagName::Empty()) {
								  throw Error(errLogic, "Cannot add a named object element to an array");
							  }
							  TreeNode* node = creator_.CreateObject(this, array->Size(), data_.contextIndex_);
							  array->PushBack(*node);
							  return *node;
						  },
						  [](auto) -> TreeNode& { throw Error(errLogic, "Cannot add an object to a non-container node"); },
					  },
					  data_.value_);
}

TreeNode& TreeNode::AddArray(TagName key, uint32_t count) {
	return std::visit(overloaded{
						  [&](ObjectNode* object) -> TreeNode& {
							  TreeNode* node = creator_.CreateArray(count, this, key.AsNumber(), data_.contextIndex_);
							  return object->Push(key, *node);
						  },
						  [&](ArrayNode* array) -> TreeNode& {
							  if (key != TagName::Empty()) {
								  throw Error(errLogic, "Cannot add a named array element to an array");
							  }
							  TreeNode* node = creator_.CreateArray(count, this, array->Size(), data_.contextIndex_);
							  array->PushBack(*node);
							  return *node;
						  },
						  [](auto) -> TreeNode& { throw Error(errLogic, "Cannot add an array to a non-container node"); },
					  },
					  data_.value_);
}

TreeNode& TreeNode::AddValue(TagName key, Variant&& val, unsigned int index, bool isUpdated) {
	return std::visit(overloaded{
						  [&](ObjectNode* object) -> TreeNode& {
							  if (isUpdated && index > 0) {
								  std::ignore = val.convert(data_.contextIndex_.pt_.Field(index).Type());
							  }
							  TreeNode* node = creator_.CreateValue(std::move(val), index, this, key.AsNumber(), data_.contextIndex_);
							  return object->Push(key, *node);
						  },
						  [&](ArrayNode* array) -> TreeNode& {
							  if (key != TagName::Empty()) {
								  throw Error(errLogic, "Cannot add a named value element to an array");
							  }
							  TreeNode* node = creator_.CreateValue(std::move(val), index, this, array->Size(), data_.contextIndex_);
							  array->PushBack(*node);
							  return *node;
						  },
						  [](auto) -> TreeNode& { throw Error(errLogic, "Cannot add a value to a non-container node"); },
					  },
					  data_.value_);
}

TagsPath TreeNode::GetNodePath() const {
	TagsPath path;
	const TreeNode* cur = this;
	do {
		if (cur->nodeParent_ != nullptr && cur->nodeParent_->IsType<ObjectNode>()) {
			path.emplace_back(cur->parentId_);
		}
		cur = cur->nodeParent_;
	} while (cur != nullptr);
	std::reverse(path.begin(), path.end());
	return path;
}

IndexedTagsPath TreeNode::GetNodeIndexedPath() const {
	IndexedTagsPath path;
	const TreeNode* cur = this;
	while (cur->nodeParent_) {
		if (cur->nodeParent_->IsType<ObjectNode>()) {
			path.emplace_back(TagName{cur->parentId_});
		} else if (cur->nodeParent_->IsType<ArrayNode>()) {
			path.emplace_back(TagIndex{cur->parentId_});
		}
		cur = cur->nodeParent_;
	}
	std::reverse(path.begin(), path.end());
	return path;
}

void TreeNode::UpdateValue(Variant val) {
	auto* valueNode = GetIf<ValueNode>();
	if (!valueNode) {
		throw Error(errLogic, "Update value not supported node");
	}
	if (data_.index_ > 0) {
		if (val.IsNullValue() && !data_.contextIndex_.indexes_[data_.index_]->Opts().IsArray()) [[unlikely]] {
			throw Error(errParams, "Non-array index fields cannot be set to null!");
		}
		std::ignore = val.convert(data_.contextIndex_.pt_.Field(data_.index_).Type());
	}
	KeyValueType arrValType{KeyValueType::Undefined{}};
	auto* parentArray = nodeParent_ ? nodeParent_->GetIf<ArrayNode>() : nullptr;
	if (parentArray) {
		if (parentArray->Homogeneous() && parentArray->Size() >= 1) {
			if (auto* firstValue = parentArray->Values()[0]->GetIf<ValueNode>()) {
				arrValType = firstValue->Value().Type();
			}
		}
	}
	auto valType = val.Type();
	valueNode->SetValue(std::move(val));
	if (parentArray && !arrValType.IsSame(valType)) {
		MarkParentArraysForNormalization();
	}
}

void TreeNode::UpdateArray(VariantArray& newValues) {
	auto* arr = GetIf<ArrayNode>();
	if (!arr) {
		throw Error(errLogic, "Cannot update array elements on a non-array node");
	}

	TagsPath path = GetNodePath();
	FieldProperties fp = data_.contextIndex_.tm_.tags2field(path);

	if (arr->Size() == 0) {
		if (data_.index_ > 0) {
			data_.contextIndex_.indexData_.SaveEmptyOldIndexData(data_.index_);
		} else if (fp.IsSparseIndex()) {
			const auto& sparseIndexData = data_.contextIndex_.tm_.SparseIndex(fp.SparseNumber());
			int indexId = data_.contextIndex_.getIndexByName_(sparseIndexData.name);
			data_.contextIndex_.indexData_.SaveEmptyOldIndexData(indexId);
		}
	}
	arr->Clear();

	if (newValues.empty()) {
		if (fp.IsSparseIndex()) {
			arr->MarkForNormalization();
			MarkParentArraysForNormalization();
			return;
		}
	}
	for (auto v : newValues) {
		if (v.Type().Is<KeyValueType::Tuple>()) {
			VariantArray nestedValues = v.As<VariantArray>();
			TreeNode& nestedNode = AddArray(TagName::Empty(), nestedValues.size());
			nestedNode.UpdateArray(nestedValues);
			continue;
		}

		int regularArrayIndex = data_.index_;
		if (regularArrayIndex == 0 && fp.IsRegularIndex()) {
			const int index = fp.IndexNumber();
			if (!data_.contextIndex_.indexes_[index]->IsFloatVector() && data_.contextIndex_.indexes_[index]->Opts().IsArray()) {
				regularArrayIndex = index;
			}
		}
		if (regularArrayIndex > 0) {
			if (v.IsNullValue()) {
				const auto& ft = data_.contextIndex_.pt_.Field(regularArrayIndex);
				throw Error(errParams, "Field '{}' type mismatch. Passed '{}', expected '{}'", ft.Name(), v.Type().Name(),
							ft.Type().Name());
			}
			std::ignore = v.convert(data_.contextIndex_.pt_.Field(regularArrayIndex).Type());
			TreeNode* tn = creator_.CreateValue(std::move(v), 0, this, arr->Size(), data_.contextIndex_);
			arr->PushBack(*tn);

		} else {
			if (fp.IsSparseIndex()) {
				const auto& sparseIndexData = data_.contextIndex_.tm_.SparseIndex(fp.SparseNumber());
				int indexId = data_.contextIndex_.getIndexByName_(sparseIndexData.name);
				std::ignore = v.convert(data_.contextIndex_.indexes_[indexId]->KeyType());
				TreeNode* tn = creator_.CreateValue(std::move(v), 0, this, arr->Size(), data_.contextIndex_);
				arr->PushBack(*tn);

			} else {
				TreeNode* tn = creator_.CreateValue(std::move(v), 0, this, arr->Size(), data_.contextIndex_);
				arr->PushBack(*tn);
			}
		}
	}
	arr->MarkForNormalization();
	MarkParentArraysForNormalization();
}

void TreeNode::MarkParentArraysForNormalization() {
	for (TreeNode* node = nodeParent_; node != nullptr; node = node->nodeParent_) {
		if (auto* array = node->GetIf<ArrayNode>()) {
			array->MarkForNormalization();
		}
	}
}

void TreeNode::MarkArraysForNormalization() {
	if (auto* object = GetIf<ObjectNode>()) {
		for (const auto& [child, tag] : *object) {
			(void)tag;
			if (child) {
				child->MarkArraysForNormalization();
			}
		}
		return;
	}
	if (auto* array = GetIf<ArrayNode>()) {
		array->MarkForNormalization();
		for (TreeNode* child : array->Values()) {
			child->MarkArraysForNormalization();
		}
	}
}

void TreeNode::BindToIndex(int index) {
	if (IsType<ValueNode>()) {
		SetIndex(index);
		return;
	}
	if (auto* array = GetIf<ArrayNode>()) {
		bool isFlat = array->Homogeneous();
		for (const TreeNode* child : array->Values()) {
			if (!child->IsType<ValueNode>()) {
				isFlat = false;
				break;
			}
		}
		if (isFlat) {
			SetIndex(index);
			for (TreeNode* child : array->Values()) {
				child->SetIndex(index);
			}
		} else {
			SetIndex(0);
			array->SetHomogeneous(false);
			for (TreeNode* child : array->Values()) {
				child->BindToIndex(index);
			}
		}
		return;
	}
	throw Error(errParams, "Unable to bind object node to regular index '{}'", data_.contextIndex_.indexes_[index]->Name());
}

void TreeNode::NormalizeCurrentArray(ArrayNode& array) {
	bool allValues = true;
	bool sameType = true;
	KeyValueType type{KeyValueType::Undefined{}};
	for (const TreeNode* child : array.Values()) {
		if (!child->IsType<ValueNode>()) {
			allValues = false;
			break;
		}
		if (type.Is<KeyValueType::Undefined>()) {
			type = child->Value().Type();
		} else if (!child->Value().Type().IsSame(type)) {
			sameType = false;
		}
	}

	const FieldProperties field = data_.contextIndex_.tm_.tags2field(GetNodePath());
	if (field.IsRegularIndex()) {
		const int index = field.IndexNumber();
		const Index& idx = *data_.contextIndex_.indexes_[index];
		if (idx.IsFloatVector()) {
			return;
		}
		if (idx.Opts().IsArray()) {
			if (allValues && sameType) {
				SetIndex(index);
				array.SetHomogeneous(true);
				for (TreeNode* child : array.Values()) {
					child->SetIndex(index);
				}
			} else {
				for (TreeNode* child : array.Values()) {
					child->BindToIndex(index);
				}
				SetIndex(0);
				array.SetHomogeneous(false);
			}
			return;
		}
	}

	bool hasIndexRefs = false;
	for (const TreeNode* child : array.Values()) {
		if (child->GetIndex() > 0) {
			hasIndexRefs = true;
			break;
		}
	}
	if (allValues && sameType && !hasIndexRefs) {
		SetIndex(0);
		array.SetHomogeneous(true);
	} else {
		array.SetHomogeneous(false);
	}
}

void TreeNode::NormalizeMarkedArrays() {
	if (auto* object = GetIf<ObjectNode>()) {
		for (const auto& [child, tag] : *object) {
			(void)tag;
			if (child) {
				child->NormalizeMarkedArrays();
			}
		}
		return;
	}
	auto* array = GetIf<ArrayNode>();
	if (!array) {
		return;
	}

	for (TreeNode* child : array->Values()) {
		child->NormalizeMarkedArrays();
	}
	if (array->NeedsNormalization()) {
		NormalizeCurrentArray(*array);
		array->ResetNormalizationFlag();
	}
}

void TreeNode::Drop(int id) {
	if (auto* arr = GetIf<ArrayNode>()) {
		arr->Erase(id);
		arr->MarkForNormalization();
		MarkParentArraysForNormalization();
		auto& values = arr->Values();
		for (size_t i = id; i < values.size(); i++) {
			values[i]->parentId_--;
		}
	} else {
		assertrx_throw(false);
	}
}

void TreeNode::Drop(TagName id) {
	if (auto* obj = GetIf<ObjectNode>()) {
		obj->Erase(id);
	} else {
		assertrx_throw(false);
	}
}

void TreeNode::Dump(std::string& outPut, const TagsMatcher& tm) const {
	std::visit(
		overloaded{[&](ObjectNode* object) {
					   outPut += '{';
					   bool first = true;
					   for (const auto& [node, tag] : *object) {
						   if (!node) {
							   continue;
						   }
						   if (!first) {
							   outPut += ',';
						   }
						   first = false;
						   outPut += '"';
						   if (data_.index_ > 0) {
							   outPut += '@';
						   }
						   outPut += tm.tag2name(tag);
						   outPut += "\":";
						   node->Dump(outPut, tm);
					   }
					   outPut += '}';
				   },
				   [&](ArrayNode* array) {
					   if (data_.index_ > 0) {
						   outPut += '@';
					   }
					   outPut += '[';
					   bool first = true;
					   for (const TreeNode* node : array->Values()) {
						   if (!first) {
							   outPut += ',';
						   }
						   first = false;
						   node->Dump(outPut, tm);
					   }
					   outPut += ']';
				   },
				   [&](ValueNode* valueNode) {
					   const Variant& value = valueNode->Value();
					   if (data_.index_ > 0) {
						   outPut += '@';
					   }
					   value.Type().EvaluateOneOf(
						   [&](KeyValueType::Bool) { outPut += value.As<bool>() ? "true" : "false"; },
						   [&](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float> auto) {
							   outPut += value.As<std::string>();
						   },
						   [&](concepts::OneOf<KeyValueType::String, KeyValueType::Uuid> auto) {
							   outPut += '"';
							   outPut += value.As<std::string>();
							   outPut += '"';
						   },
						   [&](KeyValueType::Null) { outPut += "null"; },
						   [](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::Composite> auto) {},
						   [&](KeyValueType::FloatVector) {
							   const ConstFloatVectorView fv{value};
							   outPut += '"';
							   outPut += "FloatVector ";
							   outPut += std::to_string(fv.Dimension().Value());
							   if (!fv.IsStripped()) {
								   outPut += " Data ";
								   if (!fv.IsEmpty()) {
									   const auto* data = fv.Data();
									   for (int i = 0; i < fv.Dimension().Value(); ++i) {
										   outPut += std::to_string(data[i]);
										   outPut += ' ';
									   }
								   }
							   }
							   outPut += '"';
						   });
				   }},
		data_.value_);
}

void TreeNode::SaveOldIndexDataForSubtree(IntermediateIndexesData& indexData, ConstPayload& plSave, TreeNode& root) const {
	if (auto* obj = GetIf<ObjectNode>()) {
		for (const auto& p : *obj) {
			if (p.first) {
				p.first->SaveOldIndexDataForSubtree(indexData, plSave, root);
			}
		}
	} else if (auto* arr = GetIf<ArrayNode>()) {
		saveOldIndexData(indexData, plSave, root);
		for (const auto& p : arr->Values()) {
			p->SaveOldIndexDataForSubtree(indexData, plSave, root);
		}
	} else if (IsType<ValueNode>()) {
		saveOldIndexData(indexData, plSave, root);
	} else {
		assertrx_throw(false);
	}
}

bool TreeNode::IsArrayIndex() const {
	int index = GetIndex();
	if (index <= 0) {
		index = IsSparseIndex();
	}
	return index > 0 && data_.contextIndex_.indexes_[index]->Opts().IsArray();
}

void TreeNode::ChangeToArray() { changeType(creator_.GetArrayNode()); }
void TreeNode::ChangeToObject() { changeType(creator_.GetObjectNode()); }
void TreeNode::ChangeToValue() { changeType(creator_.GetValueNode()); }

bool TreeNode::ContainsMultidimensionalArray(const TagsPath& tp) const {
	TagsPathCursor cursor{*this, tp};
	for (const TreeNode* n = cursor.Next(); n != nullptr; n = cursor.Next()) {
		IndexedTagsPath path = n->GetNodeIndexedPath();
		bool prevArray = false;
		for (auto v : path) {
			if (v.IsTagIndex()) {
				if (!prevArray) {
					prevArray = true;
				} else {
					return true;
				}
			} else if (v.IsTagName()) {
				prevArray = false;
			}
		}
	}
	return false;
}

void TreeNode::GetCJSON(WrSerializer& wrser, Payload& pl, TagName tagName, const PayloadValue& pvSave,
						const IntermediateIndexesData& indexData, std::array<IndexedArrayData, kMaxIndexes>& arrayBuf) const {
	for (auto& k : arrayBuf) {
		k.Reset();
	}

	std::bitset<kMaxIndexes> indexSaved;
	indexSaved.set(0);
	getCJSONInternal(wrser, pl, tagName, arrayBuf, indexSaved);
	for (unsigned i = 0; i < arrayBuf.size(); ++i) {
		if (arrayBuf[i].isPresent) {
			pl.Set(i, arrayBuf[i].values);
			indexSaved.set(i);
		}
	}

	if (pl.Type().NumFields() != int(indexSaved.count())) {
		ConstPayload plSave(pl.Type(), pvSave);
		for (int i = 1; i < pl.Type().NumFields(); i++) {
			if (!indexSaved.test(i)) {
				if (indexData.IsIndexChanged(i)) {
					pl.Set(i, indexData.IndexDataValueNew(i));
				} else {
					VariantArray v;
					plSave.Get(i, v);
					pl.Set(i, v);
				}
			}
		}
	}
}

void TreeNode::PutVariant(const Variant& kv, WrSerializer& wrSer) const {
	kv.Type().EvaluateOneOf(
		[&](KeyValueType::Tuple) { assertrx(false); }, [&](KeyValueType::Bool) { wrSer.PutBool(bool(kv)); },
		[&](KeyValueType::Int64) { wrSer.PutVarint(int64_t(kv)); }, [&](KeyValueType::Int) { wrSer.PutVarint(int(kv)); },
		[&](KeyValueType::Double) { wrSer.PutDouble(double(kv)); }, [&](KeyValueType::Float) { wrSer.PutFloat(float(kv)); },
		[&](KeyValueType::String) { wrSer.PutVString(std::string_view(kv)); }, [&](KeyValueType::Null) noexcept {},
		[&](KeyValueType::Uuid) { wrSer.PutUuid(Uuid{kv}); }, [&](KeyValueType::FloatVector) { assertrx(false); },
		[&](concepts::OneOf<KeyValueType::Composite, KeyValueType::Undefined> auto) {
			throw Error(errLogic, "Unknown key type '{}'", kv.Type().Name());
		});
}

void TreeNode::getCJSONInternal(WrSerializer& wrser, Payload& pl, TagName tagName, std::array<IndexedArrayData, kMaxIndexes>& arrayBuf,
								std::bitset<kMaxIndexes>& indexLoaded) const {
	std::visit(overloaded{[&](ObjectNode* o) {
							  ctag object{TAG_OBJECT, tagName};
							  wrser.PutCTag(object);
							  for (auto& v : *o) {
								  if (!v.first) {
									  continue;
								  }
								  v.first->getCJSONInternal(wrser, pl, v.second, arrayBuf, indexLoaded);
							  }
							  ctag tagEnd{TAG_END, TagName::Empty()};
							  wrser.PutCTag(tagEnd);
						  },
						  [&](ArrayNode* a) {
							  if (data_.index_ > 0) {
								  wrser.PutCTag({TAG_ARRAY, tagName, data_.index_});
								  wrser.PutVarUint(a->Size());
								  assertrx(a->Homogeneous());
								  arrayBuf[data_.index_].isPresent = true;
								  for (auto& tn : a->Values()) {
									  arrayBuf[data_.index_].values.emplace_back(tn->Value());
								  }

							  } else {
								  wrser.PutCTag({TAG_ARRAY, tagName});
								  const auto size = a->Size();
								  if (size == 0) {
									  wrser.PutCArrayTag(carraytag{0, TAG_OBJECT});
								  } else if (a->Homogeneous()) {
									  const auto type = a->Values()[0]->Value().Type().ToTagType();
									  wrser.PutCArrayTag(carraytag{uint32_t(size), type});
									  for (const TreeNode* node : a->Values()) {
										  PutVariant(node->Value(), wrser);
									  }
								  } else {
									  wrser.PutCArrayTag(carraytag{uint32_t(size), TAG_OBJECT});
									  for (const TreeNode* node : a->Values()) {
										  node->getCJSONInternal(wrser, pl, TagName::Empty(), arrayBuf, indexLoaded);
									  }
								  }
							  }
						  },
						  [&](ValueNode* val) {
							  const Variant& v = val->Value();
							  if (v.IsNullValue() && IsArrayIndex()) {
								  if (data_.index_ > 0) {
									  arrayBuf[data_.index_].isPresent = true;
								  }
								  wrser.PutCTag(ctag{TAG_NULL, tagName});
								  return;
							  }
							  if (data_.index_ > 0) {
								  if (pl.Type().Field(data_.index_).IsArray()) {  // scalar in cjson, but index isarray==true
									  if (!v.IsNullValue()) {
										  arrayBuf[data_.index_].values.emplace_back(v);
										  arrayBuf[data_.index_].isPresent = true;
									  } else {
										  arrayBuf[data_.index_].isPresent = true;
									  }
								  } else {
									  pl.Set(data_.index_, v);
									  indexLoaded.set(data_.index_);
								  }
								  if (v.Type().Is<KeyValueType::FloatVector>()) {
									  wrser.PutCTag(ctag{TAG_ARRAY, tagName, int(data_.index_)});
									  ConstFloatVectorView cfvv = ConstFloatVectorView(v);
									  wrser.PutVarUint(cfvv.Dimension().Value());
								  } else {
									  TagType tt = v.Type().ToTagType();
									  wrser.PutCTag(ctag{tt, tagName, int(data_.index_)});
								  }
							  } else {
								  wrser.PutCTag(ctag{v.Type().ToTagType(), tagName});
								  PutVariant(v, wrser);
							  }
						  }},
			   data_.value_);
}

TreeNode& ObjectNode::GetOrCreate(TagName n, TreeNode& parent, ItemsTreeContext& ic) {
	if (TreeNode* node = Get(n)) {
		return *node;
	}
	TreeNode& node = parent.AddObject(n);
	TagsPath tp = node.GetNodePath();
	int index = ic.tm_.tags2field(tp).IndexNumber();
	if (index > 0) {
		node.SetIndex(index);
	}
	return node;
}

template <concepts::OneOf<VariantArray, IntermediateIndexesData::IndexNodeData, h_vector<TreeNode*, 4>> V>
void GetNextIndexData(std::span<const TagName> p, TreeNode& cur, V& data, IsSparse isSparse, KeyValueType type, NullAllowed allowNull,
					  const ConstPayload& plSave) {
	std::visit(overloaded{[&](ObjectNode* object) {
							  if (p.empty()) {
								  // An object at the end of the path contributes no scalar index values.
								  // {"items": [10, {}, 20]}
								  return;
							  }
							  if (TreeNode* node = object->Get(p.front()); node) {
								  GetNextIndexData(p.subspan(1), *node, data, isSparse, type, allowNull, plSave);
							  }
						  },
						  [&](ArrayNode* array) {
							  if constexpr (std::is_same_v<IntermediateIndexesData::IndexNodeData, V>) {
								  if (p.empty() && allowNull) {
									  std::ignore = data.Vals.MarkArray();
								  }
							  }
							  for (TreeNode* node : array->Values()) {
								  GetNextIndexData(p, *node, data, isSparse, type, allowNull, plSave);
							  }
						  },
						  [&](ValueNode* value) {
							  if (!p.empty()) {
								  // A scalar cannot contain the remaining path, so it contributes no index values.
								  // path=items.price item={"items": [{"price": 10}, 42, {"price": 20}]}
								  return;
							  }
							  if (value->IsValueSet()) {
								  if (value->Value().IsNullValue() && cur.IsArrayIndex() && !allowNull) {
									  return;
								  }
								  Variant v = value->Value();
								  if (isSparse) {
									  std::ignore = v.convert(type);
								  }
								  if constexpr (std::is_same_v<VariantArray, V>) {
									  data.emplace_back(std::move(v));
								  } else if constexpr (std::is_same_v<IntermediateIndexesData::IndexNodeData, V>) {
									  data.Vals.emplace_back(std::move(v));
									  data.Nodes.emplace_back(&cur);
								  } else {
									  data.emplace_back(&cur);
								  }
								  return;
							  }

							  const int indexNum = cur.GetIndex();
							  // The node has just been created and assigned an index number, but its value has not been set yet.
							  // We need to collect the old values to remove them from the index.
							  if (indexNum > 0) {
								  VariantArray vals;
								  plSave.Get(indexNum, vals);
								  assertrx_throw(vals.size() == 1);
								  if constexpr (std::is_same_v<VariantArray, V>) {
									  data.emplace_back(vals[0]);
								  } else if constexpr (std::is_same_v<IntermediateIndexesData::IndexNodeData, V>) {
									  data.Vals.emplace_back(vals[0]);
									  data.Nodes.emplace_back(&cur);
								  } else {
									  data.emplace_back(&cur);
								  }
							  }
						  }},
			   cur.data_.value_);
}

template <concepts::OneOf<VariantArray, IntermediateIndexesData::IndexNodeData, h_vector<TreeNode*, 4>> V>
V GetIndexData(TreeNode& root, const Index& index, const TagsMatcher& tm, const ConstPayload& plSave) {
	const IsSparse isSparse = index.Opts().IsSparse();
	const NullAllowed allowNull{index.Type() == IndexRTree};
	V data;
	const auto& fields = index.Fields();
	size_t tagsPathIdx = 0;
	for (const auto& f : fields) {
		if (f == SetByJsonPath) {
			const auto& path = fields.getFieldsPath(tagsPathIdx++);
			const auto& tagsPath = std::get<TagsPath>(path);
			std::span<const TagName> pp{tagsPath};
			GetNextIndexData(pp, root, data, isSparse, index.KeyType(), allowNull, plSave);
		} else {
			const auto& paths = index.GetPayloadType().Field(f).JsonPaths();
			for (const auto& jsonPath : paths) {
				TagsPath path = tm.path2tag(jsonPath);
				std::span<const TagName> pp{path};
				GetNextIndexData(pp, root, data, IsSparse_False, KeyValueType::Null{}, allowNull, plSave);
			}
		}
	}
	return data;
}

template VariantArray GetIndexData<VariantArray>(TreeNode&, const Index&, const TagsMatcher&, const ConstPayload&);

template IntermediateIndexesData::IndexNodeData GetIndexData<IntermediateIndexesData::IndexNodeData>(TreeNode&, const Index&,
																									 const TagsMatcher&,
																									 const ConstPayload&);

template h_vector<TreeNode*, 4> GetIndexData<h_vector<TreeNode*, 4>>(TreeNode&, const Index&, const TagsMatcher&, const ConstPayload&);

void NextByPath(std::span<const IndexedPathNode> path, const TreeNode& node, VariantArray& data) {
	std::visit(overloaded{[&](ObjectNode* object) {
							  if (path.empty()) {
								  throw Error(errLogic, "Cannot extract scalar values from an object node");
							  }
							  if (!path.front().IsTagName()) {
								  throw Error(errLogic, "Expected a field name when traversing an object node, got an array index");
							  }
							  if (const TreeNode* child = object->Get(path.front().GetTagName())) {
								  NextByPath(path.subspan(1), *child, data);
							  }
						  },
						  [&](ArrayNode* array) {
							  const auto& children = array->Values();
							  if (path.empty()) {
								  for (const TreeNode* child : children) {
									  NextByPath(path, *child, data);
								  }
								  return;
							  }

							  const auto& tag = path.front();
							  if (!tag.IsTagIndex()) {
								  throw Error(errLogic, "Expected an array index when traversing an array node, got a field name");
							  }

							  const auto remaining = path.subspan(1);
							  if (tag.IsTagIndexNotAll()) {
								  const size_t index = tag.GetTagIndex().AsNumber();
								  if (index >= children.size()) {
									  throw Error(errLogic, "Array index {} is out of range for an array of size {}", index,
												  children.size());
								  }
								  NextByPath(remaining, *children[index], data);
							  } else {
								  for (const TreeNode* child : children) {
									  NextByPath(remaining, *child, data);
								  }
							  }
						  },
						  [&](ValueNode* value) {
							  if (!path.empty()) {
								  return;
							  }
							  if (const TreeNode* parent = node.Parent(); parent && parent->IsType<ArrayNode>()) {
								  std::ignore = data.MarkArray(true);
							  }
							  data.emplace_back(value->Value());
						  }},
			   node.data_.value_);
}

VariantArray GetDataByPath(std::string_view path, const TreeNode& root, const TagsMatcher& tm) {
	IndexedTagsPath indexedTagsPath = tm.path2indexedtag(path);
	VariantArray data;
	if (indexedTagsPath.empty()) {
		return data;
	}
	NextByPath(indexedTagsPath, root, data);
	return data;
}

TreeNode* UpdatePathCursor::findFirst(TreeNode& cur, std::span<const IndexedPathNode> path) {
	if (path.empty()) {
		throw Error(errLogic, "Cannot find a node to update: path is empty");
	}
	const auto& pathElement = path.front();
	if (pathElement.IsTagName()) {
		if (cur.IsType<ArrayNode>()) {
			throw Error(errParams, "Incorrect path, node is array, not object");
		} else if (cur.IsType<ValueNode>()) {
			if (cur.GetIndex() > 0) {
				throw Error(errParams, "Can't change type 'value' to 'object' for index node");
			} else if (cur.IsSparseIndex()) {
				throw Error(errParams, "Can't change type 'value' to 'object' for sparse index node");
			}
			cur.ChangeToObject();
		}

		auto* obj = std::get<ObjectNode*>(cur.data_.value_);
		TreeNode& node = obj->GetOrCreate(pathElement.GetTagName(), cur, ci_);
		if (path.size() == 1) {
			return &node;
		} else {
			return findFirst(node, path.subspan(1));
		}

	} else if (pathElement.IsTagIndex()) {
		auto* arr = cur.GetIf<ArrayNode>();
		if (!arr) {
			throw Error(errParams, "Can't update non-array fields using array index");
		}
		if (pathElement.IsTagIndexNotAll()) {
			uint32_t index = pathElement.GetTagIndex().AsNumber();
			if (index >= arr->Size()) {
				return nullptr;
			}
			TreeNode* n = arr->Values()[index];
			if (path.size() == 1) {
				return n;
			} else {
				return findFirst(*n, path.subspan(1));
			}
		} else {
			iterable_ = true;
			for (TreeNode* node : arr->Values()) {
				if (path.size() == 1) {
					return node;
				}
				if (TreeNode* result = findFirst(*node, path.subspan(1))) {
					return result;
				}
			}
			return nullptr;
		}
	} else {
		assertrx_throw(false);
	}
	return nullptr;
}

template <typename Node, typename First>
Node* getNextNodeByIndexedPath(Node* cur, std::span<const IndexedPathNode> path, bool iterable, First&& first) {
	if (!iterable) {
		return nullptr;
	}
	size_t pathCounter = 1;
	Node* child = cur;
	Node* parent = cur->Parent();
	while (parent != nullptr) {
		if (parent->template IsType<ArrayNode>()) {
			assertrx_throw(pathCounter <= path.size());
			const auto subPath = path.subspan(path.size() - pathCounter);
			assertrx_throw(subPath.front().IsTagIndex());
			if (!subPath.front().IsTagIndexNotAll()) {
				const auto& values = parent->Array();
				for (size_t i = size_t(child->ParentIndex()) + 1; i < values.size(); ++i) {
					if (subPath.size() == 1) {
						return values[i];
					}
					if (Node* result = first(*values[i], subPath.subspan(1))) {
						return result;
					}
				}
			}
		}
		++pathCounter;
		child = parent;
		parent = parent->Parent();
	}
	return nullptr;
}

TreeNode* UpdatePathCursor::Next() {
	if (!started_) {
		started_ = true;
		current_ = findFirst(root_, path_);
	} else if (current_) {
		current_ = getNextNodeByIndexedPath(
			current_, path_, iterable_, [&](TreeNode& node, std::span<const IndexedPathNode> subPath) { return findFirst(node, subPath); });
	}
	return current_;
}

template <concepts::OneOf<TreeNode, const TreeNode> Node>
Node* ExistingPathCursor<Node>::findFirst(Node& node, std::span<const IndexedPathNode> path) {
	if (path.empty()) {
		throw Error(errLogic, "Cannot find an existing node: path is empty");
	}
	const auto& pathElement = path.front();
	if (pathElement.IsTagName()) {
		if (!node.template IsType<ObjectNode>()) {
			return nullptr;
		}
		Node* nodeChild = node.Object(pathElement.GetTagName());
		if (nodeChild == nullptr) {
			return nullptr;
		}
		if (path.size() == 1) {
			return nodeChild;
		} else {
			return findFirst(*nodeChild, path.subspan(1));
		}

	} else if (pathElement.IsTagIndex()) {
		if (!node.template IsType<ArrayNode>()) {
			return nullptr;
		}
		const auto& values = node.Array();
		if (pathElement.IsTagIndexNotAll()) {
			uint32_t index = pathElement.GetTagIndex().AsNumber();
			if (index >= values.size()) {
				return nullptr;
			}
			Node* n = values[index];
			if (path.size() == 1) {
				return n;
			} else {
				return findFirst(*n, path.subspan(1));
			}
		} else {
			iterable_ = true;
			for (TreeNode* value : values) {
				Node* child = value;
				if (path.size() == 1) {
					return child;
				}
				if (Node* result = findFirst(*child, path.subspan(1))) {
					return result;
				}
			}
			return nullptr;
		}
	} else {
		assertrx_throw(false);
	}
	return nullptr;
}

template <concepts::OneOf<TreeNode, const TreeNode> Node>
Node* ExistingPathCursor<Node>::Next() {
	if (!started_) {
		started_ = true;
		current_ = findFirst(root_, path_);
	} else if (current_) {
		current_ = getNextNodeByIndexedPath(current_, path_, iterable_,
											[&](Node& node, std::span<const IndexedPathNode> subPath) { return findFirst(node, subPath); });
	}
	return current_;
}

template class ExistingPathCursor<TreeNode>;
template class ExistingPathCursor<const TreeNode>;

TreeNode* GetRootVectorIndexNodeImpl(TreeNode& cur, std::span<TagName> path, IsArray isArray) {
	if (path.empty()) {
		if (cur.IsType<ValueNode>()) {
			auto& value = cur.GetValueNode();
			if (value.Value().IsNullValue()) {
				if (isArray) {
					cur.ChangeToArray();
				}
				return &cur;
			} else {
				if (isArray) {
					if (cur.Parent()->IsType<ArrayNode>()) {
						cur.Parent()->Clear();
						return cur.Parent();
					} else {
						throw Error(errLogic, "Expected an array parent for a non-null value node in an array vector index");
					}
				} else {
					return &cur;
				}
			}
		} else if (cur.IsType<ArrayNode>()) {
			if (isArray) {
				auto& a = cur.Array();
				if (a.size()) {
					return GetRootVectorIndexNodeImpl(*a[0], path, isArray);
				} else {
					return &cur;
				}
			} else {
				throw Error(errLogic, "Expected a value node for a non-array vector index, got an array node");
			}
		} else {  // ObjectNode
			if (isArray) {
				cur.ChangeToArray();
				return &cur;
			} else {
				cur.ChangeToValue();
				return &cur;
			}
		}
	} else {
		if (cur.IsType<ObjectNode>()) {
			TreeNode* obj = cur.Object(path[0]);
			if (obj) {
				return GetRootVectorIndexNodeImpl(*obj, path.subspan(1), isArray);
			} else {
				TreeNode& obj = cur.AddObject(path[0]);
				return GetRootVectorIndexNodeImpl(obj, path.subspan(1), isArray);
			}
		} else if (cur.IsType<ArrayNode>()) {
			auto& a = cur.Array();
			if (a.size() < 1) {
				throw Error(errLogic, "Cannot resolve vector index path through an empty array: {} path segment(s) remaining", path.size());
			}
			return GetRootVectorIndexNodeImpl(*a[0], path, isArray);
		} else {
			throw Error(errLogic, "Cannot resolve vector index path through a non-container node: {} path segment(s) remaining",
						path.size());
		}
	}
	return nullptr;
}

TreeNode* GetRootVectorIndexNode(TreeNode& root, const Index& index, int indexNum, const TagsMatcher& tm) {
	const auto& fieldSet = index.Fields();
	if (fieldSet.empty()) {
		throw Error(errLogic, "Incorrect embedding config");
	}
	auto& f = fieldSet[0];
	TagsPath path;
	if (f == SetByJsonPath) {
		size_t pathCount = fieldSet.getTagsPathsLength();
		if (pathCount < 1) {
			throw Error(errLogic, "Incorrect embedding config. Path count < 1 ");
		}
		const auto& p = fieldSet.getFieldsPath(0);
		path = std::get<TagsPath>(p);

	} else {
		const auto& paths = index.GetPayloadType().Field(indexNum).JsonPaths();
		if (paths.size() < 1) {
			throw Error(errLogic, "Incorrect embedding config. Path count < 1 ");
		}
		path = tm.path2tag(paths[0]);
	}
	return GetRootVectorIndexNodeImpl(root, path, index.Opts().IsArray());
}

const TreeNode* TagsPathCursor::findNext() {
	if (!iterable_) {
		return nullptr;
	}

	size_t pathCounter = 1;
	const TreeNode* child = current_;
	const TreeNode* parent = current_->Parent();

	while (parent != nullptr) {
		if (parent->IsType<ArrayNode>()) {
			assertrx_throw(pathCounter <= path_.size());
			const auto subPath = path_.subspan(path_.size() - pathCounter);
			assertrx_throw(!subPath.empty());
			const auto& values = parent->Array();
			for (size_t i = size_t(child->ParentIndex()) + 1; i < values.size(); ++i) {
				if (const TreeNode* result = findFirst(*values[i], subPath.subspan(1))) {
					return result;
				}
			}
		} else {
			++pathCounter;
		}

		child = parent;
		parent = parent->Parent();
	}

	return nullptr;
}

const TreeNode* TagsPathCursor::findFirst(const TreeNode& cur, std::span<const TagName> path) {
	if (path.empty()) {
		if (cur.IsType<ValueNode>()) {
			return &cur;
		}

		if (cur.IsType<ArrayNode>()) {
			iterable_ = true;
			for (const TreeNode* child : cur.Array()) {
				if (const TreeNode* result = findFirst(*child, path)) {
					return result;
				}
			}
		}
		return nullptr;
	}

	const TagName pathElement = path.front();

	if (cur.IsType<ObjectNode>()) {
		const TreeNode* child = cur.Object(pathElement);

		if (child == nullptr) {
			return nullptr;
		}
		return findFirst(*child, path.subspan(1));
	}

	if (cur.IsType<ArrayNode>()) {
		iterable_ = true;
		for (const TreeNode* child : cur.Array()) {
			if (const TreeNode* result = findFirst(*child, path)) {
				return result;
			}
		}
	}

	return nullptr;
}

const TreeNode* TagsPathCursor::Next() {
	if (!started_) {
		started_ = true;
		current_ = findFirst(root_, path_);
	} else if (current_) {
		current_ = findNext();
	}
	return current_;
}

}  // namespace reindexer
