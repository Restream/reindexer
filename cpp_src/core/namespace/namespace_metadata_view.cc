#include "namespace_metadata_view.h"

#include "core/index/index.h"
#include "indexes/registry.h"

namespace reindexer {

NamespaceMetadataView::IndexMetadata::IndexMetadata(const Index& index) noexcept
	: name_{index.Name()}, fields_{index.Fields()}, type_{index.Type()}, keyType_{index.KeyType()}, selectKeyType_{index.SelectKeyType()} {}

bool NamespaceMetadataView::TryGetIndexPosByNameOrJsonPath(std::string_view name, int& pos) const {
	return registry_->TryGetIndexPosByNameOrJsonPath(name, pos);
}

bool NamespaceMetadataView::TryGetIndexPosByJsonPath(std::string_view jsonPath, int& pos) const noexcept {
	return registry_->TryGetIndexPosByJsonPath(jsonPath, pos);
}

bool NamespaceMetadataView::IsFloatVectorIndex(int pos) const noexcept { return registry_->Indexes()[pos]->IsFloatVector(); }

bool NamespaceMetadataView::IsFulltextIndex(int pos) const noexcept { return registry_->Indexes()[pos]->IsFulltext(); }

NamespaceMetadataView::IndexMetadata NamespaceMetadataView::GetIndexMetadata(int pos) const& noexcept {
	return IndexMetadata{*registry_->Indexes()[pos]};
}

NamespaceMetadataView::CompositeIndexesRange NamespaceMetadataView::GetCompositeIndexesRange() const noexcept {
	return {.begin = registry_->Indexes().firstCompositePos(), .end = registry_->Indexes().totalSize()};
}

const FieldsSet& NamespaceMetadataView::GetIndexFields(int pos) const& noexcept { return registry_->Indexes()[pos]->Fields(); }

const PayloadType& NamespaceMetadataView::GetPayloadType() const& noexcept { return registry_->GetPayloadType(); }

TagsPath NamespaceMetadataView::GetTagsPath(std::string_view jsonPath) const { return registry_->GetTagsMatcher().path2tag(jsonPath); }

}  // namespace reindexer
