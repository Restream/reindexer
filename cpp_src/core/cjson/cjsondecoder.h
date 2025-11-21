#pragma once

#include "core/cjson/tagspath.h"
#include "core/payload/fieldsset.h"
#include "core/payload/payloadiface.h"
#include "core/queryresults/fields_filter.h"
#include "core/type_consts.h"
#include "recoder.h"

namespace reindexer {

class Serializer;
class WrSerializer;
class FloatVectorsHolderVector;

class [[nodiscard]] CJsonDecoder {
public:
	using StrHolderT = h_vector<key_string, 16>;

	explicit CJsonDecoder(TagsMatcher& tagsMatcher, StrHolderT& storage) noexcept : tagsMatcher_(tagsMatcher), storage_(storage) {}
	class [[nodiscard]] SkipFilter {
	public:
		SkipFilter MakeCleanCopy() const noexcept { return SkipFilter(); }
		SkipFilter MakeSkipFilter() const noexcept { return SkipFilter(); }

		RX_ALWAYS_INLINE bool contains([[maybe_unused]] int field) const noexcept { return false; }
		RX_ALWAYS_INLINE bool match(const TagsPath&) const noexcept { return false; }
	};

	class [[nodiscard]] DefaultFilter {
	public:
		DefaultFilter(const FieldsFilter* fieldsFilter) noexcept : fieldsFilter_{fieldsFilter} {}
		DefaultFilter MakeCleanCopy() const noexcept { return DefaultFilter(fieldsFilter_); }
		SkipFilter MakeSkipFilter() const noexcept { return SkipFilter(); }
		RX_ALWAYS_INLINE bool HasArraysFields(const PayloadTypeImpl&) const noexcept { return false; }

		RX_ALWAYS_INLINE bool contains([[maybe_unused]] int field) const noexcept { return true; }
		RX_ALWAYS_INLINE bool containsVector([[maybe_unused]] int field) const noexcept {
			return !fieldsFilter_ || fieldsFilter_->ContainsVector(field);
		}
		RX_ALWAYS_INLINE bool match(const TagsPath&) const noexcept { return true; }

	private:
		const FieldsFilter* fieldsFilter_{nullptr};
	};

	class [[nodiscard]] IndexedSkipFilter {
	public:
		explicit IndexedSkipFilter(const FieldsSet& f) noexcept : f_(&f) {}
		IndexedSkipFilter MakeCleanCopy() const noexcept { return IndexedSkipFilter(*f_); }
		IndexedSkipFilter MakeSkipFilter() const noexcept { return IndexedSkipFilter(*f_); }

		RX_ALWAYS_INLINE bool contains(int field) const noexcept { return f_->contains(field); }
		RX_ALWAYS_INLINE bool match(const TagsPath&) const noexcept { return false; }

	private:
		const FieldsSet* f_{nullptr};
	};

	class [[nodiscard]] RestrictingFilter {
	public:
		RestrictingFilter(const FieldsSet& f) noexcept : f_(&f), match_(true) {}

		RestrictingFilter MakeCleanCopy() const noexcept { return RestrictingFilter(*f_); }
		IndexedSkipFilter MakeSkipFilter() const noexcept { return IndexedSkipFilter(*f_); }
		RX_ALWAYS_INLINE bool HasArraysFields(const PayloadTypeImpl& pt) const noexcept {
			for (auto f : *f_) {
				if (f >= 0 && pt.Field(f).IsArray()) {
					return true;
				}
			}
			return false;
		}

		RX_ALWAYS_INLINE bool contains(int field) noexcept {
			match_ = f_->contains(field);
			return match_;
		}
		RX_ALWAYS_INLINE bool match(const TagsPath& tagsPath) noexcept {
			match_ = match_ && f_->getTagsPathsLength() && f_->match(tagsPath);
			return match_;
		}

	private:
		const FieldsSet* f_{nullptr};
		bool match_{false};
	};

	class [[nodiscard]] DefaultRecoder {
	public:
		static RX_ALWAYS_INLINE DefaultRecoder MakeCleanCopy() noexcept { return DefaultRecoder(); }
		RX_ALWAYS_INLINE bool Recode(Serializer&, WrSerializer&) const { return false; }
		RX_ALWAYS_INLINE bool Recode(Serializer&, Payload&, TagName, WrSerializer&) const noexcept { return false; }
		RX_ALWAYS_INLINE TagType RegisterTagType(TagType tagType, int) const noexcept {
			// Do not recode index field
			return tagType;
		}
		RX_ALWAYS_INLINE TagType RegisterTagType(TagType tagType, const TagsPath&) noexcept { return tagType; }
	};

	class [[nodiscard]] CustomRecoder {
	public:
		CustomRecoder(Recoder& r) noexcept : r_(&r), needToRecode_(false) {}

		RX_ALWAYS_INLINE CustomRecoder MakeCleanCopy() const noexcept { return CustomRecoder(*r_); }
		RX_ALWAYS_INLINE bool Recode(Serializer& ser, WrSerializer& wser) const {
			if (needToRecode_) {
				r_->Recode(ser, wser);
				return true;
			}
			return defaultRecoder_.Recode(ser, wser);
		}
		RX_ALWAYS_INLINE bool Recode(Serializer& ser, Payload& pl, TagName tagName, WrSerializer& wser) const {
			if (needToRecode_) {
				r_->Recode(ser, pl, tagName, wser);
				return true;
			}
			return defaultRecoder_.Recode(ser, pl, tagName, wser);
		}
		RX_ALWAYS_INLINE TagType RegisterTagType(TagType tagType, int field) {
			needToRecode_ = r_->Match(field);
			return needToRecode_ ? r_->Type(tagType) : defaultRecoder_.RegisterTagType(tagType, field);
		}
		RX_ALWAYS_INLINE TagType RegisterTagType(TagType tagType, const TagsPath& tagsPath) {
			needToRecode_ = r_->Match(tagsPath);
			return needToRecode_ ? r_->Type(tagType) : defaultRecoder_.RegisterTagType(tagType, tagsPath);
		}

	private:
		DefaultRecoder defaultRecoder_;
		Recoder* r_{nullptr};
		bool needToRecode_{false};
	};
	struct [[nodiscard]] NamedTagOpt {};
	struct [[nodiscard]] NamelessTagOpt {};

	template <typename Filter, typename Recoder = DefaultRecoder>
	void Decode(Payload& pl, Serializer& rdSer, WrSerializer& wrSer, FloatVectorsHolderVector& floatVectorsHolder, Filter filter = Filter(),
				Recoder recoder = Recoder()) {
		static_assert(std::is_same_v<Filter, DefaultFilter> || std::is_same_v<Filter, RestrictingFilter>,
					  "Other filter types are not allowed for the public API");
		static_assert(std::is_same_v<Recoder, DefaultRecoder> || std::is_same_v<Recoder, CustomRecoder>,
					  "Other recoder types are not allowed for the public API");
		objectScalarIndexes_.reset();
		if (!filter.HasArraysFields(pl.Type())) [[likely]] {
			decodeCJson(pl, rdSer, wrSer, filter, recoder, NamelessTagOpt{}, floatVectorsHolder);
			return;
		}
#ifdef RX_WITH_STDLIB_DEBUG
		std::abort();
#else
		// Search of the indexed fields inside the object arrays is not implemented
		// Possible implementation has noticeable negative impact on 'FromCJSONPKOnly' benchmark.
		// Currently, we are using filter for PKs and vector only, and PKs and vectors can not be arrays,
		// so this code actually will never be called at the current moment
		decodeCJson(pl, rdSer, wrSer, DefaultFilter(nullptr), recoder, NamelessTagOpt{}, floatVectorsHolder);
#endif	// RX_WITH_STDLIB_DEBUG
	}

private:
	template <typename Filter, typename Recoder, typename TagOptT>
	bool decodeCJson(Payload&, Serializer&, WrSerializer&, Filter, Recoder, TagOptT, FloatVectorsHolderVector&);
	template <typename Filter, typename Recoder, typename Validator>
	void decodeCJson(Payload&, Serializer&, WrSerializer&, Filter, Recoder, TagType, TagName, ctag, FloatVectorsHolderVector&,
					 const Validator&);
	InArray isInArray() const noexcept { return InArray(arrayLevel_ > 0); }
	[[noreturn]] void throwTagReferenceError(ctag, const Payload&);

	Variant cjsonValueToVariant(TagType tag, Serializer& rdser, KeyValueType dstType) const;
	size_t decodeNestedArray(Payload&, Serializer&, WrSerializer&, int indexNumber, size_t count, KeyValueType fieldType,
							 size_t offset) const;

	TagsMatcher& tagsMatcher_;
	TagsPath tagsPath_;
	int32_t arrayLevel_{0};
	ScalarIndexesSetT objectScalarIndexes_;
	// storage for owning strings obtained from numbers
	StrHolderT& storage_;
};

}  // namespace reindexer
