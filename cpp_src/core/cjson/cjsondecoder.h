#pragma once

#include <deque>
#include "core/payload/payloadiface.h"

namespace reindexer {

class TagsMatcher;
class Serializer;
class WrSerializer;

class Recoder {
public:
	[[nodiscard]] virtual TagType Type(TagType oldTagType) = 0;
	virtual void Recode(Serializer &, WrSerializer &) const = 0;
	virtual void Recode(Serializer &, Payload &, int tagName, WrSerializer &) = 0;
	[[nodiscard]] virtual bool Match(int field) const noexcept = 0;
	[[nodiscard]] virtual bool Match(const TagsPath &) const noexcept = 0;
	virtual ~Recoder() = default;
};

class CJsonDecoder {
public:
	explicit CJsonDecoder(TagsMatcher &tagsMatcher, std::deque<std::string> &storage) noexcept
		: tagsMatcher_(tagsMatcher), storage_(storage) {}
	class SkipFilter {
	public:
		SkipFilter MakeCleanCopy() const noexcept { return SkipFilter(); }
		SkipFilter MakeSkipFilter() const noexcept { return SkipFilter(); }

		RX_ALWAYS_INLINE bool contains([[maybe_unused]] int field) const noexcept { return false; }
		RX_ALWAYS_INLINE bool match(const TagsPath &) const noexcept { return false; }
	};

	class DummyFilter {
	public:
		DummyFilter MakeCleanCopy() const noexcept { return DummyFilter(); }
		SkipFilter MakeSkipFilter() const noexcept { return SkipFilter(); }
		RX_ALWAYS_INLINE bool HasArraysFields(const PayloadTypeImpl &) const noexcept { return false; }

		RX_ALWAYS_INLINE bool contains([[maybe_unused]] int field) const noexcept { return true; }
		RX_ALWAYS_INLINE bool match(const TagsPath &) const noexcept { return true; }
	};

	class IndexedSkipFilter {
	public:
		IndexedSkipFilter(const FieldsSet &f) noexcept : f_(&f) {}
		IndexedSkipFilter MakeCleanCopy() const noexcept { return IndexedSkipFilter(*f_); }
		IndexedSkipFilter MakeSkipFilter() const noexcept { return IndexedSkipFilter(*f_); }

		RX_ALWAYS_INLINE bool contains(int field) const noexcept { return f_->contains(field); }
		RX_ALWAYS_INLINE bool match(const TagsPath &) const noexcept { return false; }

	private:
		const FieldsSet *f_;
	};

	class RestrictingFilter {
	public:
		RestrictingFilter(const FieldsSet &f) noexcept : f_(&f), match_(true) {}

		RestrictingFilter MakeCleanCopy() const noexcept { return RestrictingFilter(*f_); }
		IndexedSkipFilter MakeSkipFilter() const noexcept { return IndexedSkipFilter(*f_); }
		RX_ALWAYS_INLINE bool HasArraysFields(const PayloadTypeImpl &pt) const noexcept {
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
		RX_ALWAYS_INLINE bool match(const TagsPath &tagsPath) noexcept {
			match_ = match_ && f_->getTagsPathsLength() && f_->match(tagsPath);
			return match_;
		}

	private:
		const FieldsSet *f_;
		bool match_;
	};

	class DummyRecoder {
	public:
		RX_ALWAYS_INLINE DummyRecoder MakeCleanCopy() const noexcept { return DummyRecoder(); }
		RX_ALWAYS_INLINE bool Recode(Serializer &, WrSerializer &) const noexcept { return false; }
		RX_ALWAYS_INLINE bool Recode(Serializer &, Payload &, [[maybe_unused]] int tagName, WrSerializer &) const noexcept { return false; }
		RX_ALWAYS_INLINE TagType RegisterTagType(TagType oldTagType, [[maybe_unused]] int field) const noexcept { return oldTagType; }
		RX_ALWAYS_INLINE TagType RegisterTagType(TagType oldTagType, const TagsPath &) const noexcept { return oldTagType; }
	};
	class DefaultRecoder {
	public:
		DefaultRecoder(Recoder &r) noexcept : r_(&r), needToRecode_(false) {}

		RX_ALWAYS_INLINE DefaultRecoder MakeCleanCopy() const noexcept { return DefaultRecoder(*r_); }

		RX_ALWAYS_INLINE bool Recode(Serializer &ser, WrSerializer &wser) const {
			if (needToRecode_) {
				r_->Recode(ser, wser);
			}
			return needToRecode_;
		}
		RX_ALWAYS_INLINE bool Recode(Serializer &s, Payload &p, int tagName, WrSerializer &wser) const {
			if (needToRecode_) {
				r_->Recode(s, p, tagName, wser);
			}
			return needToRecode_;
		}
		RX_ALWAYS_INLINE TagType RegisterTagType(TagType oldTagType, int field) {
			needToRecode_ = r_->Match(field);
			return needToRecode_ ? r_->Type(oldTagType) : oldTagType;
		}
		RX_ALWAYS_INLINE TagType RegisterTagType(TagType oldTagType, const TagsPath &tagsPath) {
			needToRecode_ = r_->Match(tagsPath);
			return needToRecode_ ? r_->Type(oldTagType) : oldTagType;
		}

	private:
		Recoder *r_;
		bool needToRecode_;
	};
	struct NamedTagOpt {};
	struct NamelessTagOpt {};

	template <typename FilterT = DummyFilter, typename RecoderT = DummyRecoder>
	void Decode(Payload &pl, Serializer &rdSer, WrSerializer &wrSer, FilterT filter = FilterT(), RecoderT recoder = RecoderT()) {
		static_assert(std::is_same_v<FilterT, DummyFilter> || std::is_same_v<FilterT, RestrictingFilter>,
					  "Other filter types are not allowed for the public API");
		static_assert(std::is_same_v<RecoderT, DummyRecoder> || std::is_same_v<RecoderT, DefaultRecoder>,
					  "Other recoder types are not allowed for the public API");
		objectScalarIndexes_.reset();
		if rx_likely (!filter.HasArraysFields(pl.Type())) {
			decodeCJson(pl, rdSer, wrSer, filter, recoder, NamelessTagOpt{});
			return;
		}
#ifdef RX_WITH_STDLIB_DEBUG
		std::abort();
#else
		// Search of the indexed fields inside the object arrays is not imlpemented
		// Possible implementation has noticable negative effect on 'FromCJSONPKOnly' benchmark.
		// Currently we are using filter for PKs only, and PKs can not be arrays, so this code actually will never be called at the
		// current moment
		decodeCJson(pl, rdSer, wrSer, DummyFilter(), recoder, NamelessTagOpt{});
#endif	// RX_WITH_STDLIB_DEBUG
	}

private:
	template <typename FilterT, typename RecoderT, typename TagOptT>
	bool decodeCJson(Payload &pl, Serializer &rdser, WrSerializer &wrser, FilterT filter, RecoderT recoder, TagOptT);
	bool isInArray() const noexcept { return arrayLevel_ > 0; }
	[[noreturn]] void throwTagReferenceError(ctag, const Payload &);
	[[noreturn]] void throwUnexpectedArrayError(const PayloadFieldType &);

	[[nodiscard]] Variant cjsonValueToVariant(TagType tag, Serializer &rdser, KeyValueType dstType);

	TagsMatcher &tagsMatcher_;
	TagsPath tagsPath_;
	int32_t arrayLevel_ = 0;
	ScalarIndexesSetT objectScalarIndexes_;
	// storage for owning strings obtained from numbers
	std::deque<std::string> &storage_;
};

extern template bool CJsonDecoder::decodeCJson<CJsonDecoder::DummyFilter, CJsonDecoder::DummyRecoder, CJsonDecoder::NamelessTagOpt>(
	Payload &, Serializer &, WrSerializer &, CJsonDecoder::DummyFilter, CJsonDecoder::DummyRecoder, CJsonDecoder::NamelessTagOpt);
extern template bool CJsonDecoder::decodeCJson<CJsonDecoder::DummyFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt>(
	Payload &, Serializer &, WrSerializer &, CJsonDecoder::DummyFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt);
extern template bool CJsonDecoder::decodeCJson<CJsonDecoder::RestrictingFilter, CJsonDecoder::DummyRecoder, CJsonDecoder::NamelessTagOpt>(
	Payload &, Serializer &, WrSerializer &, CJsonDecoder::RestrictingFilter, CJsonDecoder::DummyRecoder, CJsonDecoder::NamelessTagOpt);
extern template bool CJsonDecoder::decodeCJson<CJsonDecoder::RestrictingFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt>(
	Payload &, Serializer &, WrSerializer &, CJsonDecoder::RestrictingFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt);

}  // namespace reindexer
