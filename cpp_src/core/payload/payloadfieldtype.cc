#include "payloadfieldtype.h"
#include <sstream>
#include "core/embedding/embedder.h"
#include "core/index/index.h"
#include "core/keyvalue/p_string.h"
#include "core/keyvalue/uuid.h"
#include "estl/one_of.h"
#include "payloadfieldvalue.h"

namespace reindexer {

namespace {
EmbedderConfig::Strategy convert(FloatVectorIndexOpts::EmbedderOpts::Strategy strategy) noexcept {
	switch (strategy) {
		case FloatVectorIndexOpts::EmbedderOpts::Strategy::Always:
			return EmbedderConfig::Strategy::Always;
		case FloatVectorIndexOpts::EmbedderOpts::Strategy::EmptyOnly:
			return EmbedderConfig::Strategy::EmptyOnly;
		case FloatVectorIndexOpts::EmbedderOpts::Strategy::Strict:
			return EmbedderConfig::Strategy::Strict;
	}
	return {};
}

std::shared_ptr<Embedder> createEmbedder(std::string_view nsName, std::string_view idxName, int idx, const FloatVectorIndexOpts::EmbedderOpts& opts) {
	EmbedderConfig embedderCfg{
		opts.cacheTag,
		opts.fields,
		convert(opts.strategy)
	};
	PoolConfig poolCfg{
		opts.pool.connections,
		opts.endpointUrl,
		opts.pool.connect_timeout_ms,
		opts.pool.read_timeout_ms,
		opts.pool.write_timeout_ms
	};

	const auto embedderName = opts.name.empty() ? std::string{nsName} + "_" + toLower(idxName) : toLower(opts.name);
	return std::make_shared<reindexer::Embedder>(embedderName, idxName, idx, std::move(embedderCfg), std::move(poolCfg));
}
}  // namespace

PayloadFieldType::PayloadFieldType(std::string_view nsName, int idx, const Index& index, const IndexDef& indexDef) noexcept
	: type_(index.KeyType()),
	  name_(indexDef.Name()),
	  jsonPaths_(indexDef.JsonPaths()),
	  offset_(0),
	  arrayDims_(0),
	  isArray_(index.Opts().IsArray()) {
	// Float indexed fields are not supported (except for the float_vector for KNN indexes)
	assertrx_throw(!type_.Is<KeyValueType::Float>());
	if (index.IsFloatVector()) {
		arrayDims_ = index.FloatVectorDimension().Value();
		assertrx(type_.Is<KeyValueType::FloatVector>());

		auto embedding = index.Opts().FloatVector().Embedding();
		if (embedding.has_value()) {
			const auto& cfg = embedding.value();
			if (cfg.upsertEmbedder.has_value()) {
				embedder_ = createEmbedder(nsName, name_, idx, cfg.upsertEmbedder.value());
			}
			if (cfg.queryEmbedder.has_value()) {
				queryEmbedder_ = createEmbedder(nsName, name_, idx, cfg.queryEmbedder.value());
			}
		}
	} else if (indexDef.IndexType() == IndexType::IndexRTree) {
		arrayDims_ = 2;
	}
}

size_t PayloadFieldType::Sizeof() const noexcept { return IsArray() ? sizeof(PayloadFieldValue::Array) : ElemSizeof(); }

size_t PayloadFieldType::ElemSizeof() const noexcept {
	return Type().EvaluateOneOf(
		[](KeyValueType::Bool) noexcept { return sizeof(bool); }, [](KeyValueType::Int) noexcept { return sizeof(int); },
		[](OneOf<KeyValueType::Int64>) noexcept { return sizeof(int64_t); },
		[](OneOf<KeyValueType::Uuid>) noexcept { return sizeof(Uuid); }, [](KeyValueType::Double) noexcept { return sizeof(double); },
		[](KeyValueType::String) noexcept { return sizeof(p_string); },
		[](KeyValueType::FloatVector) noexcept { return sizeof(ConstFloatVectorView); },
		[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null, KeyValueType::Float>) noexcept
		-> size_t {
			assertrx(0);
			abort();
		});
}

std::string PayloadFieldType::ToString() const {
	std::stringstream ss;
	ss << "{ type: " << type_.Name() << ", name: " << name_ << ", jsonpaths: [";
	for (auto jit = jsonPaths_.cbegin(); jit != jsonPaths_.cend(); ++jit) {
		if (jit != jsonPaths_.cbegin()) {
			ss << ", ";
		}
		ss << *jit;
	}
	ss << "] }";
	return ss.str();
}

}  // namespace reindexer
