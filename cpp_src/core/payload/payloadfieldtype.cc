#include "payloadfieldtype.h"
#include <sstream>
#include "core/embedding/embedder.h"
#include "core/embedding/embedderscache.h"
#include "core/index/index.h"
#include "core/keyvalue/p_string.h"
#include "core/keyvalue/uuid.h"
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

template <class T>
std::shared_ptr<T> createEmbedder(std::string_view nsName, std::string_view idxName,
								  const std::optional<FloatVectorIndexOpts::EmbedderOpts>& cfg,
								  const std::shared_ptr<EmbeddersCache>& embeddersCache, bool enablePerfStat) {
	if (!cfg.has_value()) {
		return {};
	}

	const auto& opts = cfg.value();
	EmbedderConfig embedderCfg{CacheTag{opts.cacheTag}, opts.fields, convert(opts.strategy)};
	PoolConfig poolCfg{opts.pool.connections, opts.endpointUrl, opts.pool.connect_timeout_ms, opts.pool.read_timeout_ms,
					   opts.pool.write_timeout_ms};
	const auto embedderName = opts.name.empty() ? std::string{nsName} + "_" + toLower(idxName) : toLower(opts.name);
	embeddersCache->IncludeTag(opts.cacheTag);
	return std::make_shared<T>(embedderName, idxName, std::move(embedderCfg), std::move(poolCfg), embeddersCache, enablePerfStat);
}

}  // namespace

PayloadFieldType::PayloadFieldType(std::string_view nsName, const Index& index, const IndexDef& indexDef,
								   const std::shared_ptr<EmbeddersCache>& embeddersCache, bool enablePerfStat)
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
		createEmbedders(nsName, indexDef.Opts().FloatVector().Embedding(), embeddersCache, enablePerfStat);
	} else if (indexDef.IndexType() == IndexType::IndexRTree) {
		arrayDims_ = 2;
	}
}

size_t PayloadFieldType::Sizeof() const noexcept { return IsArray() ? sizeof(PayloadFieldValue::Array) : ElemSizeof(); }

size_t PayloadFieldType::ElemSizeof() const noexcept {
	return Type().EvaluateOneOf(
		[](KeyValueType::Bool) noexcept { return sizeof(bool); }, [](KeyValueType::Int) noexcept { return sizeof(int); },
		[](KeyValueType::Int64) noexcept { return sizeof(int64_t); }, [](KeyValueType::Uuid) noexcept { return sizeof(Uuid); },
		[](KeyValueType::Double) noexcept { return sizeof(double); }, [](KeyValueType::String) noexcept { return sizeof(p_string); },
		[](KeyValueType::FloatVector) noexcept { return sizeof(ConstFloatVectorView); },
		[](const concepts::OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null,
								 KeyValueType::Float> auto) noexcept -> size_t {
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

void PayloadFieldType::createEmbedders(std::string_view nsName, const std::optional<FloatVectorIndexOpts::EmbeddingOpts>& embeddingOpts,
									   const std::shared_ptr<EmbeddersCache>& embeddersCache, bool enablePerfStat) {
	if (!embeddingOpts.has_value()) {
		return;
	}
	const auto& cfg = embeddingOpts.value();
	upsertEmbedder_ = createEmbedder<const reindexer::UpsertEmbedder>(nsName, name_, cfg.upsertEmbedder, embeddersCache, enablePerfStat);
	queryEmbedder_ = createEmbedder<const reindexer::QueryEmbedder>(nsName, name_, cfg.queryEmbedder, embeddersCache, enablePerfStat);
}

}  // namespace reindexer
