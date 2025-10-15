#pragma once

#include "reindexer_api.h"
#include "servercontrol.h"
#include "tools/fsops.h"

using reindexer::fast_hash_map;
using reindexer::WrSerializer;
using reindexer::DSN;

struct [[nodiscard]] InitShardingConfig {
	using ShardingConfig = reindexer::cluster::ShardingConfig;
	class [[nodiscard]] Namespace {
	public:
		Namespace(std::string n, bool wd = false) : name(std::move(n)), withData(wd) {}

		std::string name;
		bool withData;
		std::function<ShardingConfig::Key(int shard)> keyValuesNodeCreation = [](int shard) {
			ShardingConfig::Key key;
			key.values.emplace_back(Variant(std::string("key") + std::to_string(shard)));
			key.values.emplace_back(Variant(std::string("key") + std::to_string(shard) + "_" + std::to_string(shard)));
			key.shardId = shard;
			return key;
		};
		std::string indexName = "location";
	};

	int shards = 3;
	int nodesInCluster = 3;
	int rowsInTableOnShard = 40;
	bool disableNetworkTimeout = false;
	bool createAdditionalIndexes = true;
	bool needFillDefaultNs = true;
	std::vector<Namespace> additionalNss;
	std::chrono::seconds awaitTimeout = std::chrono::seconds(30);
	fast_hash_map<int, std::string>* insertedItemsById = nullptr;
	int nodeIdInThread = -1;  // Allows to run one of the nodes in thread, instead fo process
	uint8_t strlen = 0;		  // Strings len in items, which will be created during Fill()
	// if it is necessary to explicitly set specific cluster nodes in shards
	std::shared_ptr<std::map<int, std::vector<DSN>>> shardsMap;
};

class [[nodiscard]] ShardingApi : public ReindexerApi {
public:
	using ShardingConfig = reindexer::cluster::ShardingConfig;
	static const std::string_view kConfigTemplate;
	enum class [[nodiscard]] ApplyType : bool { Shared, Local };

	void Init(InitShardingConfig c = InitShardingConfig());

	void SetUp() override;
	void TearDown() override;

	bool StopByIndex(size_t idx);
	bool StopByIndex(size_t shard, size_t node);

	bool Stop(size_t shardId);
	void Stop();

	void Start(size_t shardId);
	void StartByIndex(size_t idx);

	bool StopSC(ServerControl& sc);
	bool StopSC(size_t i, size_t j);

	reindexer::client::Item CreateItem(std::string_view nsName, reindexer::client::Reindexer& rx, std::string_view key, int index,
									   WrSerializer& wrser, uint8_t strlen = 0);
	Error CreateAndUpsertItem(std::string_view nsName, reindexer::client::Reindexer& rx, std::string_view key, int index,
							  fast_hash_map<int, std::string>* insertedItemsById = nullptr, uint8_t strlen = 0);
	void Fill(std::string_view nsName, size_t shard, const size_t from, const size_t count,
			  fast_hash_map<int, std::string>* insertedItemsById = nullptr, uint8_t strlen = 0);
	void Fill(std::string_view nsName, reindexer::client::Reindexer& rx, std::string_view key, const size_t from, const size_t count);
	Error AddRow(std::string_view nsName, size_t shard, size_t nodeId, size_t index);

	void AwaitOnlineReplicationStatus(size_t idx);
	size_t NodesCount() const { return kShards * kNodesInCluster; }

	void MultyThreadApplyConfigTest(ApplyType type);

protected:
	class CompareShardId;
	struct [[nodiscard]] Defaults {
		size_t defaultRpcPort;
		size_t defaultHttpPort;
		std::string baseTestsetDbPath;
	};

	virtual const Defaults& GetDefaults() const {
		static Defaults def{19000, 20000, reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "rx_test/ShardingBaseApi")};
		return def;
	}

	void waitSync(std::string_view ns);
	bool checkSync(std::string_view ns);
	void waitSync(size_t shardId, std::string_view ns);
	std::pair<size_t, size_t> getSCIdxs(size_t id) { return std::make_pair(id / kNodesInCluster, id % kNodesInCluster); }
	ServerControl::Interface::Ptr getNode(size_t idx);
	void checkTransactionErrors(reindexer::client::Reindexer& rx, std::string_view nsName);

	void runSelectTest(std::string_view nsName);
	void runUpdateTest(std::string_view nsName);
	void runDeleteTest(std::string_view nsName);
	void runMetaTest(std::string_view nsName);
	void runSerialTest(std::string_view nsName);
	void runUpdateItemsTest(std::string_view nsName);
	void runDeleteItemsTest(std::string_view nsName);
	void runUpdateIndexTest(std::string_view nsName);
	void runDropNamespaceTest(std::string_view nsName);
	void runTransactionsTest(std::string_view nsName);

	template <typename T>
	void fillShards(const std::map<int, std::set<T>>& shardDataDistrib, const std::string& kNsName, const std::string& kFieldId);

	template <typename T>
	void fillShard(int shard, const std::set<T>& shardData, const std::string& kNsName, const std::string& kFieldId);

	template <typename T>
	void runLocalSelectTest(std::string_view nsName, const std::map<int, std::set<T>>& shardDataDistrib);
	template <typename T>
	void runSelectTest(std::string_view nsName, const std::map<int, std::set<T>>& shardDataDistrib);
	void runTransactionsTest(std::string_view nsName, const std::map<int, std::set<int>>& shardDataDistrib);

	template <typename T>
	ShardingConfig makeShardingConfigByDistrib(std::string_view nsName, const std::map<int, std::set<T>>& shardDataDistrib, int shards = 3,
											   int nodes = 3) const;

	Error applyNewShardingConfig(reindexer::client::Reindexer& rx, const ShardingConfig& config, ApplyType type,
								 std::optional<int64_t> sourceId = std::optional<int64_t>()) const;

	void checkConfig(const ServerControl::Interface::Ptr& server, const ShardingConfig& config);
	void checkConfigThrow(const ServerControl::Interface::Ptr& server, const ShardingConfig& config);
	int64_t getSourceIdFrom(const ServerControl::Interface::Ptr& server);
	std::optional<ShardingConfig> getShardingConfigFrom(reindexer::client::Reindexer& rx);

	void changeClusterLeader(int shardId);

	void checkMaskedDSNsInConfig(int i);

	void checkMasking();

	size_t kShards = 3;
	size_t kNodesInCluster = 3;

	const std::string kFieldId = "id";
	const std::string kFieldLocation = "location";
	const std::string kFieldData = "data";
	const std::string kFieldFTData = "ft_data";
	const std::string kFieldStruct = "struct";
	const std::string kFieldRand = "rand";
	const std::string kFieldShard = "shard";
	const std::string kFieldNestedRand = kFieldStruct + '.' + kFieldRand;
	const std::string kFieldIdLocation = kFieldId + '+' + kFieldLocation;
	const std::string kFieldLocationId = kFieldLocation + '+' + kFieldId;
	const std::string kFieldDataInt = "data_int";
	const std::string kSparseFieldDataInt = "sparse_data_int";
	const std::string kIndexDataInt = "data_int_index";
	const std::string kIndexDataIntLocation = kIndexDataInt + '+' + kFieldLocation;
	const std::string kIndexLocationDataInt = kFieldLocation + '+' + kIndexDataInt;
	const std::string kSparseIndexDataInt = "sparse_data_int_index";
	const std::string kFieldDataString = "data_string";
	const std::string kIndexDataString = "data_string_index";
	const std::string kSparseFieldDataString = "sparse_data_string";
	const std::string kSparseIndexDataString = "sparse_data_string_index";

	std::vector<std::vector<ServerControl>> svc_;  //[shard][nodeId]
};
