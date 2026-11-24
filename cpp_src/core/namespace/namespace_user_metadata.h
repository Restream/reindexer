#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace reindexer {

class AsyncStorage;

/**
 * @brief Owns user metadata and serial counters. Copies own independent values and do not retain a storage reference.
 * @pre Call under the namespace lock or during exclusive initialization. Pass the owning namespace's storage to mutations.
 */
class [[nodiscard]] NamespaceUserMetadata {
public:
	struct [[nodiscard]] SerialCounter {
		std::string key;
		int64_t value;
	};

	std::string Get(const std::string& key) const;
	std::vector<std::string> EnumKeys() const;
	void Put(const std::string& key, std::string_view value, AsyncStorage& storage);
	void Remove(const std::string& key, AsyncStorage& storage);
	void LoadFromStorage(AsyncStorage& storage);
	/** @brief Calculates the next counter without modifying metadata; the caller must persist it through the WAL/replication path. */
	SerialCounter GetNextSerial(std::string_view fieldName, std::string_view alternativeFieldName) const;

private:
	static void validateKey(std::string_view key);

	std::unordered_map<std::string, std::string> values_;
};

}  // namespace reindexer
