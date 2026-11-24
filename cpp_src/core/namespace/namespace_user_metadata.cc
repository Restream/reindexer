#include "namespace_user_metadata.h"

#include <algorithm>
#include <utility>
#include "asyncstorage.h"
#include "core/storage/storage_prefixes.h"
#include "core/type_consts.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace reindexer {
namespace {
constexpr int64_t kStorageSerialInitial = 1;
const std::string kFFFFFFFF{"\xFF\xFF\xFF\xFF"};
}  // namespace

void NamespaceUserMetadata::validateKey(std::string_view key) {
	if (key.empty()) [[unlikely]] {
		throw Error(errParams, "Empty key is not supported");
	}
}

std::string NamespaceUserMetadata::Get(const std::string& key) const {
	validateKey(key);
	const auto it = values_.find(key);
	return it == values_.end() ? std::string() : it->second;
}

std::vector<std::string> NamespaceUserMetadata::EnumKeys() const {
	std::vector<std::string> keys(values_.size());
	std::transform(values_.begin(), values_.end(), keys.begin(), [](const auto& pair) { return pair.first; });
	return keys;
}

void NamespaceUserMetadata::Put(const std::string& key, std::string_view value, AsyncStorage& storage) {
	validateKey(key);
	values_[key] = std::string(value);
	storage.Write(kStorageMetaPrefix + key, value);
}

void NamespaceUserMetadata::Remove(const std::string& key, AsyncStorage& storage) {
	validateKey(key);
	values_.erase(key);
	storage.Remove(kStorageMetaPrefix + key);
}

void NamespaceUserMetadata::LoadFromStorage(AsyncStorage& storage) {
	StorageOpts opts;
	auto dbIter = storage.GetCursor(opts);
	values_.clear();
	const size_t prefixLen = kStorageMetaPrefix.length();
	for (dbIter->Seek(kStorageMetaPrefix);
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), kStorageMetaPrefix + kFFFFFFFF) < 0; dbIter->Next()) {
		const std::string_view keySlice = dbIter->Key();
		if (keySlice.length() >= prefixLen) {
			values_.emplace(keySlice.substr(prefixLen), dbIter->Value());
		}
	}
}

NamespaceUserMetadata::SerialCounter NamespaceUserMetadata::GetNextSerial(std::string_view fieldName,
																		  std::string_view alternativeFieldName) const {
	int64_t counter = kStorageSerialInitial;
	std::string key(kSerialPrefix);
	key.append(fieldName);
	auto value = Get(key);
	if (!value.empty()) {
		counter = reindexer::stoll(value) + 1;
	}
	if (alternativeFieldName != fieldName) {
		std::string alternativeKey(kSerialPrefix);
		alternativeKey.append(alternativeFieldName);
		value = Get(alternativeKey);
		if (!value.empty()) {
			const int64_t alternativeCounter = reindexer::stoll(value) + 1;
			if (alternativeCounter > counter) {
				counter = alternativeCounter;
			}
		}
	}
	return {.key = std::move(key), .value = counter};
}

}  // namespace reindexer
