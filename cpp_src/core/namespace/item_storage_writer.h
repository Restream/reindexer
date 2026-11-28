#pragma once

#include <string_view>

namespace reindexer {

class AsyncStorage;
class FieldsSet;
class ItemImpl;
class PayloadValue;
class WrSerializer;
class lsn_t;
template <typename T>
class PayloadIface;

/**
 * @brief Encodes stored documents as a PK-derived key and an LSN-prefixed CJSON value, then submits prepared records.
 * @pre The storage must outlive the writer. Use the owning namespace's storage under its lock or during exclusive initialization.
 */
class [[nodiscard]] ItemStorageWriter {
public:
	enum class [[nodiscard]] WriteMode { Queued, Immediate };

	explicit ItemStorageWriter(AsyncStorage& storage) noexcept : storage_{storage} {}

	/** @brief Resets the buffer and returns the encoded key; the view remains valid until the buffer changes. */
	static std::string_view EncodeKey(const PayloadIface<const PayloadValue>&, const FieldsSet& pk, WrSerializer&);
	/** @brief Resets the buffer and prefixes the already serialized CJSON with its LSN. */
	static std::string_view EncodeValue(lsn_t, std::string_view cjson, WrSerializer&);
	/** @brief Appends CJSON directly after the LSN header; indexed vectors must already be materialized in the item. */
	static std::string_view EncodeValue(lsn_t, ItemImpl&, WrSerializer&);
	/**
	 * @brief Submits prepared bytes. Immediate preserves AsyncStorage's best-effort WriteSync policy, including async fallback;
	 * it does not request a durable storage sync. The storage copies the bytes before returning.
	 */
	void WriteEncoded(std::string_view key, std::string_view value, WriteMode mode = WriteMode::Queued) const;
	void RemoveEncoded(std::string_view key) const;

private:
	AsyncStorage& storage_;
};

}  // namespace reindexer
