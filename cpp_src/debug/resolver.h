#pragma once
#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string_view>

namespace reindexer {

namespace debug {

class [[nodiscard]] TraceEntry {
public:
	/// Construct entry with address. Also try to resolve symbol by dl
	/// @param addr Address of entry
	TraceEntry(uintptr_t addr);
	TraceEntry() = default;
	~TraceEntry();
	TraceEntry(TraceEntry&& other) noexcept;
	TraceEntry& operator=(TraceEntry&&) noexcept;
	TraceEntry(const TraceEntry&) = delete;
	TraceEntry& operator=(const TraceEntry&) = delete;
	std::ostream& Dump(std::ostream& os) const;
	std::string_view FuncName() { return funcName_; }

	// protected:
	/// Demangled function(symbol) name
	std::string_view funcName_;
	/// Object file name
	std::string_view objFile_;
	/// Source file name
	std::string_view srcFile_;
	/// Source file line number
	int srcLine_ = 0;
	/// Offset from symbol address to
	ptrdiff_t ofs_ = 0;
	/// Address of entry
	uintptr_t addr_ = 0;
	/// Base address of object
	uintptr_t baseAddr_ = 0;
	/// Holder of temporary data
	char* holder_ = nullptr;
};

static inline std::ostream& operator<<(std::ostream& os, const TraceEntry& e) { return e.Dump(os); }

class [[nodiscard]] TraceResolver {
public:
	static std::unique_ptr<TraceResolver> New();
	virtual ~TraceResolver() = default;
	virtual bool Resolve(TraceEntry&) { return false; }
};

}  // namespace debug
}  // namespace reindexer
