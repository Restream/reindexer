#pragma once
#include <stddef.h>
#include <stdint.h>
#include <cstdlib>
#include <memory>
#include <string_view>

namespace reindexer {

namespace debug {

using OwnedCStr = std::unique_ptr<char, decltype(&std::free)>;

class [[nodiscard]] TraceEntry {
public:
	/// Construct entry with address. Also try to resolve symbol by dl
	/// @param addr Address of entry
	TraceEntry(uintptr_t addr);
	TraceEntry() = default;
	~TraceEntry() = default;
	TraceEntry(TraceEntry&&) noexcept = default;
	TraceEntry& operator=(TraceEntry&&) noexcept = default;
	TraceEntry(const TraceEntry&) = delete;
	TraceEntry& operator=(const TraceEntry&) = delete;
	std::ostream& Dump(std::ostream& os) const;
	std::string_view FuncName() const noexcept { return funcName_; }
	void SetFuncName(const char* function) noexcept;
	std::string_view SrcFile() const noexcept { return srcFile_; }
	void SetSrcFile(const char* filename, int lineno) noexcept;
	int SrcLine() const noexcept { return srcLine_; }
	uintptr_t Addr() const noexcept { return addr_; }

private:
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
	/// Owned function/symbol name (demangled or copied from libbacktrace)
	OwnedCStr funcNameHolder_{nullptr, &std::free};
	/// Owned source file path (copied from libbacktrace)
	OwnedCStr srcFileHolder_{nullptr, &std::free};
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
