#pragma once

#include <memory>
#include <span>

namespace reindexer {

class Index;

// The index storage must outlive the cleaner and must not be resized or reallocated while it exists.
class [[nodiscard]] IndexesCacheCleaner {
public:
	explicit IndexesCacheCleaner(std::span<const std::unique_ptr<Index>> indexes) noexcept : indexes_{indexes} {}
	IndexesCacheCleaner(const IndexesCacheCleaner&) = delete;
	IndexesCacheCleaner(IndexesCacheCleaner&&) = delete;
	IndexesCacheCleaner& operator=(const IndexesCacheCleaner&) = delete;
	IndexesCacheCleaner& operator=(IndexesCacheCleaner&&) = delete;
	void Add(Index& idx) noexcept;
	~IndexesCacheCleaner();

private:
	const std::span<const std::unique_ptr<Index>> indexes_;
	bool requiresCleanup_{false};
};

}  // namespace reindexer
