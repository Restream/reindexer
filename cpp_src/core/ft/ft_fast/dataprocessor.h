#pragma once
#include <memory>
#include <string_view>
#include <thread>
#include <variant>
#include "dataholder.h"

namespace reindexer {

template <typename IdCont>
class DataProcessor {
public:
	using words_map =
		tsl::hopscotch_map<std::string, WordEntry, word_hash, word_equal, std::allocator<std::pair<std::string, WordEntry>>, 30, true>;

	DataProcessor(DataHolder<IdCont>& holder, size_t fieldSize) : holder_(holder), fieldSize_(fieldSize) {}

	void Process(bool multithread);

private:
	using WordVariant = std::variant<std::string_view, WordIdType>;

	static_assert(std::is_trivially_destructible_v<WordVariant>, "Expecting trivial destructor");
	static_assert(sizeof(WordVariant) <= 24, "Expecting same size as correspondig union");

	class WordsVector : private std::vector<WordVariant> {
		using Base = std::vector<WordVariant>;

	public:
		void emplace_back(std::string_view word) { Base::emplace_back(word); }
		void emplace_back(WordIdType word) {
			assertrx_throw(!word.IsEmpty());
			Base::emplace_back(word);
		}
		using Base::begin;
		using Base::end;
		using Base::reserve;
		using Base::size;
		using Base::empty;
		using Base::operator[];
	};

	class ExceptionPtrWrapper {
	public:
		void SetException(std::exception_ptr&& ptr) noexcept {
			std::lock_guard lck(mtx_);
			if (!ex_) {
				ex_ = std::move(ptr);
			}
		}
		void RethrowException() {
			std::lock_guard lck(mtx_);
			if (ex_) {
				auto ptr = std::move(ex_);
				ex_ = nullptr;
				std::rethrow_exception(std::move(ptr));
			}
		}
		bool HasException() const noexcept {
			std::lock_guard lck(mtx_);
			return bool(ex_);
		}

	private:
		std::exception_ptr ex_ = nullptr;
		mutable std::mutex mtx_;
	};

	class ThreadsContainer {
	public:
		template <typename F>
		void Add(F&& f) {
			threads_.emplace_back(std::forward<F>(f));
		}
		~ThreadsContainer() {
			for (auto& th : threads_) {
				if (th.joinable()) {
					th.join();
				}
			}
		}

	private:
		std::vector<std::thread> threads_;
	};

	[[nodiscard]] size_t buildWordsMap(words_map& m, bool multithread);
	void buildVirtualWord(std::string_view word, words_map& words_um, VDocIdType docType, int rfield, size_t insertPos,
						  std::vector<std::string>& container);
	void buildTyposMap(uint32_t startPos, const WordsVector& preprocWords);
	[[nodiscard]] static WordsVector insertIntoSuffix(words_map& words_um, DataHolder<IdCont>& holder);
	[[nodiscard]] static size_t commitIdRelSets(const WordsVector& preprocWords, words_map& words_um, DataHolder<IdCont>& holder,
												size_t wrdOffset);
	template <typename F, typename... Args>
	[[nodiscard]] static std::thread runInThread(ExceptionPtrWrapper&, F&&, Args&&...) noexcept;

	DataHolder<IdCont>& holder_;
	size_t fieldSize_;
};

extern template class DataProcessor<PackedIdRelVec>;
extern template class DataProcessor<IdRelVec>;

}  // namespace reindexer
