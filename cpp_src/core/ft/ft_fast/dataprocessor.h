#pragma once

#include <memory>
#include <string_view>
#include <thread>
#include <variant>
#include "dataholder.h"

namespace reindexer {

class ExceptionPtrWrapper;

template <typename IdCont>
class [[nodiscard]] DataProcessor {
public:
	using words_map =
		tsl::hopscotch_map<std::string, WordEntry, word_hash, word_equal, std::allocator<std::pair<std::string, WordEntry>>, 30, true>;

	DataProcessor(DataHolder<IdCont>& holder, size_t fieldSize) : holder_(holder), fieldSize_(fieldSize) {}

	void Process(bool multithread);

private:
	using WordVariant = std::variant<std::string_view, WordIdType>;

	static_assert(std::is_trivially_destructible_v<WordVariant>, "Expecting trivial destructor");
	static_assert(sizeof(WordVariant) <= 24, "Expecting same size as corresponding union");

	class [[nodiscard]] WordsVector : private std::vector<WordVariant> {
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

	class [[nodiscard]] ThreadsContainer {
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

	size_t buildWordsMap(words_map& m, bool multithread, intrusive_ptr<const ISplitter> textSplitter);
	void buildVirtualWord(std::string_view word, words_map& words_um, VDocIdType docType, unsigned field, unsigned arrayIdx,
						  size_t insertPos, std::vector<std::string_view>& container);
	void buildTyposMap(uint32_t startPos, const WordsVector& preprocWords);
	static WordsVector insertIntoSuffix(words_map& words_um, DataHolder<IdCont>& holder);
	static size_t commitIdRelSets(const WordsVector& preprocWords, words_map& words_um, DataHolder<IdCont>& holder, size_t wrdOffset);
	template <typename F, typename... Args>
	static std::thread runInThread(ExceptionPtrWrapper&, F&&, Args&&...) noexcept;

	DataHolder<IdCont>& holder_;
	size_t fieldSize_;
};

}  // namespace reindexer
