#pragma once

namespace reindexer {

class string_view;

class dummy_mutex {
public:
	void lock() {}
	void unlock() {}
};

enum class MutexMark : unsigned { DbManager = 1u, IndexText, Namespace, Reindexer, ReindexerStorage };
string_view DescribeMutexMark(MutexMark);

template <typename Mutex, MutexMark m>
class MarkedMutex : public Mutex {
public:
	constexpr static MutexMark mark = m;
};

}  // namespace reindexer
