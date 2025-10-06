// Based on https://github.com/nmslib/hnswlib/tree/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c
// Apache 2.0 license (copyright by yurymalkov) may be found here:
// https://github.com/nmslib/hnswlib/blob/c1b9b79af3d10c6ee7b5d0afa1ce851ae975254c/LICENSE

#pragma once

#include <string.h>
#include <deque>
#include "estl/lock.h"
#include "estl/mutex.h"

namespace hnswlib {
typedef unsigned short int vl_type;

class [[nodiscard]] VisitedList {
public:
	vl_type curV;
	vl_type* mass;
	unsigned int numelements;

	VisitedList(int numelements1) {
		curV = -1;
		numelements = numelements1;
		mass = new vl_type[numelements];
	}

	void reset() {
		curV++;
		if (curV == 0) {
			memset(mass, 0, sizeof(vl_type) * numelements);
			curV++;
		}
	}

	~VisitedList() { delete[] mass; }
};
///////////////////////////////////////////////////////////
//
// Class for multi-threaded pool-management of VisitedLists
//
/////////////////////////////////////////////////////////

class [[nodiscard]] VisitedListPool {
	std::deque<VisitedList*> pool;
	mutable reindexer::mutex poolguard;
	int numelements;

public:
	VisitedListPool(int initmaxpools, int numelements1) {
		numelements = numelements1;
		for (int i = 0; i < initmaxpools; i++) {
			pool.push_front(new VisitedList(numelements));
		}
	}

	VisitedList* getFreeVisitedList() {
		VisitedList* rez;
		{
			reindexer::unique_lock lock(poolguard);
			if (pool.size() > 0) {
				rez = pool.front();
				pool.pop_front();
			} else {
				lock.unlock();

				rez = new VisitedList(numelements);
			}
		}
		rez->reset();
		return rez;
	}

	void releaseVisitedList(VisitedList* vl) {
		reindexer::lock_guard lock(poolguard);
		pool.push_front(vl);
	}

	// Approximate values
	size_t allocatedMemSize() const noexcept {
		reindexer::lock_guard lock(poolguard);
		return pool.size() * (numelements * sizeof(vl_type) + sizeof(VisitedList));
	}

	~VisitedListPool() {
		while (pool.size()) {
			VisitedList* rez = pool.front();
			pool.pop_front();
			delete rez;
		}
	}
};
}  // namespace hnswlib
