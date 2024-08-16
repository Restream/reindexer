#include "prefilter.h"
#include <random>
#include <vector>
#include "murmurhash/MurmurHash3.h"

Prefilter::Prefilter() {
	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<uint32_t> d(0, UINT32_MAX);
	for (size_t i = 0; i < seeds_.size(); ++i) {
		seeds_[i] = d(gen);
	}
}
size_t Prefilter::CalcSimDistance(const std::wstring& first, const std::wstring& second) {
	auto first_sim = GetSimHash(first);
	auto second_sim = GetSimHash(second);
	return GetSimDistance(first_sim, second_sim);
}
std::bitset<32> Prefilter::GetSimHash(const std::wstring& data) {
	std::vector<int> simhash(32, 0);
	for (size_t i = 0; i < data.size() - 1; ++i) {
		uint32_t hash[2];
		MurmurHash3_x64_128(data.substr(i, 2).c_str(), data.size() * sizeof(wchar_t), seeds_[0], &hash);
		std::bitset<32> bit_hash(hash[0]);
		for (int f = 0; f < 32; f++) {
			if (bit_hash.test(f)) {
				simhash[f]++;
			} else {
				simhash[f]--;
			}
		}
	}
	std::bitset<32> res;
	for (size_t i = 0; i < simhash.size(); ++i) {
		if (simhash[i] > 0) {
			res[i] = true;
		} else {
			res[i] = false;
		}
	}

	return res;
}
size_t Prefilter::GetSimDistance(const std::bitset<32> first, const std::bitset<32> second) { return (first ^ second).count(); }
std::vector<int> Prefilter::findMin(const std::wstring& data) {
	std::vector<int> sig(seeds_.size());
	for (size_t g = 0; g < seeds_.size(); ++g) {
		uint64_t min_hash = UINT64_MAX;

		for (size_t i = 0; i < data.size() - 1; ++i) {
			uint32_t hash[2];
			MurmurHash3_x64_128(data.substr(i, 2).c_str(), data.size() * sizeof(wchar_t), seeds_[g], &hash);
			if (hash[0] < min_hash) {
				min_hash = hash[0];
			}
		}
		sig[g] = min_hash;
	}
	return sig;
}
double Prefilter::CalcMinDistance(const std::wstring& first, const std::wstring& second) {
	auto sig_a = findMin(first);
	auto sig_b = findMin(second);
	return GetMinDistance(sig_a, sig_b);
}
double Prefilter::GetMinDistance(const std::vector<int>& first, const std::vector<int>& second) {
	size_t dist = 0;
	for (size_t i = 0; i < first.size(); ++i) {
		if (first[i] == second[i]) {
			dist++;
		}
	}
	return dist / static_cast<double>(seeds_.size());
}
