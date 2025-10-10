#pragma once
#include <array>
#include <bitset>
#include <cstdint>
#include <string>
#include <vector>

class [[nodiscard]] Prefilter {
public:
	Prefilter();
	size_t CalcSimDistance(const std::wstring& first, const std::wstring& second);
	double CalcMinDistance(const std::wstring& first, const std::wstring& second);

private:
	std::array<uint32_t, 20> seeds_;
	size_t GetSimDistance(const std::bitset<32> first, const std::bitset<32> second);
	double GetMinDistance(const std::vector<int>& first, const std::vector<int>& second);

	std::bitset<32> GetSimHash(const std::wstring& first);
	std::vector<int> findMin(const std::wstring& first);
};
