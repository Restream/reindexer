#include "typos.h"

namespace reindexer {

RX_ALWAYS_INLINE unsigned uabs(int a) { return unsigned(std::abs(a)); }

bool TyposHandler::checkMaxTyposDist(const WordTypo& found, const TyposVec& current) {
	static_assert(kMaxTyposInWord <= 2, "Code in this function is expecting specific size of the typos positions arrays");
	if (!useMaxTypoDist_ || found.positions.size() == 0) {
		return true;
	}
	switch (current.size()) {
		case 0:
			return true;
		case 1: {
			const auto curP0 = current[0];
			const auto foundP0 = found.positions[0];

			if (found.positions.size() == 1) {
				// current.len == 1 && found.len == 1. I.e. exactly one letter must be changed and moved up to maxTypoDist_ value
				return uabs(curP0 - foundP0) <= maxTypoDist_;
			}
			// current.len == 1 && found.len == 2. I.e. exactly one letter must be changed and moved up to maxTypoDist_ value and the
			// other letter is missing in 'current'
			auto foundLeft = foundP0;
			auto foundRight = found.positions[1];
			if (foundLeft > foundRight) {
				std::swap(foundLeft, foundRight);
			}
			return uabs((foundRight - 1) - curP0) <= maxTypoDist_ || uabs(foundLeft - curP0) <= maxTypoDist_;
		}
		case 2: {
			const auto foundP0 = found.positions[0];
			const auto curP0 = current[0];
			const auto curP1 = current[1];

			if (found.positions.size() == 1) {
				// current.len == 2 && found.len == 1. I.e. exactly one letter must be changed and moved up to maxTypoDist_ value and
				// 'current' also has one extra letter
				auto curLeft = curP0;
				auto curRight = curP1;
				if (curLeft > curRight) {
					std::swap(curLeft, curRight);
				}

				return uabs((curRight - 1) - foundP0) <= maxTypoDist_ || uabs(curLeft - foundP0) <= maxTypoDist_;
			}

			// current.len == 2 && found.len == 2. I.e. exactly two letters must be changed and moved up to maxTypoDist_ value
			const auto foundP1 = found.positions[1];
			return ((uabs(curP0 - foundP0) <= maxTypoDist_) && (uabs(curP1 - foundP1) <= maxTypoDist_)) ||
				   ((uabs(curP0 - foundP1) <= maxTypoDist_) && (uabs(curP1 - foundP0) <= maxTypoDist_));
		}
		default:
			throw Error(errLogic, "Unexpected typos count: {}", current.size());
	}
}

bool TyposHandler::checkMaxLettPermDist(std::string_view foundWord, const WordTypo& found, std::wstring_view currentWord,
										const TyposVec& current) {
	if (found.positions.size() == 0) {
		return true;
	}
	static_assert(kMaxTyposInWord <= 2, "Code in this function is expecting specific size of the typos positions arrays");
	utf8_to_utf16(foundWord, foundWordUTF16_);
	switch (current.size()) {
		case 0:
			throw Error(errLogic, "Internal logic error. Unable to handle max_typos_distance or max_symbol_permutation_distance settings");
		case 1: {
			const auto foundP0 = found.positions[0];
			const auto curP0 = current[0];
			if (foundWordUTF16_[foundP0] == currentWord[curP0] && (!useMaxLettPermDist_ || uabs(curP0 - foundP0) <= maxLettPermDist_)) {
				return true;
			}
			const auto foundP1 = found.positions[1];
			return (found.positions.size() == 2 && foundWordUTF16_[foundP1] == currentWord[curP0] &&
					(!useMaxLettPermDist_ || uabs(curP0 - foundP1) <= maxLettPermDist_));

			if (found.positions.size() == 1) {
				// current.len == 1 && found.len == 1. I.e. exactly one letter must be moved up to maxLettPermDist_ value
				return (foundWordUTF16_[foundP0] == currentWord[curP0]) &&
					   (!useMaxLettPermDist_ || uabs(curP0 - foundP0) <= maxLettPermDist_);
			}
			// current.len == 1 && found.len == 2. I.e. exactly one letter must be moved up to maxLettPermDist_ value and the other
			// letter is missing in 'current'
			auto foundLeft = foundP0;
			auto foundRight = found.positions[1];
			if (foundLeft > foundRight) {
				std::swap(foundLeft, foundRight);
			}

			// Right letter position requires correction for the comparison with distance, but not for the letter itself
			const auto foundRightLetter = foundWordUTF16_[foundRight--];
			const auto foundLeftLetter = foundWordUTF16_[foundLeft];
			const auto curP0Letter = currentWord[curP0];
			return (foundRightLetter == curP0Letter && (!useMaxLettPermDist_ || uabs(foundRight - curP0) <= maxLettPermDist_)) ||
				   (foundLeftLetter == curP0Letter && (!useMaxLettPermDist_ || uabs(foundLeft - curP0) <= maxLettPermDist_));
		}
		case 2: {
			const auto foundP0 = found.positions[0];
			const auto curP0 = current[0];
			const auto curP1 = current[1];

			if (found.positions.size() == 1) {
				// current.len == 2 && found.len == 1. I.e. exactly one letter must be moved up to maxLettPermDist_ value and 'current'
				// also has one extra letter
				auto curLeft = curP0;
				auto curRight = curP1;
				if (curLeft > curRight) {
					std::swap(curLeft, curRight);
				}
				// Right letter position requires correction for the comparison with distance, but not for the letter itself
				const auto curRightLetter = currentWord[curRight--];
				const auto curLeftLetter = currentWord[curLeft];
				const auto foundP0Letter = foundWordUTF16_[foundP0];
				return (foundP0Letter == curRightLetter && (!useMaxLettPermDist_ || uabs((curRight - 1) - foundP0) <= maxLettPermDist_)) ||
					   (foundP0Letter == curLeftLetter && (!useMaxLettPermDist_ || uabs(curLeft - foundP0) <= maxLettPermDist_));
			}

			// current.len == 2 && found.len == 2. I.e. two letters must be moved up to maxLettPermDist_ value
			const auto foundP1 = found.positions[1];
			const auto foundP0Letter = foundWordUTF16_[foundP0];
			const auto foundP1Letter = foundWordUTF16_[foundP1];
			const auto curP0Letter = currentWord[curP0];
			const auto curP1Letter = currentWord[curP1];
			const bool permutationOn00 =
				(foundP0Letter == curP0Letter && (!useMaxLettPermDist_ || uabs(curP0 - foundP0) <= maxLettPermDist_));
			const bool permutationOn11 =
				(foundP1Letter == curP1Letter && (!useMaxLettPermDist_ || uabs(curP1 - foundP1) <= maxLettPermDist_));
			if (permutationOn00 && permutationOn11) {
				return true;
			}
			const bool permutationOn01 =
				(foundP0Letter == curP1Letter && (!useMaxLettPermDist_ || uabs(curP1 - foundP0) <= maxLettPermDist_));
			const bool permutationOn10 =
				(foundP1Letter == curP0Letter && (!useMaxLettPermDist_ || uabs(curP0 - foundP1) <= maxLettPermDist_));
			if (permutationOn01 && permutationOn10) {
				return true;
			}
			const bool switchOn00 = (uabs(curP0 - foundP0) <= maxTypoDist_);
			if (permutationOn11 && switchOn00) {
				return true;
			}
			const bool switchOn11 = (uabs(curP1 - foundP1) <= maxTypoDist_);
			if (permutationOn00 && switchOn11) {
				return true;
			}
			const bool switchOn10 = (uabs(curP0 - foundP1) <= maxTypoDist_);
			if (permutationOn01 && switchOn10) {
				return true;
			}
			const bool switchOn01 = (uabs(curP1 - foundP0) <= maxTypoDist_);
			return permutationOn10 && switchOn01;
		}
		default:
			throw Error(errLogic, "Unexpected typos count: {}", current.size());
	}
}

}  // namespace reindexer