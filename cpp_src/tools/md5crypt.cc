/*
 * md5crypt function in this file is based on OpenSSL sources
 * Copyright 2000-2018 The OpenSSL Project Authors. All Rights Reserved.
 *
 * Licensed under the Apache License 2.0 (the "License").  You may not use
 * this file except in compliance with the License.  You can obtain a copy
 * in the file LICENSE in the source distribution or at
 * https://www.openssl.org/source/license.html
 */

#include "crypt.h"
#include "estl/fast_hash_map.h"
#include "tools/stringstools.h"
#include "vendor/hash/md5.h"

namespace reindexer {

constexpr std::string_view kCryptDelimiter = "$";

constexpr std::string_view kMD5CryptMagic{"1"};
#if WITH_OPENSSL
constexpr std::string_view kSHA256CryptMagic{"5"};
constexpr std::string_view kSHA512CryptMagic{"6"};
#endif

static const fast_hash_map<std::string_view, HashAlgorithm> kHashOptions{{kMD5CryptMagic, HashAlgorithm::MD5}
#if WITH_OPENSSL
																		 ,
																		 {kSHA256CryptMagic, HashAlgorithm::SHA256},
																		 {kSHA512CryptMagic, HashAlgorithm::SHA512}
#endif
};

std::string MD5crypt(const std::string& passwd, const std::string& salt) noexcept {
	static const unsigned char cov2char[64] = {
		/* from crypto/des/fcrypt.c */
		0x2E, 0x2F, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A,
		0x4B, 0x4C, 0x4D, 0x4E, 0x4F, 0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66,
		0x67, 0x68, 0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7A};

	unsigned char buf[MD5::HashBytes];
	char trunkatedSalt[9] = {0}; /* Max 8 chars plus '\0' */

	/* The salt gets truncated to 8 chars */
	strncpy(trunkatedSalt, salt.c_str(), sizeof(trunkatedSalt) - 1);
	auto saltLen = strlen(trunkatedSalt);
	{
		MD5 md;
		md.add(passwd.c_str(), passwd.size());
		md.add(kCryptDelimiter.data(), kCryptDelimiter.size());
		md.add(reindexer::kMD5CryptMagic.data(), reindexer::kMD5CryptMagic.size());
		md.add(kCryptDelimiter.data(), kCryptDelimiter.size());
		md.add(trunkatedSalt, saltLen);

		MD5 md2;
		md2.add(passwd.c_str(), passwd.size());
		md2.add(trunkatedSalt, saltLen);
		md2.add(passwd.c_str(), passwd.size());
		md2.getHash(buf);

		size_t i = 0;
		for (i = passwd.size(); i > sizeof(buf); i -= sizeof(buf)) {
			md.add(buf, sizeof(buf));
		}
		md.add(buf, i);

		auto n = passwd.size();
		while (n) {
			md.add((n & 1) ? "\0" : passwd.c_str(), 1);
			n >>= 1;
		}
		md.getHash(buf);
	}

	for (size_t i = 0; i < 1000; i++) {
		MD5 md;
		md.add((i & 1) ? reinterpret_cast<const unsigned char*>(passwd.c_str()) : buf, (i & 1) ? passwd.size() : sizeof(buf));
		if (i % 3) {
			md.add(trunkatedSalt, saltLen);
		}
		if (i % 7) {
			md.add(passwd.c_str(), passwd.size());
		}
		md.add((i & 1) ? buf : reinterpret_cast<const unsigned char*>(passwd.c_str()), (i & 1) ? sizeof(buf) : passwd.size());
		md.getHash(buf);
	}

	char resultBuf[23] = {0};
	/* transform buf into output string */
	unsigned char bufPerm[sizeof(buf)];
	char* output = resultBuf;
	for (int dest = 0, source = 0; dest < 14; dest++, source = (source + 6) % 17) {
		bufPerm[dest] = buf[source];
	}
	bufPerm[14] = buf[5];
	bufPerm[15] = buf[11];

	for (size_t i = 0; i < 15; i += 3) {
		*output++ = cov2char[bufPerm[i + 2] & 0x3f];
		*output++ = cov2char[((bufPerm[i + 1] & 0xf) << 2) | (bufPerm[i + 2] >> 6)];
		*output++ = cov2char[((bufPerm[i] & 3) << 4) | (bufPerm[i + 1] >> 4)];
		*output++ = cov2char[bufPerm[i] >> 2];
	}
	*output++ = cov2char[bufPerm[15] & 0x3f];
	*output++ = cov2char[bufPerm[15] >> 6];
	*output = 0;

	return std::string(resultBuf);
}

Error ParseCryptString(const std::string& input, std::string& outHash, std::string& outSalt, HashAlgorithm& hashAlgorithm) {
	if (input.empty() || input.find(kCryptDelimiter) != 0) {
		outHash = input;
		outSalt.clear();
		return errOK;
	} else {
		std::vector<std::string> hashParts;
		std::ignore = split(input, kCryptDelimiter, false, hashParts);
		if (hashParts.size() != 4) {
			return Error(errParams, "Unexpected hash format. Expectig '$type$salt$hash");
		}
		if (auto it = kHashOptions.find(hashParts[1]); it != kHashOptions.end()) {
			hashAlgorithm = it->second;
		} else {
			return Error(errParams, "Unsupported hash magic: {}", hashParts[1].c_str());
		}
		outHash = std::move(hashParts[3]);
		outSalt = std::move(hashParts[2]);
	}
	return errOK;
}

}  // namespace reindexer
