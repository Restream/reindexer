/*
 * Copyright (C) Alex Nekipelov (alex@nekipelov.net)
 * License: MIT
 */

#pragma once
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include "tools/assertrx.h"

namespace httpparser {

class UrlParser {
public:
	UrlParser() noexcept : valid(false) {}

	explicit UrlParser(const std::string& url) : valid(true) { parse(url); }

	bool parse(const std::string& str) {
		url = Url();
		parse_(str);

		return isValid();
	}

	bool isValid() const { return valid; }

	const std::string& scheme() const {
		assertrx(isValid());
		return url.scheme;
	}

	const std::string& username() const {
		assertrx(isValid());
		return url.username;
	}

	const std::string& password() const {
		assertrx(isValid());
		return url.password;
	}

	const std::string& hostname() const {
		assertrx(isValid());
		return url.hostname;
	}

	const std::string& port() const {
		assertrx(isValid());
		return url.port;
	}

	const std::string& path() const {
		assertrx(isValid());
		return url.path;
	}

	std::string db() const {
		assert(isValid());
		std::string::size_type pos = 0;
		while (pos != url.path.size() && (url.path[pos] == '\\' || url.path[pos] == '/')) {
			++pos;
		}
		return url.path.substr(pos);
	}

	const std::string& query() const {
		assertrx(isValid());
		return url.query;
	}

	const std::string& fragment() const {
		assertrx(isValid());
		return url.fragment;
	}

	uint16_t httpPort() const {
		const uint16_t defaultHttpPort = 80;
		const uint16_t defaultHttpsPort = 443;

		assertrx(isValid());

		if (url.port.empty()) {
			if (scheme() == "https")
				return defaultHttpsPort;
			else
				return defaultHttpPort;
		} else {
			return url.integerPort;
		}
	}

private:
	bool isUnreserved(char ch) const {
		if (isalnum(ch)) return true;

		switch (ch) {
			case '-':
			case '.':
			case '_':
			case '~':
				return true;
		}

		return false;
	}

	void parse_(const std::string& str) {
		enum {
			Scheme,
			SlashAfterScheme1,
			SlashAfterScheme2,
			UsernameOrHostname,
			Password,
			Hostname,
			IPV6Hostname,
			PortOrPassword,
			Port,
			Path,
			Query,
			Fragment
		} state = Scheme;

		std::string usernameOrHostname;
		std::string portOrPassword;

		valid = true;
#if !defined(_MSC_VER) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wrestrict"
#endif
		url.path = "/";
#if !defined(_MSC_VER) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
		url.integerPort = 0;

		for (size_t i = 0; i < str.size() && valid; ++i) {
			char ch = str[i];

			switch (state) {
				case Scheme:
					if (isalnum(ch) || ch == '+' || ch == '-' || ch == '.') {
						url.scheme += ch;
					} else if (ch == ':') {
						state = SlashAfterScheme1;
					} else {
						valid = false;
						url = Url();
					}
					break;
				case SlashAfterScheme1:
					if (ch == '/') {
						state = SlashAfterScheme2;
					} else if (isalnum(ch)) {
						usernameOrHostname = ch;
						state = UsernameOrHostname;
					} else {
						valid = false;
						url = Url();
					}
					break;
				case SlashAfterScheme2:
					if (ch == '/') {
						state = UsernameOrHostname;
					} else {
						valid = false;
						url = Url();
					}
					break;
				case UsernameOrHostname:
					if (isUnreserved(ch) || ch == '%') {
						usernameOrHostname += ch;
					} else if (ch == ':') {
						state = PortOrPassword;
					} else if (ch == '@') {
						state = Hostname;
						std::swap(url.username, usernameOrHostname);
					} else if (ch == '/') {
						state = Path;
						std::swap(url.hostname, usernameOrHostname);
					} else {
						valid = false;
						url = Url();
					}
					break;
				case Password:
					if (isalnum(ch) || ch == '%') {
						url.password += ch;
					} else if (ch == '@') {
						state = Hostname;
					} else {
						valid = false;
						url = Url();
					}
					break;
				case Hostname:
					if (ch == '[' && url.hostname.empty()) {
						state = IPV6Hostname;
					} else if (isUnreserved(ch) || ch == '%') {
						url.hostname += ch;
					} else if (ch == ':') {
						state = Port;
					} else if (ch == '/') {
						state = Path;
					} else {
						valid = false;
						url = Url();
					}
					break;
				case IPV6Hostname:
					abort();  // TODO
				case PortOrPassword:
					if (isdigit(ch)) {
						portOrPassword += ch;
					} else if (ch == '/') {
						std::swap(url.hostname, usernameOrHostname);
						std::swap(url.port, portOrPassword);
						url.integerPort = atoi(url.port.c_str());
						state = Path;
					} else if (isalnum(ch) || ch == '%') {
						std::swap(url.username, usernameOrHostname);
						std::swap(url.password, portOrPassword);
						url.password += ch;
						state = Password;
					} else if (ch == '@') {
						std::swap(url.username, usernameOrHostname);
						std::swap(url.password, portOrPassword);
						state = Hostname;
					} else {
						valid = false;
						url = Url();
					}
					break;
				case Port:
					if (isdigit(ch)) {
						portOrPassword += ch;
					} else if (ch == '/') {
						std::swap(url.port, portOrPassword);
						url.integerPort = atoi(url.port.c_str());
						state = Path;
					} else {
						valid = false;
						url = Url();
					}
					break;
				case Path:
					if (ch == '#') {
						state = Fragment;
					} else if (ch == '?') {
						state = Query;
					} else {
						url.path += ch;
					}
					break;
				case Query:
					if (ch == '#') {
						state = Fragment;
					} else if (ch != '?') {
						url.query += ch;
					}
					break;
				case Fragment:
					url.fragment += ch;
					break;
			}
		}

		if (state == PortOrPassword || state == Port) {
			std::swap(url.port, portOrPassword);
			url.integerPort = atoi(url.port.c_str());
		}

		assertrx(portOrPassword.empty());

		if (!usernameOrHostname.empty()) url.hostname = usernameOrHostname;
	}

	bool valid;

	struct Url {
		Url() : integerPort(0) {}

		std::string scheme;
		std::string username;
		std::string password;
		std::string hostname;
		std::string port;
		std::string path;
		std::string query;
		std::string fragment;
		uint16_t integerPort;
	} url;
};

}  // namespace httpparser
