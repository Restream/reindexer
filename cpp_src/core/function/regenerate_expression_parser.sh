#!/usr/bin/env bash
set -euo pipefail

dir=$(cd "$(dirname "$0")" && pwd)
cd "$dir"

bison_bin=${1:-bison}
"$bison_bin" -o expression_yy.cc --defines=expression_yy.hh expression.y

reserve_parser_stack() {
	local f=$1
	local tmp
	tmp=$(mktemp)
	if ! awk '
		ctor && $0 == "        : seq_ (n)" { print "      {"; print "        seq_.reserve (n);"; body = 1; ctor = 0; patched = 1; next }
		body && $0 == "      {}" { print "      }"; body = 0; next }
		$0 == "      stack (size_type n = 200) YY_NOEXCEPT" { print "      stack (size_type n = 16) YY_NOEXCEPT"; ctor = 1; next }
		{ ctor = 0; print }
		END { exit !patched }
	' "$f" >"$tmp"; then
		rm -f "$tmp"
		echo "Failed to patch the parser stack constructor in $f" >&2
		exit 1
	fi
	mv "$tmp" "$f"
}

reserve_parser_stack expression_yy.hh

wrap_nolicint() {
	local f=$1
	local tmp
	tmp=$(mktemp)
	awk '
		NR == 1 && $0 == "// NOLINTBEGIN" { next }
		{ buf[++n] = $0 }
		END {
			while (n > 0 && buf[n] == "") n--
			if (n > 0 && buf[n] == "// NOLINTEND") n--
			while (n > 0 && buf[n] == "") n--
			print "// NOLINTBEGIN"
			for (i = 1; i <= n; i++) print buf[i]
			print "// NOLINTEND"
		}
	' "$f" >"$tmp"
	mv "$tmp" "$f"
}

wrap_nolicint expression_yy.cc
wrap_nolicint expression_yy.hh
