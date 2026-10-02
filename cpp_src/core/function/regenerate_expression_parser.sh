#!/usr/bin/env bash
set -euo pipefail

dir=$(cd "$(dirname "$0")" && pwd)
cd "$dir"

bison_bin=${1:-bison}
"$bison_bin" -o expression_yy.cc --defines=expression_yy.hh expression.y

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
