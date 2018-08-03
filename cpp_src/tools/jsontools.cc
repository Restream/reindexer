#include <string.h>
#include <tools/errors.h>
#include <vector>
#include "gason/gason.h"
#include "stringstools.h"

namespace reindexer {

void parseJsonField(const char *name, string &ref, const JsonNode *elem) {
	if (strcmp(name, elem->key)) return;
	if (elem->value.getTag() == JSON_STRING) {
		ref = elem->value.toString();
	} else
		throw Error(errParseJson, "Expected string setting '%s'", name);
}

void parseJsonField(const char *name, bool &ref, const JsonNode *elem) {
	if (strcmp(name, elem->key)) return;
	if (elem->value.getTag() == JSON_TRUE) {
		ref = true;
	} else if (elem->value.getTag() == JSON_FALSE) {
		ref = false;
	} else
		throw Error(errParseJson, "Expected value `true` of `false` for setting '%s'", name);
}

template <typename T>
void parseJsonField(const char *name, T &ref, const JsonNode *elem, double min, double max) {
	if (strcmp(name, elem->key)) return;
	if (elem->value.getTag() == JSON_NUMBER) {
		T v = elem->value.toNumber();
		if (v < min || v > max) {
			throw Error(errParseJson, "Value of setting '%s' is out of range [%g,%g]", name, min, max);
		}
		ref = v;
	} else
		throw Error(errParseJson, "Expected type number for setting '%s'", name);
}

template void parseJsonField(const char *, int &, const JsonNode *, double, double);
template void parseJsonField(const char *, size_t &, const JsonNode *, double, double);
template void parseJsonField(const char *, double &, const JsonNode *, double, double);

}  // namespace reindexer
