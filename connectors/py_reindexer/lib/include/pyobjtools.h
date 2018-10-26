#pragma once

#include <Python.h>
#include <cmath>
#include <vector>
#include "tools/jsontools.h"
#include "tools/serializer.h"

namespace pyreindexer {

using reindexer::Error;
using reindexer::WrSerializer;
using std::vector;
using std::string;

void pyValueSerialize(PyObject **item, WrSerializer &wrSer);
void pyListSerialize(PyObject **list, WrSerializer &wrSer);
void pyDictSerialize(PyObject **dict, WrSerializer &wrSer);
PyObject *pyValueFromJsonValue(const JsonValue &value);

vector<string> ParseListToStrVec(PyObject **dict);

void PyObjectToJson(PyObject **dict, WrSerializer &wrSer);
PyObject *PyObjectFromJson(char *json);

}  // namespace pyreindexer
