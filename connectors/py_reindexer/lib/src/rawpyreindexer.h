#pragma once

#ifdef PYREINDEXER_CPROTO
#define MODULE_NAME "RawPyReindexer_cproto"
#define MODULE_DESCRIPTION "A cproto connector that allows to interact with Reindexer from Python"
#define MODULE_EXPORT_FUNCTION PyInit_rawpyreindexerc
#else
#define MODULE_NAME "RawPyReindexer_builtin"
#define MODULE_DESCRIPTION "A builtin connector that allows to interact with Reindexer from Python"
#define MODULE_EXPORT_FUNCTION PyInit_rawpyreindexerb
#endif

#include <Python.h>

#include "pyobjtools.h"
#include "queryresults_wrapper.h"
#include "tools/serializer.h"

#ifdef PYREINDEXER_CPROTO
#include "client/reindexer.h"
#else
#include "core/reindexer.h"
#endif

namespace pyreindexer {

using reindexer::Error;
using reindexer::IndexDef;
using reindexer::NamespaceDef;
using reindexer::WrSerializer;

#ifdef PYREINDEXER_CPROTO
using reindexer::client::Item;
using reindexer::client::QueryResults;
using reindexer::client::Reindexer;
#else
using reindexer::Item;
using reindexer::QueryResults;
using reindexer::Reindexer;
#endif

inline static uintptr_t initReindexer() {
	Reindexer *db = new Reindexer();

	return reinterpret_cast<uintptr_t>(db);
}

inline static Reindexer *getDB(uintptr_t rx) { return reinterpret_cast<Reindexer *>(rx); }

inline static void destroyReindexer(uintptr_t rx) {
	Reindexer *db = getDB(rx);

	delete db;
}

inline static PyObject *pyErr(const Error &err) { return Py_BuildValue("is", err.code(), err.what().c_str()); }

inline static QueryResultsWrapper *getQueryResultsWrapper(uintptr_t qresWrapperAddr) {
	return reinterpret_cast<QueryResultsWrapper *>(qresWrapperAddr);
}

static void queryResultsWrapperDelete(uintptr_t qresWrapperAddr);

static PyObject *Init(PyObject *self, PyObject *args);
static PyObject *Destroy(PyObject *self, PyObject *args);
static PyObject *Connect(PyObject *self, PyObject *args);
static PyObject *NamespaceOpen(PyObject *self, PyObject *args);
static PyObject *NamespaceClose(PyObject *self, PyObject *args);
static PyObject *NamespaceDrop(PyObject *self, PyObject *args);
static PyObject *IndexAdd(PyObject *self, PyObject *args);
static PyObject *IndexUpdate(PyObject *self, PyObject *args);
static PyObject *IndexDrop(PyObject *self, PyObject *args);
static PyObject *ItemInsert(PyObject *self, PyObject *args);
static PyObject *ItemUpdate(PyObject *self, PyObject *args);
static PyObject *ItemUpsert(PyObject *self, PyObject *args);
static PyObject *ItemDelete(PyObject *self, PyObject *args);
static PyObject *Commit(PyObject *self, PyObject *args);
static PyObject *PutMeta(PyObject *self, PyObject *args);
static PyObject *GetMeta(PyObject *self, PyObject *args);
static PyObject *Select(PyObject *self, PyObject *args);
static PyObject *EnumMeta(PyObject *self, PyObject *args);
static PyObject *EnumNamespaces(PyObject *self, PyObject *args);

static PyObject *QueryResultsWrapperIterate(PyObject *self, PyObject *args);
static PyObject *QueryResultsWrapperDelete(PyObject *self, PyObject *args);

// clang-format off
static PyMethodDef module_methods[] = {
	{"init", Init, METH_NOARGS, "init reindexer instance"},
	{"destroy", Destroy, METH_VARARGS, "destroy reindexer instance"},
	{"connect", Connect, METH_VARARGS, "connect to reindexer database"},
	{"namespace_open", NamespaceOpen, METH_VARARGS, "open namespace"},
	{"namespace_close", NamespaceClose, METH_VARARGS, "close namespace"},
	{"namespace_drop", NamespaceDrop, METH_VARARGS, "drop namespace"},
	{"namespaces_enum", EnumNamespaces, METH_VARARGS, "enum namespaces"},
	{"index_add", IndexAdd, METH_VARARGS, "add index"},
	{"index_update", IndexUpdate, METH_VARARGS, "update index"},
	{"index_drop", IndexDrop, METH_VARARGS, "drop index"},
	{"item_insert", ItemInsert, METH_VARARGS, "insert item"},
	{"item_update", ItemUpdate, METH_VARARGS, "update item"},
	{"item_upsert", ItemUpsert, METH_VARARGS, "upsert item"},
	{"item_delete", ItemDelete, METH_VARARGS, "delete item"},
	{"commit", Commit, METH_VARARGS, "commit"},
	{"meta_put", PutMeta, METH_VARARGS, "put meta"},
	{"meta_get", GetMeta, METH_VARARGS, "get meta"},
	{"meta_enum", EnumMeta, METH_VARARGS, "enum meta"},
	{"select", Select, METH_VARARGS, "select query"},

	{"query_results_iterate", QueryResultsWrapperIterate, METH_VARARGS, "get query result"},
	{"query_results_delete", QueryResultsWrapperDelete, METH_VARARGS, "free query results buffer"},

	{NULL, NULL, 0, NULL}
};
// clang-format on

static struct PyModuleDef module_definition = {PyModuleDef_HEAD_INIT, MODULE_NAME, MODULE_DESCRIPTION, -1, module_methods};

PyMODINIT_FUNC MODULE_EXPORT_FUNCTION(void) {
	Py_Initialize();
	return PyModule_Create(&module_definition);
}

}  // namespace pyreindexer
