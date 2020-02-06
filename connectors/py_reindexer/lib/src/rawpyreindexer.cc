#include "rawpyreindexer.h"

namespace pyreindexer {

static void queryResultsWrapperDelete(uintptr_t qresWrapperAddr) {
	QueryResultsWrapper *qresWrapperPtr = getQueryResultsWrapper(qresWrapperAddr);

	delete qresWrapperPtr;
}

static PyObject *queryResultsWrapperIterate(uintptr_t qresWrapperAddr) {
	QueryResultsWrapper *qresWrapperPtr = getQueryResultsWrapper(qresWrapperAddr);

	WrSerializer wrSer;
	qresWrapperPtr->itPtr.GetJSON(wrSer, false);

	qresWrapperPtr->next();

	PyObject *dictFromJson = nullptr;
	try {
		dictFromJson = PyObjectFromJson(giftStr(wrSer.Slice()));  // stolen ref
	} catch (const Error &err) {
		Py_XDECREF(dictFromJson);

		return Py_BuildValue("is{}", err.code(), err.what().c_str());
	}

	return Py_BuildValue("isO", errOK, "", dictFromJson);
}

static PyObject *Init(PyObject *self, PyObject *args) {
	uintptr_t rx = initReindexer();

	if (rx == 0) {
		PyErr_SetString(PyExc_RuntimeError, "Initialization error");

		return NULL;
	}

	return Py_BuildValue("k", rx);
}

static PyObject *Destroy(PyObject *self, PyObject *args) {
	uintptr_t rx;

	if (!PyArg_ParseTuple(args, "k", &rx)) {
		return NULL;
	}

	destroyReindexer(rx);

	Py_RETURN_NONE;
}

static PyObject *Connect(PyObject *self, PyObject *args) {
	uintptr_t rx;
	char *dsn;

	if (!PyArg_ParseTuple(args, "ks", &rx, &dsn)) {
		return NULL;
	}

	Error err = getDB(rx)->Connect(dsn);

	return pyErr(err);
}

static PyObject *NamespaceOpen(PyObject *self, PyObject *args) {
	uintptr_t rx;
	char *ns;

	if (!PyArg_ParseTuple(args, "ks", &rx, &ns)) {
		return NULL;
	}

	Error err = getDB(rx)->OpenNamespace(ns);

	return pyErr(err);
}

static PyObject *NamespaceClose(PyObject *self, PyObject *args) {
	uintptr_t rx;
	char *ns;

	if (!PyArg_ParseTuple(args, "ks", &rx, &ns)) {
		return NULL;
	}

	Error err = getDB(rx)->CloseNamespace(ns);

	return pyErr(err);
}

static PyObject *NamespaceDrop(PyObject *self, PyObject *args) {
	uintptr_t rx;
	char *ns;

	if (!PyArg_ParseTuple(args, "ks", &rx, &ns)) {
		return NULL;
	}

	Error err = getDB(rx)->DropNamespace(ns);

	return pyErr(err);
}

static PyObject *IndexAdd(PyObject *self, PyObject *args) {
	uintptr_t rx;
	char *ns;
	PyObject *indexDefDict = NULL;	// borrowed ref after ParseTuple

	if (!PyArg_ParseTuple(args, "ksO!", &rx, &ns, &PyDict_Type, &indexDefDict)) {
		return NULL;
	}

	Py_INCREF(indexDefDict);

	WrSerializer wrSer;

	try {
		PyObjectToJson(&indexDefDict, wrSer);
	} catch (const Error &err) {
		Py_DECREF(indexDefDict);

		return pyErr(err);
	}

	Py_DECREF(indexDefDict);

	IndexDef indexDef;
	Error err = indexDef.FromJSON(giftStr(wrSer.Slice()));
	if (!err.ok()) {
		return pyErr(err);
	}

	err = getDB(rx)->AddIndex(ns, indexDef);

	return pyErr(err);
}

static PyObject *IndexUpdate(PyObject *self, PyObject *args) {
	uintptr_t rx;
	char *ns;
	PyObject *indexDefDict = NULL;	// borrowed ref after ParseTuple

	if (!PyArg_ParseTuple(args, "ksO!", &rx, &ns, &PyDict_Type, &indexDefDict)) {
		return NULL;
	}

	Py_INCREF(indexDefDict);

	WrSerializer wrSer;

	try {
		PyObjectToJson(&indexDefDict, wrSer);
	} catch (const Error &err) {
		Py_DECREF(indexDefDict);

		return pyErr(err);
	}

	Py_DECREF(indexDefDict);

	IndexDef indexDef;
	Error err = indexDef.FromJSON(giftStr(wrSer.Slice()));
	if (!err.ok()) {
		return pyErr(err);
	}

	err = getDB(rx)->UpdateIndex(ns, indexDef);

	return pyErr(err);
}

static PyObject *IndexDrop(PyObject *self, PyObject *args) {
	uintptr_t rx;
	char *ns;
	char *indexName;

	if (!PyArg_ParseTuple(args, "kss", &rx, &ns, &indexName)) {
		return NULL;
	}

	Error err = getDB(rx)->DropIndex(ns, IndexDef(indexName));

	return pyErr(err);
}

static PyObject *itemModify(PyObject *self, PyObject *args, ItemModifyMode mode) {
	uintptr_t rx;
	char *ns;
	PyObject *itemDefDict = NULL;	// borrowed ref after ParseTuple
	PyObject *preceptsList = NULL;	// borrowed ref after ParseTuple if passed

	if (!PyArg_ParseTuple(args, "ksO!|O!", &rx, &ns, &PyDict_Type, &itemDefDict, &PyList_Type, &preceptsList)) {
		return NULL;
	}

	Py_INCREF(itemDefDict);
	Py_XINCREF(preceptsList);

	Item item = getDB(rx)->NewItem(ns);
	Error err = item.Status();
	if (!err.ok()) {
		return pyErr(err);
	}

	WrSerializer wrSer;

	try {
		PyObjectToJson(&itemDefDict, wrSer);
	} catch (const Error &err) {
		Py_DECREF(itemDefDict);
		Py_XDECREF(preceptsList);

		return pyErr(err);
	}

	Py_DECREF(itemDefDict);

	char *json = const_cast<char *>(wrSer.c_str());

	err = item.Unsafe().FromJSON(json, 0, mode == ModeDelete);
	if (!err.ok()) {
		return pyErr(err);
	}

	if (preceptsList != NULL && mode != ModeDelete) {
		vector<string> itemPrecepts;

		try {
			itemPrecepts = ParseListToStrVec(&preceptsList);
		} catch (const Error &err) {
			Py_DECREF(preceptsList);

			return pyErr(err);
		}

		item.SetPrecepts(itemPrecepts);
	}

	Py_XDECREF(preceptsList);

	switch (mode) {
		case ModeInsert:
			err = getDB(rx)->Insert(ns, item);
			break;
		case ModeUpdate:
			err = getDB(rx)->Update(ns, item);
			break;
		case ModeUpsert:
			err = getDB(rx)->Upsert(ns, item);
			break;
		case ModeDelete:
			err = getDB(rx)->Delete(ns, item);
			break;
		default:
			PyErr_SetString(PyExc_RuntimeError, "Unknown item modify mode");
			return NULL;
	}

	return pyErr(err);
}

static PyObject *ItemInsert(PyObject *self, PyObject *args) { return itemModify(self, args, ModeInsert); }

static PyObject *ItemUpdate(PyObject *self, PyObject *args) { return itemModify(self, args, ModeUpdate); }

static PyObject *ItemUpsert(PyObject *self, PyObject *args) { return itemModify(self, args, ModeUpsert); }

static PyObject *ItemDelete(PyObject *self, PyObject *args) { return itemModify(self, args, ModeDelete); }

static PyObject *Commit(PyObject *self, PyObject *args) {
	uintptr_t rx;
	char *ns;

	if (!PyArg_ParseTuple(args, "ks", &rx, &ns)) {
		return NULL;
	}

	Error err = getDB(rx)->Commit(ns);

	return pyErr(err);
}

static PyObject *PutMeta(PyObject *self, PyObject *args) {
	uintptr_t rx;
	char *ns;
	char *key;
	char *value;

	if (!PyArg_ParseTuple(args, "ksss", &rx, &ns, &key, &value)) {
		return NULL;
	}

	Error err = getDB(rx)->PutMeta(ns, key, value);

	return pyErr(err);
}

static PyObject *GetMeta(PyObject *self, PyObject *args) {
	uintptr_t rx;
	char *ns;
	char *key;

	if (!PyArg_ParseTuple(args, "kss", &rx, &ns, &key)) {
		return NULL;
	}

	string value;
	Error err = getDB(rx)->GetMeta(ns, key, value);

	return Py_BuildValue("iss", err.code(), err.what().c_str(), value.c_str());
}

static PyObject *Select(PyObject *self, PyObject *args) {
	uintptr_t rx;
	char *query;

	if (!PyArg_ParseTuple(args, "ks", &rx, &query)) {
		return NULL;
	}

	QueryResultsWrapper *qresWrapper = new QueryResultsWrapper();

	Error err = getDB(rx)->Select(query, qresWrapper->qresPtr);
	qresWrapper->iterInit();

	return Py_BuildValue("iskI", err.code(), err.what().c_str(), reinterpret_cast<uintptr_t>(qresWrapper), qresWrapper->qresPtr.Count());
}

static PyObject *EnumMeta(PyObject *self, PyObject *args) {
	uintptr_t rx;
	char *ns;

	if (!PyArg_ParseTuple(args, "ks", &rx, &ns)) {
		return NULL;
	}

	vector<string> keys;
	Error err = getDB(rx)->EnumMeta(ns, keys);
	if (!err.ok()) {
		return Py_BuildValue("is[]", err.code(), err.what().c_str());
	}

	PyObject *list = PyList_New(keys.size());  // new ref
	if (!list) {
		return NULL;
	}

	for (auto it = keys.begin(); it != keys.end(); it++) {
		unsigned pos = std::distance(keys.begin(), it);

		PyList_SetItem(list, pos, Py_BuildValue("s", it->c_str()));	 // stolen ref
	}

	PyObject *res = Py_BuildValue("isO", err.code(), err.what().c_str(), list);
	Py_DECREF(list);

	return res;
}

static PyObject *EnumNamespaces(PyObject *self, PyObject *args) {
	uintptr_t rx;
	unsigned enumAll;

	if (!PyArg_ParseTuple(args, "kI", &rx, &enumAll)) {
		return NULL;
	}

	vector<NamespaceDef> nsDefs;
	Error err = getDB(rx)->EnumNamespaces(nsDefs, reindexer::EnumNamespacesOpts().WithClosed(enumAll));
	if (!err.ok()) {
		return Py_BuildValue("is[]", err.code(), err.what().c_str());
	}

	PyObject *list = PyList_New(nsDefs.size());	 // new ref
	if (!list) {
		return NULL;
	}

	WrSerializer wrSer;
	for (auto it = nsDefs.begin(); it != nsDefs.end(); it++) {
		unsigned pos = std::distance(nsDefs.begin(), it);

		wrSer.Reset();
		it->GetJSON(wrSer, false);

		PyObject *dictFromJson = nullptr;
		try {
			dictFromJson = PyObjectFromJson(giftStr(wrSer.Slice()));  // stolen ref
		} catch (const Error &err) {
			Py_XDECREF(dictFromJson);
			Py_XDECREF(list);

			return Py_BuildValue("is{}", err.code(), err.what().c_str());
		}

		PyList_SetItem(list, pos, dictFromJson);  // stolen ref
	}

	PyObject *res = Py_BuildValue("isO", err.code(), err.what().c_str(), list);
	Py_DECREF(list);

	return res;
}

static PyObject *QueryResultsWrapperIterate(PyObject *self, PyObject *args) {
	uintptr_t qresWrapperAddr;

	if (!PyArg_ParseTuple(args, "k", &qresWrapperAddr)) {
		return NULL;
	}

	return queryResultsWrapperIterate(qresWrapperAddr);
}

static PyObject *QueryResultsWrapperDelete(PyObject *self, PyObject *args) {
	uintptr_t qresWrapperAddr;

	if (!PyArg_ParseTuple(args, "k", &qresWrapperAddr)) {
		return NULL;
	}

	queryResultsWrapperDelete(qresWrapperAddr);

	Py_RETURN_NONE;
}
}  // namespace pyreindexer
