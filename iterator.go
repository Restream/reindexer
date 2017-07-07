package reindexer

import (
	"reflect"
	"sync"
)

var iterPool sync.Pool

type Iterator struct {
	data []interface{}
	ptr  int
	t    reflect.Type
	err  error
	pool bool
}

func newIterator(data []interface{}, err error, t reflect.Type) *Iterator {
	obj := iterPool.Get()
	if obj != nil {
		return initIterator(obj.(*Iterator), data, err, t)
	}
	return &Iterator{data: data, ptr: -1, t: t, err: err, pool: true}
}

func initIterator(it *Iterator, data []interface{}, err error, t reflect.Type) *Iterator {
	it.data = data
	it.t = t
	it.err = err
	it.ptr = -1
	return it
}

// PtrSlice fetches all query results as slice of pounters to objects, and closes iterator
// res,err := iter.PtrSlice ()
// item := res[n].(*TypeOfObject)
func (it *Iterator) PtrSlice() ([]interface{}, error) {
	defer it.Close()
	return it.data, it.err
}

// Ptr returns current result as pointer
// res,err := iter.Ptr ()
// item := res.(*TypeOfObject)
func (it *Iterator) Ptr() interface{} {
	return it.data[it.ptr]
}

// Next iterate to next query result. Return false on query end
// On query end iterator auto closes
func (it *Iterator) Next() bool {
	it.ptr++
	return it.ptr < len(it.data)
}

// Close iterator
func (it *Iterator) Close() {
	// now do nothing
	if it.pool {
		iterPool.Put(it)
	}
}

// Error return error
func (it *Iterator) Error() error {
	return it.err
}

// Len return len of data
func (it *Iterator) Len() int {
	return len(it.data)
}
