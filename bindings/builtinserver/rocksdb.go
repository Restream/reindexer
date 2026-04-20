//go:build rocksdb
// +build rocksdb

package builtinserver

// #cgo LDFLAGS: -lrocksdb -lz
import "C"
