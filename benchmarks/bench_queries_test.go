package main

import (
	"testing"

	"./repo"
)

func BenchmarkElasticGetByID(b *testing.B)            { repo.Get("elastic").QueryByID(b.N, false) }
func BenchmarkMongoGetByID(b *testing.B)              { repo.Get("mongo").QueryByID(b.N, false) }
func BenchmarkTarantoolGetByID(b *testing.B)          { repo.Get("tarantool").QueryByID(b.N, false) }
func BenchmarkSqliteGetByID(b *testing.B)             { repo.Get("sqlite").QueryByID(b.N, false) }
func BenchmarkSqliteGetByIDNoObject(b *testing.B)     { repo.Get("sqlite").QueryByID(b.N, true) }
func BenchmarkMysqlGetByID(b *testing.B)              { repo.Get("mysql").QueryByID(b.N, false) }
func BenchmarkMysqlGetByIDNoObject(b *testing.B)      { repo.Get("mysql").QueryByID(b.N, true) }
func BenchmarkClickhouseGetByID(b *testing.B)         { repo.Get("clickhouse").QueryByID(b.N, false) }
func BenchmarkClickhouseGetByIDNoObject(b *testing.B) { repo.Get("clickhouse").QueryByID(b.N, true) }
func BenchmarkReindexGetByID(b *testing.B)            { repo.Get("reindex").QueryByID(b.N, false) }
func BenchmarkReindexGetByIDUnsafe(b *testing.B)      { repo.Get("reindex").QueryByID(b.N, true) }
func BenchmarkRedisGetByID(b *testing.B)              { repo.Get("redis").QueryByID(b.N, false) }
func BenchmarkRedisGetByIDNoObject(b *testing.B)      { repo.Get("redis").QueryByID(b.N, true) }

func BenchmarkElastic1Cond(b *testing.B)         { repo.Get("elastic").Query1Cond(b.N, false, 10) }
func BenchmarkElastic1CondNoObj(b *testing.B)    { repo.Get("elastic").Query1Cond(b.N, true, 10) }
func BenchmarkMongo1Cond(b *testing.B)           { repo.Get("mongo").Query1Cond(b.N, false, 10) }
func BenchmarkMongo1CondNoObj(b *testing.B)      { repo.Get("mongo").Query1Cond(b.N, true, 10) }
func BenchmarkTarantool1Cond(b *testing.B)       { repo.Get("tarantool").Query1Cond(b.N, false, 10) }
func BenchmarkTarantool1CondNoObj(b *testing.B)  { repo.Get("tarantool").Query1Cond(b.N, true, 10) }
func BenchmarkSqlite1Cond(b *testing.B)          { repo.Get("sqlite").Query1Cond(b.N, false, 10) }
func BenchmarkSqlite1CondNoObj(b *testing.B)     { repo.Get("sqlite").Query1Cond(b.N, true, 10) }
func BenchmarkMysql1Cond(b *testing.B)           { repo.Get("mysql").Query1Cond(b.N, false, 10) }
func BenchmarkMysql1CondNoObj(b *testing.B)      { repo.Get("mysql").Query1Cond(b.N, true, 10) }
func BenchmarkReindex1Cond(b *testing.B)         { repo.Get("reindex").Query1Cond(b.N, false, 10) }
func BenchmarkReindex1CondObjCache(b *testing.B) { repo.Get("reindex").Query1Cond(b.N, true, 10) }
func BenchmarkRedis1Cond(b *testing.B)           { repo.Get("redis").Query1Cond(b.N, false, 10) }
func BenchmarkRedis1CondNoObject(b *testing.B)   { repo.Get("redis").Query1Cond(b.N, true, 10) }

func BenchmarkElastic2Cond(b *testing.B)        { repo.Get("elastic").Query2Cond(b.N, false, 10) }
func BenchmarkElastic2CondNoObj(b *testing.B)   { repo.Get("elastic").Query2Cond(b.N, true, 10) }
func BenchmarkMongo2Cond(b *testing.B)          { repo.Get("mongo").Query2Cond(b.N, false, 10) }
func BenchmarkMongo2CondNoObj(b *testing.B)     { repo.Get("mongo").Query2Cond(b.N, true, 10) }
func BenchmarkTarantool2Cond(b *testing.B)      { repo.Get("tarantool").Query2Cond(b.N, false, 10) }
func BenchmarkTarantool2CondNoObj(b *testing.B) { repo.Get("tarantool").Query2Cond(b.N, true, 10) }

func BenchmarkReindex2Cond(b *testing.B)         { repo.Get("reindex").Query2Cond(b.N, false, 10) }
func BenchmarkReindex2CondObjCache(b *testing.B) { repo.Get("reindex").Query2Cond(b.N, true, 10) }
func BenchmarkSqlite2CondQuery(b *testing.B)     { repo.Get("sqlite").Query2Cond(b.N, false, 10) }
func BenchmarkSqlite2CondNoObj(b *testing.B)     { repo.Get("sqlite").Query2Cond(b.N, true, 10) }
func BenchmarkMysql2CondQuery(b *testing.B)      { repo.Get("mysql").Query2Cond(b.N, false, 10) }
func BenchmarkMysql2CondNoObj(b *testing.B)      { repo.Get("mysql").Query2Cond(b.N, true, 10) }
