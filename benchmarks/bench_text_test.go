package main

import (
	"testing"

	"./repo"
)

func BenchmarkElasticFullText(b *testing.B) {
	repo.Get("elastic").QueryFullText(func() string { return randStringWord() + " " + randStringWord() }, b.N, 10)
}
func BenchmarkMongoFullText(b *testing.B) {
	repo.Get("mongo").QueryFullText(func() string { return randStringWord() + " " + randStringWord() }, b.N, 10)
}
func BenchmarkSqliteFullText(b *testing.B) {
	repo.Get("sqlite").QueryFullText(func() string { return randStringWord() + " OR " + randStringWord() }, b.N, 10)
}
func BenchmarkMysqlFullText(b *testing.B) {
	repo.Get("mysql").QueryFullText(func() string { return randStringWord() + " " + randStringWord() }, b.N, 10)
}
func BenchmarkSphinxFullText(b *testing.B) {
	repo.Get("sphinx").QueryFullText(func() string { return "=" + randStringWord() + " | " + "=" + randStringWord() }, b.N, 10)
}
func BenchmarkReindexFullText(b *testing.B) {
	repo.Get("reindex").QueryFullText(func() string { return "=" + randStringWord() + " =" + randStringWord() }, b.N, 10)
}
func BenchmarkElasticFullTextPref(b *testing.B) {
	repo.Get("elastic").QueryFullText(func() string { return randStringPref() + "* " + randStringPref() + "* " }, b.N, 10)
}
func BenchmarkSqliteFullTextPref(b *testing.B) {
	repo.Get("sqlite").QueryFullText(func() string { return randStringPref() + "* OR " + randStringPref() + "*" }, b.N, 10)
}
func BenchmarkMysqlFullTextPref(b *testing.B) {
	repo.Get("mysql").QueryFullText(func() string { return randStringPref() + "* " + randStringPref() + "*" }, b.N, 10)
}
func BenchmarkSphinxFullTextPref(b *testing.B) {
	repo.Get("sphinx").QueryFullText(func() string { return randStringPref() + "* | " + randStringPref() + "*" }, b.N, 10)
}
func BenchmarkReindexFullTextPref(b *testing.B) {
	repo.Get("reindex").QueryFullText(func() string { return randStringPref() + "* " + randStringPref() + "* " }, b.N, 10)
}
func BenchmarkElasticFullText3Fuzzy(b *testing.B) {
	repo.Get("elastic").QueryFullText(func() string { return randStringWord() + "*~ " + randStringWord() + "*~ " + randStringWord() + "*~ " }, b.N, 10)
}
func BenchmarkSqliteFullText3Fuzzy(b *testing.B) {
	repo.Get("sqlite").QueryFullText(func() string { return randStringWord() + "* OR " + randStringWord() + "* OR " + randStringWord() + "*" }, b.N, 10)
}
func BenchmarkMysqlFullText3Fuzzy(b *testing.B) {
	repo.Get("mysql").QueryFullText(func() string { return randStringWord() + "* " + randStringWord() + "* " + randStringWord() + "*" }, b.N, 10)
}
func BenchmarkSphinxFullText3Fuzzy(b *testing.B) {
	repo.Get("sphinx").QueryFullText(func() string { return randStringWord() + "* | " + randStringWord() + "* | " + randStringWord() + "*" }, b.N, 10)
}
func BenchmarkReindexFullText3Fuzzy(b *testing.B) {
	repo.Get("reindex").QueryFullText(func() string { return randStringWord() + "*~ " + randStringWord() + "*~ " + randStringWord() + "*~ " }, b.N, 10)
}
