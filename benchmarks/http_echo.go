package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/restream/reindexer/benchmarks/repo"

	"github.com/labstack/echo"
)

func EchoQueryGetByIDHandler(ctx echo.Context) error {
	r := ctx.Param("repo")
	item := repo.Get(r).QueryByID(1, false)
	return ctx.JSON(http.StatusOK, item)
}

func EchoQuery1CondHandler(ctx echo.Context) error {
	r := ctx.Param("repo")
	items := repo.Get(r).Query1Cond(1, false, 10)
	return ctx.JSON(http.StatusOK, items)
}

func EchoQuery2CondHandler(ctx echo.Context) error {
	r := ctx.Param("repo")
	items := repo.Get(r).Query2Cond(1, false, 10)
	return ctx.JSON(http.StatusOK, items)
}

func EchoQueryTextHandler(ctx echo.Context) error {
	r := ctx.Param("repo")

	dsl := ""
	switch r {
	case "elastic", "mysql", "reindex":
		dsl = "%s %s"
	case "sqlite":
		dsl = "%s OR %s"
	case "sphinx":
		dsl = "%s | %s"
	case "mongo":
		dsl = "%s %s"
	case "arango":
		dsl = "%s,|%s"
	}

	items := repo.Get(r).QueryFullText(func() string { return fmt.Sprintf(dsl, randStringWord(), randStringWord()) }, 1, 10)
	return ctx.JSON(http.StatusOK, items)
}

func EchoQueryTextPrefixHandler(ctx echo.Context) error {
	r := ctx.Param("repo")

	dsl := ""
	switch r {
	case "elastic", "mysql", "reindex":
		dsl = "%s* %s*"
	case "sqlite":
		dsl = "%s* OR %s*"
	case "sphinx":
		dsl = "%s* | %s*"
	case "arango":
		dsl = "prefix:%s,|prefix:%s"
	}

	items := repo.Get(r).QueryFullText(func() string { return fmt.Sprintf(dsl, randStringPref(), randStringPref()) }, 1, 10)
	return ctx.JSON(http.StatusOK, items)
}

func EchoUpdateHandler(ctx echo.Context) error {
	r := ctx.Param("repo")
	repo.Get(r).Update(1)
	return ctx.String(http.StatusOK, "{}")
}

func StartEchoHTTP() {
	e := echo.New()
	e.HideBanner = true

	e.GET("/byid/:repo", EchoQueryGetByIDHandler)
	e.GET("/1cond/:repo", EchoQuery1CondHandler)
	e.GET("/2cond/:repo", EchoQuery2CondHandler)
	e.GET("/text/:repo", EchoQueryTextHandler)
	e.GET("/text_prefix/:repo", EchoQueryTextPrefixHandler)
	e.GET("/update/:repo", EchoUpdateHandler)

	log.Printf("Starting listen echo on 8082")
	err := e.Start(":8082")
	if err != nil {
		panic(err)
	}

}
