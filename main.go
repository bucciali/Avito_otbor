package main

import (
	"avito_otbor/api"
	dbtablesgo "avito_otbor/dbTablesGo"
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func main() {

	r := chi.NewRouter()
	if err := dbtablesgo.DbInit(); err != nil {
		log.Fatal("Database initialization failed:", err)
	}
	api.Init(r)
	fmt.Println("Server is running on port :8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatal("Server failed:", err)
	}

}
