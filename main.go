package main

import (
	badger "github.com/dgraph-io/badger"
	"log"
)

func main() {
	db, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

}
