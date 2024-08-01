package main

import (
	"fmt"
	minibitcask "github.com/0xAlanYin/mini-bitcask-db"
)

func main() {
	// Open DB
	db, err := minibitcask.Open("/tmp/minibitcask")
	if err != nil {
		panic(err)
	}

	var (
		key   = []byte("key")
		value = []byte("value")
	)

	// Put
	err = db.Put(key, value)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Put key: %s, value: %s success\n", key, value)

	// Get
	currValue, err := db.Get(key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Get key: %s, value: %s success\n", key, currValue)

	// Delete
	err = db.Delete(key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Delete key: %s success\n", key)

	// Merge
	err = db.Merge()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Merge success: compact data to a new datafile\n")

	// Close
	err = db.Close()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Close db success\n")
}
