package main

import (
	"log"
	"os"
	"path/filepath"
)

func main() {
	switch filepath.Base(os.Args[0]) {
	case "scanner":
		runScanner()
	case "transfer":
		runTransfer()
	default:
		log.Fatalf("unknown worker mode %q, expected binary name scanner or transfer", filepath.Base(os.Args[0]))
	}
}
