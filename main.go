package main

import (
	"flag"
	"log"

	"github.com/hanwen/go-fuse/v2/fs"
)

func main() {
	debug := flag.Bool("debug", false, "print debug data")
	dsn := flag.String("dsn", "", "data source name")
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("mountpoint is required")
	}
	opts := &fs.Options{}

	p := flag.Arg(0)
	root := NewMySQLRoot(*dsn, nil, *debug)
	server, err := fs.Mount(p, root, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Wait()
}
