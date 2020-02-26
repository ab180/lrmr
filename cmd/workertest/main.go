package main

import (
	"fmt"
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/playground"
	"os"
	"strconv"
)

var _ = &playground.Counter{}

func main() {
	opt := lrmr.DefaultOptions()
	if len(os.Args) > 1 {
		port, err := strconv.Atoi(os.Args[1])
		if err != nil {
			fmt.Printf("error parsing args: %v", err)
			os.Exit(1)
		}
		opt.Worker.Port = port
		opt.Worker.Host = fmt.Sprintf("localhost:%d", port)
	}
	if err := lrmr.RunWorker(opt); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
