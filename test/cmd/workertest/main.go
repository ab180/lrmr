package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/ab180/lrmr"
	_ "github.com/ab180/lrmr/test"
)

func main() {
	opt := lrmr.DefaultOptions()
	if len(os.Args) > 1 {
		port, err := strconv.Atoi(os.Args[1])
		if err != nil {
			fmt.Printf("error parsing args: %v", err)
			os.Exit(1)
		}
		url := fmt.Sprintf("127.0.0.1:%d", port)
		opt.Executor.ListenHost = url
		opt.Executor.AdvertisedHost = url
	}
	if err := lrmr.RunExecutor(opt); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
