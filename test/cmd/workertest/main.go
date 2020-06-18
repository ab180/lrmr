package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/therne/lrmr"
	_ "github.com/therne/lrmr/test"
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
		opt.Worker.ListenHost = url
		opt.Worker.AdvertisedHost = url
	}
	if err := lrmr.RunWorker(opt); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
