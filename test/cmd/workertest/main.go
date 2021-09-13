package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/executor"
	_ "github.com/ab180/lrmr/test"
)

func main() {
	opt := executor.DefaultOptions()
	if len(os.Args) > 1 {
		port, err := strconv.Atoi(os.Args[1])
		if err != nil {
			log.Printf("error parsing args: %v", err)
			os.Exit(1)
		}
		url := fmt.Sprintf("127.0.0.1:%d", port)
		opt.ListenHost = url
		opt.AdvertisedHost = url
	}
	c, err := lrmr.ConnectToCluster()
	if err != nil {
		log.Fatalf("failed to join cluster: %v", err)
	}
	exec, err := lrmr.NewExecutor(c, opt)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	defer exec.Close()

	if err := exec.Start(); err != nil {
		log.Fatalf("failed to start executor: %v", err)
	}
}
