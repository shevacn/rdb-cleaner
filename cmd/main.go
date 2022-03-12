package main

import (
	"github.com/shevacn/rdb-cleaner/internal/cleaner"
	"github.com/shevacn/rdb-cleaner/internal/connector"
	"os"
	"os/signal"
)

func main() {

	config, err := cleaner.LoadConfig(os.Args[1:])
	if err != nil {
		panic(err)
	}
	if config == nil {
		return
	}
	connector.InitNodes(config.Nodes)
	cleaner := &cleaner.Cleaner{Config: config}
	cleaner.Start()

	waitForSignal()
}

func waitForSignal() os.Signal {
	signalChan := make(chan os.Signal, 1)
	defer close(signalChan)
	signal.Notify(signalChan, os.Kill, os.Interrupt)
	s := <-signalChan
	signal.Stop(signalChan)
	return s
}
