package main

import (
	"github.com/urfave/cli/v2"
	"log"
	"os"
)

func initCLI() {

	var configFile string

	app := &cli.App{
		Name:  "kafka-consumer",
		Usage: "Consume Kafka messages",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "config",
				Aliases:     []string{"c"},
				Usage:       "consumer config [ config.yaml ]",
				Destination: &configFile,
				Required:    true,
			},
		},
		Action: func(c *cli.Context) error {

			consumerConfig := getConsumerConfig(configFile)
			consumerClient := getConsumerClient(consumerConfig)
			consumeMessages(consumerClient, []string{"userEvents"})

			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
