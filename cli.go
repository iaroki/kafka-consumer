package main

import (
	"github.com/urfave/cli/v2"
	"log"
	"os"
)

func initCLI() {

	var configFile, topicName string

	app := &cli.App{
		Name:  "kafka-consumer",
		Usage: "Consume Kafka messages",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "config",
				Aliases:     []string{"c"},
				Usage:       "consumer config `config.yaml`",
				Destination: &configFile,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "topic",
				Aliases:     []string{"t"},
				Usage:       "topic name to consume `userEvents`",
				Destination: &topicName,
				Required:    false,
			},
		},
		Action: func(c *cli.Context) error {

			consumerConfig := getConsumerConfig(configFile)
			consumerClient := getConsumerClient(consumerConfig)

			var topic string

			if topicName != "" {
				topic = topicName
			} else {
				topic = consumerConfig.KafkaTopic
			}

			consumeMessages(consumerClient, []string{topic})

			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
