package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func getConsumerClient(consumerConfig Config) *kafka.Consumer {

	consumerClient, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": consumerConfig.BootstrapServers,
		"sasl.mechanism":    consumerConfig.SaslMechanism,
		"security.protocol": consumerConfig.SecurityProtocol,
		"ssl.ca.location":   consumerConfig.SslTruststoreLocation,
		"sasl.username":     consumerConfig.KafkaUsername,
		"sasl.password":     consumerConfig.KafkaPassword,
		"group.id":          consumerConfig.KafkaConsumerGroup,
		"auto.offset.reset": consumerConfig.AutoOffsetReset})
	if err != nil {
		log.Fatal(err)
	}

	return consumerClient

}

func consumeMessages(consumerClient *kafka.Consumer, topic []string) {
	err := consumerClient.SubscribeTopics(topic, nil)
	if err != nil {
		log.Fatal(err)
	}

	run := true

	for run == true {
		ev := consumerClient.Poll(0)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Println(string(e.Value))
		case kafka.Error:
			fmt.Println("ERROR: ", e)
			run = false
		}
	}

	err = consumerClient.Close()
	if err != nil {
		log.Fatal(err)
	}
}
