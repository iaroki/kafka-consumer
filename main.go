package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

func main() {

	kafkaBrokers := ""
	kafkaSaslMechanisms := ""
	kafkaSecurityProtocol := ""
	kafkaUsername := ""
	kafkaPassword := ""
	kafkaConsumerGroup := ""

	topics := []string{
		"userEvents_gen2",
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBrokers,
		"sasl.mechanisms":   kafkaSaslMechanisms,
		"security.protocol": kafkaSecurityProtocol,
		"sasl.username":     kafkaUsername,
		"sasl.password":     kafkaPassword,
		"group.id":          kafkaConsumerGroup,
		"auto.offset.reset": "smallest"})
	if err != nil {
		fmt.Println("ERROR: ", err)
		os.Exit(333)
	}

	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		fmt.Println("ERROR: ", err)
		os.Exit(333)
	}

	run := true

	for run == true {
		ev := consumer.Poll(0)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Println(string(e.Value))
		case kafka.Error:
			fmt.Println("ERROR: ", e)
			run = false
		}
	}

	err = consumer.Close()
	if err != nil {
		fmt.Println("ERROR: ", err)
	}

}
