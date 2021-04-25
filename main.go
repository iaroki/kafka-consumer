package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

func main() {

	topics := []string{
		"mytopic",
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "host1:9092,host2:9092",
		"group.id":          "foo",
		"auto.offset.reset": "smallest"})
	if err != nil {
		fmt.Println(err)
		os.Exit(333)
	}

	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		fmt.Println(err)
		os.Exit(333)
	}

	run := false

	for run == true {
		ev := consumer.Poll(0)
		switch e := ev.(type) {
		case *kafka.Message:
			// application-specific processing
			fmt.Printf("%% Message on %s:\n%s\n",
				e.TopicPartition, string(e.Value))
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			run = false
			//default:
			//	fmt.Printf("Ignored %v\n", e)
		}
	}

	err = consumer.Close()
	if err != nil {
		fmt.Println(err)
	}

}
