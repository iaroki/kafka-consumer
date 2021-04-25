package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

type Config struct {
	BootstrapServers      string `yaml:"bootstrap.servers"`
	SaslMechanism         string `yaml:"sasl.mechanism"`
	SecurityProtocol      string `yaml:"security.protocol"`
	SslTruststoreLocation string `yaml:"ssl.truststore.location"`
	KafkaUsername         string `yaml:"kafka.username"`
	KafkaPassword         string `yaml:"kafka.password"`
	KafkaConsumerGroup    string `yaml:"kafka.consumer.group"`
	AutoOffsetReset       string `yaml:"auto.offset.reset"`
}

func getConfig(configFilePath string) Config {

	yamlFile, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		fmt.Printf("Error reading YAML file: %s\n", err)
	}

	var brokerConfig Config

	err = yaml.Unmarshal(yamlFile, &brokerConfig)
	if err != nil {
		fmt.Printf("Error parsing YAML file: %s\n", err)
	}

	return brokerConfig

}

func main() {

	brokerConfig := getConfig("config.yaml")

	topics := []string{
		"userEvents_gen2",
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokerConfig.BootstrapServers,
		"sasl.mechanism":    brokerConfig.SaslMechanism,
		"security.protocol": brokerConfig.SecurityProtocol,
		"ssl.ca.location":   brokerConfig.SslTruststoreLocation,
		"sasl.username":     brokerConfig.KafkaUsername,
		"sasl.password":     brokerConfig.KafkaPassword,
		"group.id":          brokerConfig.KafkaConsumerGroup,
		"auto.offset.reset": brokerConfig.AutoOffsetReset})
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
