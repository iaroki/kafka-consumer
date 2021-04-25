package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Config struct {
	BootstrapServers      string `yaml:"bootstrap.servers"`
	SaslMechanism         string `yaml:"sasl.mechanism"`
	SecurityProtocol      string `yaml:"security.protocol"`
	SslTruststoreLocation string `yaml:"ssl.truststore.location"`
	KafkaUsername         string `yaml:"kafka.username"`
	KafkaPassword         string `yaml:"kafka.password"`
	KafkaTopic            string `yaml:"kafka.topic"`
	KafkaConsumerGroup    string `yaml:"kafka.consumer.group"`
	AutoOffsetReset       string `yaml:"auto.offset.reset"`
}

func getConsumerConfig(configFilePath string) Config {

	yamlFile, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		fmt.Printf("Error reading YAML file: %s\n", err)
	}

	var consumerConfig Config

	err = yaml.Unmarshal(yamlFile, &consumerConfig)
	if err != nil {
		fmt.Printf("Error parsing YAML file: %s\n", err)
	}

	return consumerConfig

}
