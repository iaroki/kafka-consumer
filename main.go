package main

func main() {

	configPath := "config_dev.yaml"

	consumerConfig := getConsumerConfig(configPath)
	consumerClient := getConsumerClient(consumerConfig)
	consumeMessages(consumerClient, []string{"userEvents"})

}
