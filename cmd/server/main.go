package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	connectionString := "amqp://guest:guest@localhost:5672/"
	fmt.Println("Starting Peril server...")
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("Server failed to connect to RabbitMQ: %s\n", err)
		return
	}
	defer conn.Close()
	fmt.Println("Server connected to RabbitMQ successfully!")

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("Server failed to open a channel: %s\n", err)
		return
	}
	defer ch.Close()
	fmt.Println("Server channel opened successfully!")

	val := routing.PlayingState{
		IsPaused: true,
	}
	err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, val)
	if err != nil {
		fmt.Printf("Server failed to publish JSON: %s\n", err)
		return
	}
	fmt.Println("Server published JSON message successfully!")
	// wait for ctrl+c to exit
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("") // so the ^C is on a separate line

	fmt.Println("Shutting down Peril server...")
}
