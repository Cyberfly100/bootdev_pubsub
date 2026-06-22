package main

import (
	"fmt"
	"strings"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
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
	// fmt.Println("Server connected to RabbitMQ successfully!")

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("Server failed to open a channel: %s\n", err)
		return
	}
	defer ch.Close()
	// fmt.Println("Server channel opened successfully!")

	topicCh, _, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, strings.Join([]string{routing.GameLogSlug, "*"}, "."), pubsub.SimpleQueueDurable)
	if err != nil {
		fmt.Printf("Client failed to declare and bind queue: %s\n", err)
		return
	}
	defer topicCh.Close()

	gamelogic.PrintServerHelp()

replLoop:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		cmd := words[0]
		switch cmd {
		case "pause":
			val := routing.PlayingState{
				IsPaused: true,
			}
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, val)
			if err != nil {
				fmt.Printf("Server failed to publish JSON: %s\n", err)
				return
			}
			fmt.Println("Server published pause message successfully!")
		case "resume":
			val := routing.PlayingState{
				IsPaused: false,
			}
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, val)
			if err != nil {
				fmt.Printf("Server failed to publish JSON: %s\n", err)
				return
			}
			fmt.Println("Server published resume message successfully!")
		case "help":
			gamelogic.PrintServerHelp()
		case "quit":
			fmt.Println("Shutting down Peril server...")
			break replLoop
		default:
			fmt.Printf("Unknown command: %s\n", cmd)
			gamelogic.PrintServerHelp()
		}
	}
}
