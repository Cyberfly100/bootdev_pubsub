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
	fmt.Println("Starting Peril client...")
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("Client failed to connect to RabbitMQ: %s\n", err)
		return
	}
	defer conn.Close()
	fmt.Println("Client connected to RabbitMQ successfully!")

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("Client failed to open a channel: %s\n", err)
		return
	}
	defer ch.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("Client error: %s\n", err)
		return
	}
	gamestate := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, strings.Join([]string{routing.PauseKey, username}, "."), routing.PauseKey, pubsub.SimpleQueueTransient, handlerPause(gamestate))
	if err != nil {
		fmt.Printf("Client failed to subscribe to pause messages: %s\n", err)
		return
	}

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, strings.Join([]string{routing.ArmyMovesPrefix, username}, "."), strings.Join([]string{routing.ArmyMovesPrefix, "*"}, "."), pubsub.SimpleQueueTransient, handlerMove(gamestate, ch))
	if err != nil {
		fmt.Printf("Client failed to subscribe to army move messages: %s\n", err)
		return
	}

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, "war", strings.Join([]string{routing.WarRecognitionsPrefix, "*"}, "."), pubsub.SimpleQueueDurable, handlerWar(gamestate, ch))
	if err != nil {
		fmt.Printf("Client failed to subscribe to war recognition messages: %s\n", err)
		return
	}

replLoop:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		cmd := words[0]
		switch cmd {
		case "spawn":
			err := gamestate.CommandSpawn(words)
			if err != nil {
				fmt.Printf("Client error: %s\n", err)
			}
		case "move":
			move, err := gamestate.CommandMove(words)
			if err != nil {
				fmt.Printf("Client error: %s\n", err)
			}
			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, strings.Join([]string{routing.ArmyMovesPrefix, move.Player.Username}, "."), move)
			if err != nil {
				fmt.Printf("Client failed to publish army move: %s\n", err)
			}
			fmt.Printf("Client published army move to %s successfully!\n", move.ToLocation)
		case "status":
			gamestate.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			CommandSpam(gamestate, words, ch)
		case "quit":
			gamelogic.PrintQuit()
			break replLoop
		default:
			fmt.Printf("Unknown command: %s\n", cmd)
			gamelogic.PrintClientHelp()
		}
	}
}
