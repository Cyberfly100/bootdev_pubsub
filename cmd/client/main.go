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
			_, err := gamestate.CommandMove(words)
			if err != nil {
				fmt.Printf("Client error: %s\n", err)
			}
			// fmt.Printf("Moving %v to %v.\n", move.Units, move.ToLocation)
		case "status":
			gamestate.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			break replLoop
		default:
			fmt.Printf("Unknown command: %s\n", cmd)
			gamelogic.PrintClientHelp()
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(playingState routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(playingState)
	}
}
