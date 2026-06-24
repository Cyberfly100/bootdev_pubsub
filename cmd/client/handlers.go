package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(playingState routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(playingState)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(armyMove gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(armyMove)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			player := gs.GetPlayerSnap()
			data := gamelogic.RecognitionOfWar{
				Attacker: armyMove.Player,
				Defender: player,
			}
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, strings.Join([]string{routing.WarRecognitionsPrefix, player.Username}, "."), data)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			log.Printf("Unknown move outcome: %v", outcome)
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(recognition gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(recognition)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeDraw, gamelogic.WarOutcomeYouWon:
			logMessage := createWarLogMessage(outcome, winner, loser)
			initiator := recognition.Attacker.Username
			gl := routing.GameLog{
				CurrentTime: time.Now().UTC(),
				Message:     logMessage,
				Username:    initiator,
			}
			err := pubsub.PublishGameLog(ch, gl, initiator)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			log.Printf("Unknown war outcome: %v", outcome)
			return pubsub.NackDiscard
		}
	}
}

func createWarLogMessage(outcome gamelogic.WarOutcome, winner string, loser string) string {
	switch outcome {
	case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon:
		return fmt.Sprintf("%s won a war against %s!", winner, loser)
	case gamelogic.WarOutcomeDraw:
		return fmt.Sprintf("A war between %s and %s resulted in a draw!", winner, loser)
	default:
		return "Unknown war outcome."
	}
}
