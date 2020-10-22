package transport

import (
	"errors"
	"flag"
	"strings"

	"github.com/cloudflare/goflow/v3/utils"
	log "github.com/sirupsen/logrus"
)

type transportType int

const (
	Log transportType = iota
	Kafka
	RabbitMq
	Unknown
)

var (
	transportsStrings = [...]string{"Log", "Kafka", "RabbitMq"}
	UnknownType       = errors.New("unsupported transport type")
)

var (
	Type = flag.String("transport", "log", "Transport to use: kafka, rabbitmq, log")

	LogFmt      = flag.String("logfmt", "normal", "Log formatter")
	FixedLength = flag.Bool("proto.fixedlen", false, "Enable fixed length protobuf")
)

func RegisterFlags() {
	KafkaRegisterFlags()
	RMQRegisterFlags()
}

func parseType(t string) (transportType, error) {
	for i, val := range transportsStrings {
		if strings.EqualFold(t, val) {
			return transportType(i), nil
		}
	}
	return Unknown, UnknownType
}

func CreateTransport() (*utils.Transport, error) {
	var transport utils.Transport
	transport = &utils.DefaultLogTransport{}

	switch *LogFmt {
	case "json":
		log.SetFormatter(&log.JSONFormatter{})
		transport = &utils.DefaultJSONTransport{}
	}

	t, err := parseType(*Type)
	if err != nil {
		return nil, err
	}

	switch t {
	default:
		return nil, UnknownType

	case Log:
		break

	case Kafka:
		kafkaState, err := StartKafkaProducerFromArgs(log.StandardLogger())
		if err != nil {
			return nil, err
		}
		kafkaState.FixedLengthProto = *FixedLength
		transport = kafkaState

	case RabbitMq:
		rmqState, err := StartRMQProducerFromArgs(log.StandardLogger())
		if err != nil {
			return nil, err
		}
		rmqState.FixedLengthProto = *FixedLength
		transport = rmqState
	}
	return &transport, nil
}
