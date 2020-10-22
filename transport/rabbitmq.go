package transport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	flowmessage "github.com/cloudflare/goflow/v3/pb"
	"github.com/cloudflare/goflow/v3/utils"
	proto "github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
	"os"
)

var (
	RMQTLS          *bool
	RMQSASL         *bool
	RMQSrv          *string
	RMQExchangeName *string

	EmptySaslAuth = errors.New("RabbitMQ SASL config from environment was unsuccessful. RABBITMQ_SASL_USER and RABBITMQ_SASL_PASS need to be set.")
)

type RMQState struct {
	FixedLengthProto bool

	con      *amqp.Connection
	channel  *amqp.Channel
	exchange string
}

func RMQRegisterFlags() {
	RMQTLS = flag.Bool("rabbitmq.tls", false, "Use TLS to connect to RabbitMQ")
	RMQSASL = flag.Bool("rabbitmq.sasl", false, "Use SASL/PLAIN data to connect to RabbitMQ (TLS is recommended and the environment variables RABBITMQ_SASL_USER and RABBITMQ_SASL_PASS need to be set)")
	RMQSrv = flag.String("rabbitmq.srv", "amqp://guest:guest@localhost:5672", "SRV record containing a list of RabbitMQ brokers")
	RMQExchangeName = flag.String("rabbitmq.exchange", "flow-messages", "RabbitMQ Exchange to publish to")
}

func StartRMQProducerFromArgs(log utils.Logger) (*RMQState, error) {
	return StartRMQProducer(*RMQSrv, *RMQExchangeName, *RMQTLS, *RMQSASL, log)
}

func StartRMQProducer(url string, exchange string, useTls, useSasl bool, log utils.Logger) (*RMQState, error) {
	var con *amqp.Connection
	var err error
	var conf *amqp.Config
	if useTls {
		rootCAs, err := x509.SystemCertPool()
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Error initializing TLS: %v", err))
		}
		conf = &amqp.Config{TLSClientConfig: &tls.Config{RootCAs: rootCAs}}
	}

	if useSasl {
		if !useTls && log != nil {
			log.Warn("Using SASL without TLS will transmit the authentication in plaintext!")
		}
		auth := amqp.AMQPlainAuth{
			Username: os.Getenv("RABBITMQ_SASL_USER"),
			Password: os.Getenv("RABBITMQ_SASL_PASS"),
		}
		if auth.Username == "" && auth.Password == "" {
			return nil, EmptySaslAuth
		} else if log != nil {
			log.Infof("Authenticating as user '%s'...", auth.Username)
		}

		if conf == nil {
			conf = &amqp.Config{}
		}
		conf.SASL = []amqp.Authentication{&auth}
	}

	if len(url) < len("amqps://") || (url[:9] != "amqps://" && url[:8] != "amqp://") {
		if useTls {
			url = "amqps://" + url
		} else {
			url = "amqp://" + url
		}
	}

	if conf != nil {
		con, err = amqp.DialConfig(url, *conf)
	} else {
		con, err = amqp.Dial(url)
	}

	if err != nil {
		return nil, err
	}

	ch, err := con.Channel()
	if err != nil {
		_ = con.Close()
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		_ = ch.Close()
		_ = con.Close()
		return nil, err
	}

	state := RMQState{
		con:      con,
		exchange: exchange,
		channel:  ch,
	}

	return &state, nil
}

func (s *RMQState) Close() {
	if s == nil {
		return
	}
	if s.channel != nil {
		s.channel.Close()
	}
	if s.con != nil {
		defer s.con.Close()
	}
}

func (s RMQState) SendRMQFlowMessage(flowMessage *flowmessage.FlowMessage) {
	var b []byte
	if !s.FixedLengthProto {
		b, _ = proto.Marshal(flowMessage)
	} else {
		buf := proto.NewBuffer([]byte{})
		buf.EncodeMessage(flowMessage)
		b = buf.Bytes()
	}

	_ = s.channel.Publish(
		s.exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/protobuf;messageType=\"goflow-message\"",
			Body:        b,
		},
	)
}

func (s RMQState) Publish(msgs []*flowmessage.FlowMessage) {
	for _, msg := range msgs {
		s.SendRMQFlowMessage(msg)
	}
}
