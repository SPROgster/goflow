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
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"os"
	"time"
)

var (
	RMQTLS               *bool
	RMQSASL              *bool
	RMQSrv               *string
	RMQExchangeName      *string
	RMQReconnectionDelay *int

	EmptySaslAuth = errors.New("RabbitMQ SASL config from environment was unsuccessful. RABBITMQ_SASL_USER and RABBITMQ_SASL_PASS need to be set.")
)

type RMQState struct {
	FixedLengthProto bool

	url    string
	config *amqp.Config

	con      *amqp.Connection
	channel  *amqp.Channel
	exchange string
	stop     chan bool
}

func RMQRegisterFlags() {
	RMQTLS = flag.Bool("rabbitmq.tls", false, "Use TLS to connect to RabbitMQ")
	RMQSASL = flag.Bool("rabbitmq.sasl", false, "Use SASL/PLAIN data to connect to RabbitMQ (TLS is recommended and the environment variables RABBITMQ_SASL_USER and RABBITMQ_SASL_PASS need to be set)")
	RMQSrv = flag.String("rabbitmq.srv", "amqp://guest:guest@localhost:5672", "SRV record containing a list of RabbitMQ brokers")
	RMQExchangeName = flag.String("rabbitmq.exchange", "flow-messages", "RabbitMQ Exchange to publish to")
	RMQReconnectionDelay = flag.Int("rabbitmq.reconnectdelay", 5, "Delay in seconds to reconnect")
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
		url:      url,
		config:   conf,
		con:      con,
		exchange: exchange,
		channel:  ch,
		stop:     make(chan bool),
	}

	go state.watchdog()

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

func (s RMQState) SendRMQFlowMessage(flowMessage *flowmessage.FlowMessage) error {
	var b []byte
	c := s.channel
	if c == nil {
		return errors.New("RabbitMQ connection lost")
	}

	if !s.FixedLengthProto {
		b, _ = proto.Marshal(flowMessage)
	} else {
		buf := proto.NewBuffer([]byte{})
		buf.EncodeMessage(flowMessage)
		b = buf.Bytes()
	}

	err := c.Publish(
		s.exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/protobuf;messageType=\"goflow-message\"",
			Body:        b,
		},
	)

	return err
}

func (s RMQState) Publish(msgs []*flowmessage.FlowMessage) {
	for _, msg := range msgs {
		s.SendRMQFlowMessage(msg)
	}
}

func (s *RMQState) reconnect() *amqp.Connection {
	for {
		log.Info("Reconnecting to RabbitMQ server")
		var con *amqp.Connection
		var err error

		if s.config != nil {
			con, err = amqp.DialConfig(s.url, *s.config)
		} else {
			con, err = amqp.Dial(s.url)
		}

		if err == nil {
			log.Info("Connection established")
			s.con = con
			return con
		}

		log.WithError(err).WithField("delay", *RMQReconnectionDelay).Error("Unable to reconnect. Sleeping")
		time.Sleep(time.Duration(*RMQReconnectionDelay) * time.Second)

		t := time.NewTimer(time.Duration(*RMQReconnectionDelay) * time.Second)
		select {
		case <-s.stop:
			return nil

		case <-t.C:
		}
	}
}

func (s *RMQState) recreateChannel() *amqp.Channel {
	for {
		log.Info("Recreating RabbitMQ channel")

		ch, err := s.con.Channel()

		if err == nil {
			log.Info("Channel recreated")
			s.channel = ch

			ch.ExchangeDeclare(
				s.exchange,
				"direct",
				true,
				false,
				false,
				false,
				nil,
			)

			return ch
		}

		log.WithError(err).WithField("delay", *RMQReconnectionDelay).Error("Unable to create channel. Sleeping")
		time.Sleep(time.Duration(*RMQReconnectionDelay) * time.Second)

		t := time.NewTimer(time.Duration(*RMQReconnectionDelay) * time.Second)
		select {
		case <-s.stop:
			return nil

		case <-t.C:
		}
	}
}

func (s *RMQState) watchdog() {
	for {
		select {
		case <-s.stop:
			return

		case reason, ok := <-s.con.NotifyClose(make(chan *amqp.Error)):
			s.channel = nil
			if ok {
				log.WithError(reason).Error("Connection closed")
			}
			if s.reconnect() != nil && s.recreateChannel() != nil {
				return
			}

		case reason, ok := <-s.channel.NotifyClose(make(chan *amqp.Error)):
			if ok {
				log.WithError(reason).Error("Channel closed")
			}
			_ = s.channel.Close()

			if s.recreateChannel() != nil {
				return
			}
		}
	}
}
