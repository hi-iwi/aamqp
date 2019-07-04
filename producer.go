package aamqp

import (
	"encoding/json"
	"errors"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

var (
	connPool *sync.Pool
	connOnce sync.Once
)

type Producer struct {
	cc      ConnectionConfig
	conn    *amqp.Connection
	channel *amqp.Channel
	done    chan error

	uri      string
	exchange Exchange
}
type Conn struct {
	longConnection bool
	connection     *amqp.Connection
}

// Close close short connection
func (c *Conn) Close() {
	if c.longConnection {
		connPool.Put(c.connection)
	} else {
		c.connection.Close()
	}
}

func NewProducer(uri string, ex Exchange, ccs ...ConnectionConfig) *Producer {
	var cc ConnectionConfig
	if len(ccs) > 0 {
		cc = ccs[0]
	}

	producer := &Producer{
		cc:       cc.withDefault(),
		uri:      uri,
		exchange: ex,
	}
	return producer
}

func (p *Producer) connect() (*amqp.Connection, error) {
	ac := amqp.Config{
		Properties: amqp.Table{
			"product": defaultProduct,
			"version": defaultVersion,
			"role":    "producer",
		},
		Heartbeat: p.cc.Heartbeat,
		Locale:    p.cc.Locale,
		Dial:      amqp.DefaultDial(p.cc.Timeout),
	}
	return amqp.DialConfig(p.uri, ac)

}

func (p *Producer) ConnectionPool() (*Conn, error) {
	connOnce.Do(func() {
		connPool = &sync.Pool{
			New: func() interface{} {
				c, err := p.connect()
				if err != nil {
					log.Println("amqp connection error: " + err.Error())
				}
				return c
			},
		}
	})

	c := connPool.Get().(*amqp.Connection)
	if c == nil || c.IsClosed() {
		// using short connection
		c, err := p.connect()
		if err != nil {
			return nil, err
		}
		return &Conn{
			longConnection: false,
			connection:     c,
		}, nil
	}

	return &Conn{
		longConnection: true,
		connection:     c,
	}, nil
}

// SimplePub msg[, key[, exchange]]
func (p *Producer) SimplePub(msg interface{}, args ...string) error {
	var key string
	var data amqp.Publishing
	var ok bool
	exchange := p.exchange.Name

	if data, ok = msg.(amqp.Publishing); !ok {
		data.Headers = amqp.Table{}
		data.ContentEncoding = ""
		data.DeliveryMode = amqp.Transient

		if str, ok := msg.(string); ok {
			data.ContentType = "text/plain"
			data.Body = []byte(str)
			//data.Priority = 0
		} else if b, ok := msg.([]byte); ok {
			data.ContentType = "text/plain"
			data.Body = b
		} else {
			if m, err := json.Marshal(msg); err == nil {
				data.ContentType = "application/json"
				data.Body = m
			} else {
				return errors.New("unrecognized type of the SimplePub msg")
			}
		}
	}

	if len(args) > 1 {
		exchange = args[1]
		key = args[0]
	} else if len(args) == 1 {
		key = args[0]
	}
	return p.Pub(exchange, key, false, false, data, nil)

}

// Pub short-connection publish
// @param confirm: Reliable publisher confirms require confirm.select support from the connection.
//        e.g. func confirmOne(confirms <-chan amqp.Confirmation){if confirmed := <-confirms; confirmed.Ack{OK} else {FAIL}}
func (p *Producer) Pub(exchange, key string, mandatory, immediate bool, msg amqp.Publishing, confirming func(<-chan amqp.Confirmation)) error {
	c, err := p.ConnectionPool()
	if err != nil {
		return errors.New("failed to connect to AMQP broker: " + err.Error())
	}
	defer c.Close()

	ch, err := c.connection.Channel()
	if err != nil {
		return errors.New("failed to open a channel: " + err.Error())
	}
	defer ch.Close()

	if err = ch.ExchangeDeclare(p.exchange.Name, p.exchange.Kind, p.exchange.Durable, p.exchange.AutoDelete, p.exchange.Internal, p.exchange.NoWait, p.exchange.Args); err != nil {
		return errors.New("exchange declare error: " + err.Error())
	}

	if confirming != nil {
		if err = ch.Confirm(false); err != nil {
			return errors.New("channel could not be put into confirm mode: " + err.Error())
		}
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		defer confirming(confirms)
	}
	return ch.Publish(exchange, key, mandatory, immediate, msg)
}
