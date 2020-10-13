// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

const (
	// Keys
	mqttURL          = "url"
	mqttQOS          = "qos"
	mqttRetain       = "retain"
	mqttClientID     = "consumerID"
	mqttCleanSession = "cleanSession"
	mqttCACert       = "caCert"
	mqttClientCert   = "clientCert"
	mqttClientKey    = "clientKey"

	// errors
	errorMsgPrefix = "mqtt pub sub error:"

	// Defaults
	defaultQOS          = 0
	defaultRetain       = false
	defaultWait         = 3 * time.Second
	defaultCleanSession = true
)

// mqttPubSub type allows sending and receiving data to/from MQTT broker.
type mqttPubSub struct {
	producer mqtt.Client
	consumer mqtt.Client
	metadata *metadata
	logger   logger.Logger
	topics   map[string]byte
	cancel   context.CancelFunc
}

// NewMQTTPubSub returns a new mqttPubSub instance.
func NewMQTTPubSub(logger logger.Logger) pubsub.PubSub {
	return &mqttPubSub{logger: logger}
}

// isValidPEM validates the provided input has PEM formatted block.
func isValidPEM(val string) bool {
	block, _ := pem.Decode([]byte(val))

	return block != nil
}

func parseMQTTMetaData(md pubsub.Metadata) (*metadata, error) {
	m := metadata{}

	// required configuration settings
	if val, ok := md.Properties[mqttURL]; ok && val != "" {
		m.url = val
	} else {
		return &m, fmt.Errorf("%s missing url", errorMsgPrefix)
	}

	// optional configuration settings
	m.qos = defaultQOS
	if val, ok := md.Properties[mqttQOS]; ok && val != "" {
		qosInt, err := strconv.Atoi(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid qos %s, %s", errorMsgPrefix, val, err)
		}
		m.qos = byte(qosInt)
	}

	m.retain = defaultRetain
	if val, ok := md.Properties[mqttRetain]; ok && val != "" {
		var err error
		m.retain, err = strconv.ParseBool(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid retain %s, %s", errorMsgPrefix, val, err)
		}
	}

	m.clientID = uuid.New().String()
	if val, ok := md.Properties[mqttClientID]; ok && val != "" {
		m.clientID = val
	}

	m.cleanSession = defaultCleanSession
	if val, ok := md.Properties[mqttCleanSession]; ok && val != "" {
		var err error
		m.cleanSession, err = strconv.ParseBool(val)
		if err != nil {
			return &m, fmt.Errorf("%s invalid clean session %s, %s", errorMsgPrefix, val, err)
		}
	}

	if val, ok := md.Properties[mqttCACert]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, fmt.Errorf("%s invalid ca certificate", errorMsgPrefix)
		}
		m.tlsCfg.caCert = val
	}
	if val, ok := md.Properties[mqttClientCert]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, fmt.Errorf("%s invalid client certificate", errorMsgPrefix)
		}
		m.tlsCfg.clientCert = val
	}
	if val, ok := md.Properties[mqttClientKey]; ok && val != "" {
		if !isValidPEM(val) {
			return &m, fmt.Errorf("%s invalid client certificate key", errorMsgPrefix)
		}
		m.tlsCfg.clientKey = val
	}

	return &m, nil
}

// Init parses metadata and creates a new Pub Sub client.
func (m *mqttPubSub) Init(metadata pubsub.Metadata) error {
	mqttMeta, err := parseMQTTMetaData(metadata)
	if err != nil {
		return err
	}
	m.metadata = mqttMeta

	// mqtt broker allows only one connection at a given time from a clientID.
	producerClientID := fmt.Sprintf("%s-producer", m.metadata.clientID)
	p, err := m.connect(producerClientID)
	if err != nil {
		m.logger.Warn("mqtt message bus initialization failed")
		return nil
	}

	m.producer = p
	m.topics = make(map[string]byte)

	m.logger.Debug("mqtt message bus initialization complete")

	return nil
}

// Publish the topic to mqtt pub sub.
func (m *mqttPubSub) Publish(req *pubsub.PublishRequest) error {
	var err error
	if m.producer == nil || !m.producer.IsConnected() {
		producerClientID := fmt.Sprintf("%s-producer", m.metadata.clientID)
		m.producer, err = m.connect(producerClientID)
		if err != nil {
			return err
		}
	}
	//m.logger.Debugf("mqtt publishing topic %s with data: %v", req.Topic, req.Data)

	token := m.producer.Publish(req.Topic, m.metadata.qos, m.metadata.retain, req.Data)
	if !token.WaitTimeout(defaultWait) || token.Error() != nil {
		return fmt.Errorf("mqtt error from publish: %v", token.Error())
	}

	return nil
}

func (m *mqttPubSub) subscribeHandler(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	registerHandler := func(client mqtt.Client, mqttMsg mqtt.Message) {
		msg := &pubsub.NewMessage{Topic: mqttMsg.Topic(), Data: mqttMsg.Payload()}
		if err := handler(msg); err != nil {
			m.consumer.Disconnect(0)
		}
	}

	token := m.consumer.SubscribeMultiple(m.topics, registerHandler)
	if err := token.Error(); err != nil {
		return err
	}
	return nil
}

// isHandlerReady checks if the subscribe grpc handler is ready.
func (m *mqttPubSub) isHandlerReady(handler func(msg *pubsub.NewMessage) error) bool {
	sampleCloudEvent := pubsub.NewCloudEventsEnvelope("", "", "", "", "", "", nil)
	sampleData, _ := json.Marshal(sampleCloudEvent)
	msg := &pubsub.NewMessage{
		Data:  sampleData,
		Topic: "",
	}
	if err := handler(msg); err != nil {
		return false
	}
	return true
}

// Subscribe to the mqtt pub sub topic.
func (m *mqttPubSub) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	// mqtt broker allows only one connection at a given time from a clientID.
	consumerClientID := fmt.Sprintf("%s-consumer", m.metadata.clientID)

	m.topics[req.Topic] = m.metadata.qos

	var err error
	connCreated := make(chan bool, 1)
	var once sync.Once
	// reset synchronization
	if m.consumer != nil {
		m.logger.Warnf("re-initializing the subscriber")
		m.cancel()
		m.consumer.Disconnect(0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	go func(mps *mqttPubSub, handler func(msg *pubsub.NewMessage) error) {
		ticker := time.NewTicker(defaultWait)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if (mps.consumer == nil || !mps.consumer.IsConnected()) && mps.isHandlerReady(handler) {
					mps.logger.Debug("connecting for subscription")
					mps.consumer, err = mps.connect(consumerClientID)
					once.Do(func() {
						connCreated <- true
					})
					if err != nil {
						continue
					}
					if err = mps.subscribeHandler(req, handler); err != nil {
						mps.logger.Warnf("error creating subscribe handler: %v", err)
						mps.consumer.Disconnect(0)
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}(m, handler)

	<-connCreated // wait for connection creation
	return nil
}

func (m *mqttPubSub) connect(clientID string) (mqtt.Client, error) {
	uri, err := url.Parse(m.metadata.url)
	if err != nil {
		return nil, err
	}
	opts := m.createClientOptions(uri, clientID)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	for !token.WaitTimeout(defaultWait) {
	}
	if err := token.Error(); err != nil {
		return nil, err
	}

	return client, nil
}

func (m *mqttPubSub) newTLSConfig() *tls.Config {
	tlsConfig := new(tls.Config)

	if m.metadata.clientCert != "" && m.metadata.clientKey != "" {
		cert, err := tls.X509KeyPair([]byte(m.metadata.clientCert), []byte(m.metadata.clientKey))
		if err != nil {
			m.logger.Warnf("unable to load client certificate and key pair. Err: %v", err)

			return tlsConfig
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if m.metadata.caCert != "" {
		tlsConfig.RootCAs = x509.NewCertPool()
		if ok := tlsConfig.RootCAs.AppendCertsFromPEM([]byte(m.metadata.caCert)); !ok {
			m.logger.Warnf("unable to load ca certificate.")
		}
	}

	return tlsConfig
}

func (m *mqttPubSub) createClientOptions(uri *url.URL, clientID string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.SetClientID(clientID)
	opts.SetCleanSession(m.metadata.cleanSession)
	opts.AddBroker(uri.Scheme + "://" + uri.Host)
	opts.SetUsername(uri.User.Username())
	password, _ := uri.User.Password()
	opts.SetPassword(password)
	// tls config
	opts.SetTLSConfig(m.newTLSConfig())

	return opts
}

func (m *mqttPubSub) Close() error {
	m.consumer.Disconnect(0)
	m.producer.Disconnect(0)

	return nil
}
