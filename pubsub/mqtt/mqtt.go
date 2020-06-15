// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mqtt

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	// Keys
	mqttURL          = "url"
	mqttQOS          = "qos"
	mqttRetain       = "retain"
	mqttClientID     = "consumerID"
	mqttCleanSession = "cleanSession"

	// errors
	errorMsgPrefix = "mqtt pub sub error:"

	// Defaults
	defaultQOS          = 0
	defaultRetain       = false
	defaultWait         = 3 * time.Second
	defaultCleanSession = true
)

var (
	clientIDSub = "%s-sub"
	clientIDPub = "%s-pub"
)

// mqttPubSub type allows sending and receiving data to/from MQTT broker.
type mqttPubSub struct {
	clientSub mqtt.Client
	clientPub mqtt.Client
	metadata  *metadata
	logger    logger.Logger
}

// NewMQTTPubSub returns a new mqttPubSub instance.
func NewMQTTPubSub(logger logger.Logger) pubsub.PubSub {
	return &mqttPubSub{logger: logger}
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

	return &m, nil
}

// Init parses metadata and creates a new Pub Sub client.
func (m *mqttPubSub) Init(metadata pubsub.Metadata) error {
	mqttMeta, err := parseMQTTMetaData(metadata)
	if err != nil {
		return err
	}
	m.metadata = mqttMeta
	clientIDSub = fmt.Sprintf(clientIDSub, m.metadata.clientID)
	clientIDPub = fmt.Sprintf(clientIDPub, m.metadata.clientID)

	clientP, err := m.connect(clientIDPub)
	if err != nil {
		return err
	}

	m.clientPub = clientP

	m.logger.Debug("mqtt message bus initialization complete")
	return nil
}

// Publish the topic to mqtt pub sub.
func (m *mqttPubSub) Publish(req *pubsub.PublishRequest) error {
	m.logger.Debugf("mqtt publishing topic %s with data: %v", req.Topic, req.Data)

	token := m.clientPub.Publish(req.Topic, m.metadata.qos, m.metadata.retain, req.Data)
	if !token.WaitTimeout(defaultWait) || token.Error() != nil {
		return fmt.Errorf("mqtt error from publish: %v", token.Error())
	}
	return nil
}

func (m *mqttPubSub) subscribeHandler(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	registerHandler := func(client mqtt.Client, mqttMsg mqtt.Message) {
		msg := &pubsub.NewMessage{Topic: mqttMsg.Topic(), Data: mqttMsg.Payload()}
		if err := handler(msg); err != nil {
			m.clientSub.Disconnect(0)
		}
	}
	token := m.clientSub.Subscribe(req.Topic, m.metadata.qos, registerHandler)
	if err := token.Error(); err != nil {
		return err
	}
	return nil
}

// isHandlerReady checks if the subscribe grpc handler is ready.
func (m *mqttPubSub) isHandlerReady(handler func(msg *pubsub.NewMessage) error) bool {
	sampleCloudEvent := pubsub.NewCloudEventsEnvelope("validate", "", "", "", nil)
	sampleData, _ := json.Marshal(sampleCloudEvent)
	msg := &pubsub.NewMessage{
		Data:  sampleData,
		Topic: "",
	}
	err := handler(msg)
	if err != nil {
		return false
	}
	return true
}

// Subscribe to the mqtt pub sub topic.
func (m *mqttPubSub) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	// initial subscription
	var err error
	m.clientSub, err = m.connect(clientIDSub)
	if err != nil {
		return err
	}
	err = m.subscribeHandler(req, handler)
	if err != nil {
		return fmt.Errorf("mqtt error from subscribe: %v", err)
	}

	go func(mps *mqttPubSub, handler func(msg *pubsub.NewMessage) error) {
		timer := time.NewTicker(defaultWait)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				if !mps.clientSub.IsConnected() && mps.isHandlerReady(handler) {
					mps.logger.Debug("connecting for subscription")
					mps.clientSub, err = mps.connect(clientIDSub)
					if err != nil {
						continue
					}
					if err = mps.subscribeHandler(req, handler); err != nil {
						mps.logger.Debugf("error creating subscribe handler: %v", err)
						mps.clientSub.Disconnect(0)
						continue
					}
				}
			}
		}
	}(m, handler)

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

func (m *mqttPubSub) createClientOptions(uri *url.URL, clientID string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.SetClientID(clientID)
	opts.SetCleanSession(m.metadata.cleanSession)
	opts.AddBroker(uri.Host)
	opts.SetUsername(uri.User.Username())
	password, _ := uri.User.Password()
	opts.SetPassword(password)
	return opts
}
