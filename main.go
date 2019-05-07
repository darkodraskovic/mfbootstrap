package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/pelletier/go-toml"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

var (
	urlUsers  = "http://localhost/users"
	urlTokens = "http://localhost/tokens"
)

type Config struct {
	URL         string
	ExternalID  string
	ExternalKey string
}

type Channel struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type MainfluxData struct {
	MainfluxID    string    `json:"mainflux_id"`
	MainfluxKey   string    `json:"mainflux_key"`
	MainfluxChans []Channel `json:"mainflux_channels"`
	Content       string    `json:"content"`
}

var choke = make(chan [3]string)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal("Error loading config. ", err)
	}

	mfData := getConfig(cfg)
	fmt.Printf("%+v\n", *mfData)

	client := connectToMQTTBroker("localhost:1883", mfData.MainfluxID, mfData.MainfluxKey)

	msg := `[{"bn":"some-base-name:","bt":1.276020076001e+09, "bu":"A","bver":5, "n":"voltage","u":"V","v":120.1}, {"n":"current","t":-5,"v":1.2}, {"n":"current","t":-4,"v":1.3}]`
	topic := flag.String("topic", "channels/"+mfData.MainfluxChans[0].ID+"/messages", "The topic name to/from which to publish/subscribe")
	qos := flag.Int("qos", 0, "The Quality of Service 0,1,2 (default 0)")
	payload := flag.String("message", msg, "The message text to publish (default empty)")
	num := flag.Int("num", 3, "The number of messages to publish or subscribe (default 1)")
	flag.Parse()

	if token := client.Subscribe(*topic, byte(*qos), nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	for i := 0; i < *num; i++ {
		fmt.Println("---- doing publish ----")
		token := client.Publish(*topic, byte(*qos), false, *payload)
		token.Wait()
	}

	receiveCount := 0
	for receiveCount < *num {
		incoming := <-choke
		fmt.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", incoming[0], incoming[1])
		receiveCount++
	}
	client.Disconnect(250)
}

func connectToMQTTBroker(mqttURL, thingID, thingKey string) mqtt.Client {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(mqttURL)
	opts.SetClientID("gateway")
	opts.SetUsername(thingID)
	opts.SetPassword(thingKey)
	opts.SetCleanSession(true)
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		choke <- [3]string{msg.Topic(), string(msg.Payload())}
	})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Failed to connect to MQTT broker: %s", token.Error())
	}

	return client
}

func loadConfig() (Config, error) {
	tomlConfig, err := toml.LoadFile("config.toml")
	if err != nil {
		fmt.Println("Error ", err.Error())
		return Config{}, err
	} else {
		return Config{
			URL:         tomlConfig.Get("server.URL").(string),
			ExternalID:  tomlConfig.Get("thing.external_ID").(string),
			ExternalKey: tomlConfig.Get("thing.external_key").(string),
		}, nil
	}
}

func getConfig(cfg Config) *MainfluxData {
	url := cfg.URL + cfg.ExternalID
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal("Error reading request. ", err)
	}

	req.Header.Set("Authorization", cfg.ExternalKey)

	client := &http.Client{Timeout: time.Second * 10}

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("Error reading response. ", err)
	}
	defer resp.Body.Close()

	fmt.Println("response Status:", resp.Status)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Error reading body. ", err)
	}

	var mfData MainfluxData
	json.Unmarshal([]byte(body), &mfData)

	return &mfData
}
