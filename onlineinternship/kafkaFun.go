package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topic         = "News"
	brokerAddress = "localhost:9092"
)

type Data struct {
	Timestamp string `json:"timestamp"`
	Source    string `json:"source"`
	Title     string `json:"title"`
	Body      string `json:"body"`
	//Types     []string `json:"types"`
}

func kafkaProduce(ctx context.Context, dataList []Data) {
	// initialize a counter

	l := log.New(os.Stdout, "kafka writer: ", 0)
	// intialize the writer with the broker addresses, and the topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		// assign the logger to the writer
		Logger: l,
	})

	//newsDataBytes, _ := json.Marshal(dataList)
	length := len(dataList)
	for i := 0; i < length; i++ {
		newsDataBytes, _ := json.Marshal(dataList[i])
		// each kafka message has a key and value. The key is used
		// to decide which partition (and consequently, which broker)
		// the message gets published on
		err := w.WriteMessages(ctx, kafka.Message{
			Key: []byte(strconv.Itoa(i)),
			// Write news
			Value: []byte(newsDataBytes),
		})
		if err != nil {
			panic("could not write message " + err.Error())
		}

		// log a confirmation once the message is written
		fmt.Println("writes:", i)
		// sleep for a second
		time.Sleep(time.Second)
	}
}

// Kibana
// 消费后去重,关键词/敏感词过滤
//多模式匹配 ac自动机O(1)
// 文章自动分类
// Redis + md5
// DDIA
// csapp
func kafkaConsume(ctx context.Context) {
	// create a new logger that outputs to stdout
	// and has the `kafka reader` prefix
	l := log.New(os.Stdout, "kafka reader: ", 0)
	// initialize a new reader with the brokers and topic
	// the groupID identifies the consumer and prevents
	// it from receiving duplicate messages
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: "News",
		// assign the logger to the reader
		Logger: l,
	})
	for {
		// the `ReadMessage` method blocks until we receive the next event
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
		// after receiving the message, log its value
		fmt.Println("received: ", string(msg.Value))

		// NLP 分类生成，情感分析
		// Web后端
		// 自己设计的能力
		// RESTful API -》惯例来设计API接口

		// unmarshal 回 struct、
		news := Data{}
		// newsLength := len(news)

		json.Unmarshal(msg.Value, &news)
		// 做去重
		kewWords := getKeyWords(news.Body)
		fmt.Println("Tokenized News Key Words:", strings.Join(kewWords, "/"))
		weights := getWeights(news.Body)
		fmt.Println("Weights of Key Words:", weights)

	}
}
