package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/olivere/elastic"
	"github.com/segmentio/kafka-go"
	//elastic "gopkg.in/olivere/elastic.v7"
)

const (
	topic         = "News"
	brokerAddress = "localhost:9092"
)

type Page struct {
	Code     int    `json:"code"`
	Msg      string `json:"msg"`
	Newslist []News `json:"newslist"`
}

type News struct {
	Ctime       string `json:"ctime"`
	Title       string `json:"title"`
	Description string `json:"description"`
	PicUrl      string `json:"picUrl"`
	Url         string `json:"url"`
	Source      string `json:"source"`
}

type Data struct {
	Timestamp string `json:"timestamp"`
	Source    string `json:"source"`
	Title     string `json:"title"`
	Body      string `json:"body"`
	//Types     []string `json:"types"`
}

func fetch(url string) Page {
	fmt.Println("Fetch Url", url)

	//var p Page
	p := Page{}

	// 创建请求
	req, _ := http.NewRequest("GET", url, nil)
	// 创建HTTP客户端
	client := &http.Client{}
	// 发出请求
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Http get err:", err)
		return p
	}
	if resp.StatusCode != 200 {
		fmt.Println("Http status code:", resp.StatusCode)
		return p
	}
	// 读取HTTP响应正文
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	//json.NewDecoder(resp.Body).Decode(&p)

	if err != nil {
		fmt.Println("Read error", err)
		return p
	}

	json.Unmarshal([]byte(body), &p)
	fmt.Println("Successful")

	return p
}

func printPage(p Page) {
	fmt.Println(fmt.Sprintf("%+v", p))
}

func fetchData(p Page) []Data {
	nl := p.Newslist
	length := len(nl)
	dataList := []Data{}
	fmt.Println("The length is ", length)
	for i := 0; i < length; i++ {
		// reflect 要求 名字一样，所以用的append
		temp := Data{p.Newslist[i].Ctime, p.Newslist[i].Source, p.Newslist[i].Title, p.Newslist[i].Description}
		dataList = append(dataList, temp)
	}

	fmt.Println(fmt.Sprintf("%+v", dataList))
	return dataList
}

/* task manager goroutine / select/ time after
channel -> kafka
producer consumer model
DDIA
golang trie
1. md5 + redis (specific)
2. same hash + redis (opaque)
3. bloom filter + redis(low overhead)

raw dataset backup: -> es
*/

func fetchDataFunc(ch chan string) {

	p := fetch("http://api.tianapi.com/topnews/index?key=5b3105d3a9e6a64376361e84e0b6660d")
	printPage(p)
	dataList := fetchData(p)
	checkDataIndex("news")
	dataInsersion(dataList)

	time.Sleep(3600 * time.Second)
	ch <- "Start fetch data complete next fetch will start after one hour"

}

// type Data struct {
// 	Timestamp string `json:"timestamp"`
// 	Source    string `json:"source"`
// 	Title     string `json:"title"`
// 	Body      string `json:"body"`
// 	//Types     []string `json:"types"`
// }

const mappingNews = `{
    "mappings":{
        "news":{
            "timestamp":                 { "type": "string" },
            "source":         { "type": "string" },
            "title":            { "type": "string" },
            "body":            { "type": "string" },
            }
        }
	}`

func checkDataIndex(index string) {
	ctx := context.Background()
	esclient, err := GetESClient()
	if err != nil {
		fmt.Println("Error initializing : ", err)
		panic("Client fail ")
	}

	exists, err := esclient.IndexExists(index).Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}

	if !exists {
		// Create a new index.
		createIndex, err := esclient.CreateIndex(index).BodyString(mappingNews).Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}
}

func dataInsersion(dataList []Data) {
	ctx := context.Background()
	esclient, err := GetESClient()
	if err != nil {
		fmt.Println("Error initializing : ", err)
		panic("Client fail ")
	}
	length := len(dataList)
	for i := 0; i < length; i++ {
		dataJSON, err := json.Marshal(dataList[i])
		js := string(dataJSON)
		ind, err := esclient.Index().
			Index("News").
			BodyJson(js).
			Do(ctx)

		if err != nil {
			panic(err)
		}

		fmt.Printf("The news id is %s, the index is  %s\n", ind.Id, ind.Index)

	}
}

func dataQuerying(id string) {
	ctx := context.Background()
	esclient, err := GetESClient()
	// 根据id查询文档
	get1, err := esclient.Get().
		Index("News").
		Id(id).
		Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	if get1.Found {
		fmt.Printf("News id=%s Version =%d Index =%s\n", get1.Id, get1.Version, get1.Index)
	}

	news := Data{}
	// 提取文档内容，原始类型是json数据
	data, _ := get1.Source.MarshalJSON()
	// 将json转成struct结果
	json.Unmarshal(data, &news)
	// 打印结果
	fmt.Println(news.Body)
}

func dataDelete(id string) {
	ctx := context.Background()
	esclient, err := GetESClient()
	_, err = esclient.Delete().
		Index("News").
		Id(id).
		Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
}

func GetESClient() (*elastic.Client, error) {

	client, err := elastic.NewClient(elastic.SetURL("http://localhost:9200"),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false))

	fmt.Println("ES initialized...")

	return client, err

}

func kafkaProduce(ctx context.Context) {
	// initialize a counter
	i := 0

	l := log.New(os.Stdout, "kafka writer: ", 0)
	// intialize the writer with the broker addresses, and the topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		// assign the logger to the writer
		Logger: l,
	})

	for {
		// each kafka message has a key and value. The key is used
		// to decide which partition (and consequently, which broker)
		// the message gets published on
		err := w.WriteMessages(ctx, kafka.Message{
			Key: []byte(strconv.Itoa(i)),
			// Write news
			Value: []byte("This is message" + strconv.Itoa(i)),
		})
		if err != nil {
			panic("could not write message " + err.Error())
		}

		// log a confirmation once the message is written
		fmt.Println("writes:", i)
		i++
		// sleep for a second
		time.Sleep(time.Second)
	}
}

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
	}
}

// Api Key: 5b3105d3a9e6a64376361e84e0b6660d
//url := "http://api.tianapi.com/generalnews/index?key=5b3105d3a9e6a64376361e84e0b6660d"
// url := "http://api.tianapi.com/topnews/index?key=5b3105d3a9e6a64376361e84e0b6660d"

//fmt.Println(fetch("http://api.tianapi.com/topnews/index?key=5b3105d3a9e6a64376361e84e0b6660d"))
func main() {

	//p := fetch("http://api.tianapi.com/topnews/index?key=5b3105d3a9e6a64376361e84e0b6660d")
	//printPage(p)
	// url := "http://api.tianapi.com/topnews/index?key=5b3105d3a9e6a64376361e84e0b6660d"
	// req, _ := http.NewRequest("GET", url, nil)
	// res, _ := http.DefaultClient.Do(req)
	// defer res.Body.Close()
	// body, _ := ioutil.ReadAll(res.Body)
	// fmt.Println(string(body))
	fetchDataChannel := make(chan string)
	go fetchDataFunc(fetchDataChannel)
	select {
	case <-fetchDataChannel:
		fmt.Println("Fetch new data")
	}

}
