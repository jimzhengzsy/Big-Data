package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
	//elastic "gopkg.in/olivere/elastic.v7"
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
	ctx := context.Background()
	go kafkaProduce(ctx, dataList)
	kafkaConsume(ctx)
	//dataInsersion(dataList)

	time.Sleep(3600 * time.Second)
	ch <- "Start fetch data complete next fetch will start after one hour"

}

// Kibana
// 消费后去重,关键词/敏感词过滤
//多模式匹配 ac自动机O(1)
// 文章自动分类
// Redis + md5
// DDIA
// csapp

// 在Redis 上实现simhash （实现可以写到简历里）
// Simhash - 64 位
// 计算差四位及四位以内
// 抽屉原理

// 用GO写，trie树双数组

// 多思考怎么才是客观实现

// Api Key: 5b3105d3a9e6a64376361e84e0b6660d
//url := "http://api.tianapi.com/generalnews/index?key=5b3105d3a9e6a64376361e84e0b6660d"
// url := "http://api.tianapi.com/topnews/index?key=5b3105d3a9e6a64376361e84e0b6660d"

//fmt.Println(fetch("http://api.tianapi.com/topnews/index?key=5b3105d3a9e6a64376361e84e0b6660d"))

// func main() {

// 	//p := fetch("http://api.tianapi.com/topnews/index?key=5b3105d3a9e6a64376361e84e0b6660d")
// 	//printPage(p)
// 	// url := "http://api.tianapi.com/topnews/index?key=5b3105d3a9e6a64376361e84e0b6660d"
// 	// req, _ := http.NewRequest("GET", url, nil)
// 	// res, _ := http.DefaultClient.Do(req)
// 	// defer res.Body.Close()
// 	// body, _ := ioutil.ReadAll(res.Body)
// 	// fmt.Println(string(body))

// 	//
// 	fetchDataChannel := make(chan string)
// 	go fetchDataFunc(fetchDataChannel)
// 	select {
// 	case <-fetchDataChannel:
// 		fmt.Println("Fetch new data")
// 	}

// }
