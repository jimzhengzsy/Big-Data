package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
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
	Timestamp uint     `json:"timestamp"`
	Source    string   `json:"source"`
	Title     string   `json:"title"`
	Body      string   `json:"body"`
	Types     []string `json:"types"`
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

	//error := json.NewDecoder(resp.Body).Decode(&p)
	json.Unmarshal([]byte(body), &p)

	// if error != nil {
	// 	fmt.Println("json get error:", error)
	// 	return p
	// }
	fmt.Println("Successful")

	return p
}

func printPage(p Page) {
	fmt.Println(fmt.Sprintf("%+v", p))
}

func fetchData(p Page) {
	nl := p.Newslist
	length := len(nl)
	dataList := []Data{}
	for i := 0; i < length; i++ {
		dataList[i].Body = p.Newslist[i].Description
		dataList[i].Source = p.Newslist[i].Source
		//dataList[i].Timestamp = uint(p.Newslist[i].Ctime)
		dataList[i].Title = p.Newslist[i].Title
		//dataList[i].Types = p.Newslist[i].
	}
}

// func printPage(p Page) {
// 	fmt.Println(p.Timestamp)
// 	fmt.Println(p.Source)
// 	fmt.Println(p.Title)
// 	fmt.Println(p.Body)
// 	fmt.Println(p.Types)
// }

func main() {
	// Api Key: 5b3105d3a9e6a64376361e84e0b6660d
	//url := "http://api.tianapi.com/generalnews/index?key=5b3105d3a9e6a64376361e84e0b6660d"
	// url := "http://api.tianapi.com/topnews/index?key=5b3105d3a9e6a64376361e84e0b6660d"

	//fmt.Println(fetch("http://api.tianapi.com/topnews/index?key=5b3105d3a9e6a64376361e84e0b6660d"))
	p := fetch("http://api.tianapi.com/topnews/index?key=5b3105d3a9e6a64376361e84e0b6660d")
	printPage(p)
	// url := "http://api.tianapi.com/topnews/index?key=5b3105d3a9e6a64376361e84e0b6660d"
	// req, _ := http.NewRequest("GET", url, nil)
	// res, _ := http.DefaultClient.Do(req)
	// defer res.Body.Close()
	// body, _ := ioutil.ReadAll(res.Body)
	// fmt.Println(string(body))

}
