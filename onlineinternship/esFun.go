package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/olivere/elastic"
	//elastic "gopkg.in/olivere/elastic.v7"
)

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
			Index("news").
			Type("_doc").
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

	// 有Type要注意
	get1, err := esclient.Get().
		Index("news").
		Type("_doc").
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
		Index("news").
		Type("_doc").
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
