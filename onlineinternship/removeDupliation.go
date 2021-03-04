package main

import (
	"github.com/yanyiwu/gojieba"
)

func cutString(s string) []string {
	var words []string
	x := gojieba.NewJieba()
	defer x.Free()
	words = x.Cut(s, true)
	return words
}

func getKeyWords(s string) []string {
	var words []string
	x := gojieba.NewJieba()
	defer x.Free()
	words = x.Extract(s, 15)
	return words
}
