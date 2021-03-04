package main

import (
	"math"
	"strconv"
	"strings"
)

type SimHash struct {
	IntSimHash int64
	HashBits   int
}

// func main() {
// 	str := "夜空中最亮的星是否记清那仰望的人 心里的孤独和叹息夜空中最亮的星是否记清那仰望的人 心里的孤独和叹息夜空中最亮的星是否记清那仰望的人 心里的孤独和叹息夜空中最亮的星是否记清那仰望的人 心里的孤独和叹息夜空中最亮的星是否记清那仰望的人  心里的孤独和叹息夜空中最亮的星是否记清那仰望的人 心里的孤独和叹息"
// 	str2 := "夜空中最亮的星是否记清那仰望的人 心里的孤独和叹息夜空中最亮的星是否记清那仰望的人 心里的孤独和叹息夜空中最亮的星是否记清那仰望的人 心里的孤独和叹息夜空中最亮的星是否记清那仰望的人 心里的孤独和叹息夜空中最亮的星是否记清那仰望的人  心里的孤独和叹息夜空中最亮的星是否记清那仰望的人 心里的孤独和叹息"
// 	//str3:="nai nai ge xiong cao"
// 	s := params()
// 	//str hash 值
// 	hash := s.Simhash(str)
// 	fmt.Println(hash)
// 	//str2 hash
// 	hash2 := s.Simhash(str2)
// 	fmt.Println(hash2)
// 	//计算相似度
// 	sm := s.Similarity(hash, hash2)
// 	fmt.Println(sm)
// 	//距离
// 	ts := s.HammingDistance(hash, hash2)
// 	fmt.Println(ts)

// }

/**
距离 补偿
*/
func (s *SimHash) HammingDistance(hash, other int64) int {
	x := (hash ^ other) & ((1 << uint64(s.HashBits)) - 1)
	tot := 0
	for x != 0 {
		tot += 1
		x &= x - 1
	}
	return tot
}

/**
  相似度
*/
func (s *SimHash) Similarity(hash, other int64) float64 {
	a := float64(hash)
	b := float64(other)
	if a > b {
		return b / a
	}
	return a / b
}

/**
  海明距离hash
*/
func (s *SimHash) Simhash(str string) int64 {
	m := strings.Split(str, " ")

	token_int := make([]int, s.HashBits)
	for i := 0; i < len(m); i++ {
		temp := m[i]
		t := s.Hash(temp)
		for j := 0; j < s.HashBits; j++ {
			bitMask := int64(1 << uint(j))
			if t&bitMask != 0 {
				token_int[j] += 1
			} else {
				token_int[j] -= 1
			}
		}

	}
	var fingerprint int64 = 0
	for i := 0; i < s.HashBits; i++ {
		if token_int[i] >= 0 {
			fingerprint += 1 << uint64(i)
		}
	}
	return fingerprint
}

/**
  初始化
*/
func params() (s *SimHash) {
	s = &SimHash{}
	s.HashBits = 64
	return s
}

/**
hash 值
*/
func (s *SimHash) Hash(token string) int64 {
	if token == "" {
		return 0
	} else {
		x := int64(int(token[0]) << 7)
		m := int64(1000003)
		mask := math.Pow(2, float64(s.HashBits-1))
		s := strconv.FormatFloat(mask, 'f', -1, 64)
		tsk, _ := strconv.ParseInt(s, 10, 64)
		for i := 0; i < len(token); i++ {
			tokens := int64(int(token[0]))
			x = ((x * m) ^ tokens) & tsk
		}
		x ^= int64(len(token))
		if x == -1 {
			x = -2
		}
		return int64(x)
	}
}
