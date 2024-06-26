package main

import (
	s "bitcaskClone/store"
	"os"
)

func main() {
	file, _ := os.Open("./store/instructions.txt")
	defer file.Close()
	store := s.Initialize()
	store.Stdin = file
	store.Start()
}
