package main

import "fmt"
import "encoding/json"
import "os"
type KeyValue struct {
	Key   string
	Value string
}

func main() {
	file, _ := os.Open("mr-1-1")

	dec := json.NewDecoder(file)
	for {
	  var kv KeyValue
	  if err := dec.Decode(&kv); err != nil {
		break
	  }
		fmt.Println(kv)
	}
}
