package main

import "os"
import "fmt"
import "mapreduce"
import "strings"
import "unicode"
import "errors"
import "strconv"

// import "log"

import "container/list"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file content. the return
// value should be a list of key/value pairs, each represented
// by a mapreduce.KeyValue.
func Map(value string) *list.List {
	delimiter := func(c rune) bool {
		return !unicode.IsLetter(c)
	}

	words := strings.FieldsFunc(value, delimiter)
	result := list.New()
	for _, word := range words {
		exist, err := ListContains(result, word)
		if err != nil {
			// the value is not exist
			// log.Println(word, 1)
			entry := mapreduce.KeyValue{Key: word, Value: "1"}
			result.PushBack(entry)
		} else {
			if keyValue, ok := exist.Value.(mapreduce.KeyValue); ok {
				count, _ := strconv.Atoi(keyValue.Value)
				count++
				// log.Println(word, count)
				result.Remove(exist)
				result.PushBack(mapreduce.KeyValue{Key: word, Value: strconv.Itoa(count)})
			}

		}
	}
	return result
}

func ListContains(l *list.List, v interface{}) (*list.Element, error) {
	for e := l.Front(); e != nil; e = e.Next() {
		if keyValue, ok := e.Value.(mapreduce.KeyValue); ok {
			if keyValue.Key == v {
				return e, nil
			}
		}
	}
	return nil, errors.New("no value")
}

// called once for each key generated by Map, with a list
// of that key's string value. should return a single
// output value for that key.
func Reduce(key string, values *list.List) string {
	var count int
	for e := values.Front(); e != nil; e = e.Next() {
		if kv, ok := e.Value.(string); ok {
			temp, _ := strconv.Atoi(kv)
			fmt.Println(key, count)
			count += temp
		}
	}
	return strconv.Itoa(count)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) != 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		if os.Args[3] == "sequential" {
			mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
		} else {
			mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
			// Wait until MR is done
			<-mr.DoneChannel
		}
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
	}
}