package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/gzip"
	_ "github.com/segmentio/kafka-go/lz4"
	_ "github.com/segmentio/kafka-go/snappy"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var wg sync.WaitGroup

const listenPartitionNumber = 300

func main() {
	//arguments : 실행파일명 브로커주소 토픽명
	arguments := os.Args
	if len(arguments) != 3 {
		fmt.Println("brokers(ex:10.0.0.1:9092,10.0.0.2:9092) and topicName(ex:bambi-test) require!")
		return
	}

	brokers := strings.Split(arguments[1], ",")
	topicName := arguments[2]

	fmt.Println("brokers : ", brokers)
	fmt.Println("topicName : ", topicName)

	wg.Add(listenPartitionNumber);
	for i := 0; i < listenPartitionNumber; i++ {
		go func(partition int) {
			r := kafka.NewReader(kafka.ReaderConfig{
				Brokers:   brokers,
				Topic:     topicName,
				Partition: partition,
				MinBytes:  1,    // 1B
				MaxBytes:  10e6, // 10MB
			})
			defer func() {
				_ = r.Close()
				wg.Done()
			}()

			// 파티션정보를 사전에 알수있는 방법이 없어서,
			// 일단 listenPartitionNumber 숫자대로 offset 요청을 보냈을때 3초간 응답이 없으면 없는 파티션으로 간주
			deadlineContext, _ := context.WithDeadline(context.Background(), time.Now().Add(time.Second*3))
			err := r.SetOffsetAt(deadlineContext, time.Now())
			if err != nil {
				return;
			}

			fmt.Println(topicName + ":" + strconv.Itoa(partition) + " partition consumer start")

			for {
				m, err := r.ReadMessage(context.Background())
				if err != nil {
					fmt.Println(strconv.Itoa(partition) + " partition error : " + err.Error())
					break
				}
				fmt.Printf("[%v],topic:[%v],partition:[%v],offset:[%v],key:[%s]\n", m.Time, m.Topic, m.Partition, m.Offset, string(m.Key))
				fmt.Println(string(m.Value))
			}
		}(i)
	}

	// hold main
	wg.Wait()

	fmt.Println("program exit.")
}
