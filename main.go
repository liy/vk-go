package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

func main() {
	// TODO: do we need this?
	kconn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		panic(err.Error())
	}

	router := mux.NewRouter()

	// Get a list of the topic names
	router.HandleFunc("/topics", func(res http.ResponseWriter, req *http.Request) {
		res.Header().Set("Access-Control-Allow-Origin", "*")
		res.Header().Set("Content-Type", "application/json")

		partitions, err := kconn.ReadPartitions()
		if err != nil {
			http.Error(res, err.Error(), http.StatusInternalServerError)
			return
		}

		m := map[string]int{}
		for _, p := range partitions {
			m[p.Topic] = p.ID
		}

		topics := make([]string, 0, len(m))
		for topic := range m {
			topics = append(topics, topic)
		}

		bytes, _ := json.Marshal(topics)
		res.Write(bytes)
	})

	// Get the partitions of a topics
	router.HandleFunc("/partitions/{topic}", func(res http.ResponseWriter, req *http.Request) {
		res.Header().Set("Access-Control-Allow-Origin", "*")
		res.Header().Set("Content-Type", "application/json")
		vars := mux.Vars(req)

		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancel()

		partitions, err := kafka.LookupPartitions(ctx, "tcp", "localhost:9092", vars["topic"])
		if err != nil {
			http.Error(res, err.Error(), http.StatusInternalServerError)
			return
		}

		bytes, _ := json.Marshal(partitions)
		res.Write(bytes)
	})

	// Get a list of broker information
	router.HandleFunc("/brokers", func(res http.ResponseWriter, req *http.Request) {
		res.Header().Set("Access-Control-Allow-Origin", "*")
		res.Header().Set("Content-Type", "application/json")

		brokers, err := kconn.Brokers()
		if err != nil {
			http.Error(res, err.Error(), http.StatusInternalServerError)
			return
		}

		bytes, err := json.Marshal(brokers)
		if err != nil {
			http.Error(res, err.Error(), http.StatusInternalServerError)
		}
		res.Write(bytes)
	})

	// Subscribe to a topic to receive messages via websocket, read only
	router.HandleFunc("/subscribe/{topic}/{partition}", func(res http.ResponseWriter, req *http.Request) {
		var upgrader = websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		}

		conn, _ := upgrader.Upgrade(res, req, nil)
		defer conn.Close()
		vars := mux.Vars(req)

		partition, err := strconv.ParseUint(vars["partition"], 10, 32)
		if err != nil {
			http.Error(res, "Invalid partition number", http.StatusInternalServerError)
			return
		}

		reader := kafka.NewReader(kafka.ReaderConfig{
			Dialer:    kafka.DefaultDialer,
			Brokers:   []string{"localhost:9092"},
			Topic:     vars["topic"],
			Partition: int(partition),
			MinBytes:  10e3, // 10KB
			MaxBytes:  10e6, // 10MB,
		})
		// TODO: set offset
		reader.SetOffset(0)

		for {
			m, err := reader.ReadMessage(req.Context())
			if err != nil {
				break
			}
			conn.WriteMessage(websocket.TextMessage, m.Value)
		}
	})

	type Action struct {
		Key  int    `json:"key"`
		Data string `json:"data"`
	}

	router.HandleFunc("/open/{topic}", func(res http.ResponseWriter, req *http.Request) {
		var upgrader = websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		}
		conn, _ := upgrader.Upgrade(res, req, nil)
		vars := mux.Vars(req)

		// To inform other goroutine that socket has closed, stop working.
		closeSocket := make(chan bool)

		// Handle client to server actions
		go func() {
			var action Action
			for {
				err := conn.ReadJSON(&action)
				if err != nil {
					fmt.Println("Session closed")
					// Socket close,
					closeSocket <- true
					return
				}

				// TODO: Handle client data
				fmt.Println(action)
			}
		}()

		// handle server to client subscribed messages
		partitions, err := kconn.ReadPartitions()
		messageChan := make(chan []byte, len(partitions))
		// partition, err := strconv.ParseUint(vars["partition"], 10, 32)
		if err != nil {
			http.Error(res, "Invalid partition number", http.StatusInternalServerError)
			return
		}

		for partition := range partitions {
			go func(pt int) {
				reader := kafka.NewReader(kafka.ReaderConfig{
					Dialer:    kafka.DefaultDialer,
					Brokers:   []string{"localhost:9092"},
					Topic:     vars["topic"],
					Partition: pt,
					MinBytes:  10e3, // 10KB
					MaxBytes:  10e6, // 10MB,
				})
				// TODO: set offset
				reader.SetOffset(0)

				for {
					select {
					case <-closeSocket:
						fmt.Println("exist goroutine 1")
						return
					default:
						m, err := reader.ReadMessage(req.Context())
						if err != nil {
							return
						}
						// send message to channel
						bytes, err := json.Marshal(m)
						if err == nil {
							messageChan <- bytes
						}
					}
				}
			}(partition)
		}

		for {
			select {
			case <-closeSocket:
				fmt.Println("exist goroutine 2")
				return
			case bytes := <-messageChan:
				conn.WriteMessage(websocket.TextMessage, bytes)
			}
		}
	})

	http.ListenAndServe(":8080", router)
}
