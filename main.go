package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

type ApiResponse struct {
	Data map[string]int `json:"data"`
}

func main() {
	nodeFlag := flag.Int("n", 1, "number of nodes to be used")
	flag.Parse()
	files := flag.Args()
	if *nodeFlag != len(files) {
		panic("The no of files dont match the number of nodes")
	}
	apiClient, err := client.NewClientWithOpts(client.FromEnv)
	check(err)
	defer apiClient.Close()

	reader, err := apiClient.ImagePull(context.Background(), "tushar12345678/wordcount:latest", image.PullOptions{})
	check(err)
	io.Copy(os.Stdout, reader)

	containers := make([]string, 0)
	ports := make([]string, 0)

	for i := 0; i < *nodeFlag; i++ {
		port := "800" + strconv.Itoa(i)
		ports = append(ports, port)
		runContainer(apiClient, port, &containers)
	}

	fmt.Println("Containers processing data")

	time.Sleep(1 * time.Second)

	processedData := make(map[string]int)

	for i, file := range files {
		data, err := os.ReadFile(file)
		check(err)
		resp := sendData(string(data), ports[i])
		for key, val := range resp {
			processedData[key] += val
		}
	}

	for _, id := range containers {
		if err := apiClient.ContainerRemove(context.Background(), id, container.RemoveOptions{Force: true}); err != nil {
			panic(err)
		}
	}

	fmt.Println("The combined word count is ", processedData)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func runContainer(apiClient *client.Client, port string, containers *[]string) {
	hostBinding := nat.PortBinding{
		HostIP:   "0.0.0.0",
		HostPort: port,
	}
	containerPort, err := nat.NewPort("tcp", "3000")
	check(err)
	portBinding := nat.PortMap{containerPort: []nat.PortBinding{hostBinding}}
	cont, err := apiClient.ContainerCreate(
		context.Background(),
		&container.Config{
			Image: "tushar12345678/wordcount:latest",
		},
		&container.HostConfig{
			PortBindings: portBinding,
		}, nil, nil, fmt.Sprintf("wordCount%s", port))
	check(err)
	apiClient.ContainerStart(context.Background(), cont.ID, container.StartOptions{})
	fmt.Printf("Container %s is started\n", cont.ID)
	*containers = append(*containers, cont.ID)
}

func sendData(data string, port string) map[string]int {
	postBody, _ := json.Marshal(map[string]string{
		"data": data,
	})

	responseBody := bytes.NewReader(postBody)

	resp, err := http.Post(fmt.Sprintf("http://localhost:%s", port), "application/json", responseBody)
	check(err)
	defer resp.Body.Close()

	var apiResp ApiResponse

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		panic("Cannot deserialize data")
	}

	return apiResp.Data
}
