// +build e2e

package influxclient_test

import (
	"context"
	"fmt"
	influxclient "github.com/influxdata/influxdb-client-go/inluxclient"
	"log"
	"testing"
	"time"
)

func TestClient_Query(t *testing.T) {
	client, err := influxclient.New(influxclient.Params{ServerURL: "https://ec2-13-57-118-201.us-west-1.compute.amazonaws.com:9999/", AuthToken: "Pm_gVYAZs5EAEAKEdvp-7UJ2TenDmt2keqU4gAQnC-uBPLM7aQ6uFV2B4fAk7TDKIg5_twxobiRK_Jr2EGd3RQ=="})
	//client, err := influxclient.New(influxclient.Params{ServerURL: "http://localhost:666", AuthToken: "Pm_gVYAZs5EAEAKEdvp-7UJ2TenDmt2keqU4gAQnC-uBPLM7aQ6uFV2B4fAk7TDKIg5_twxobiRK_Jr2EGd3RQ=="})
	if err != nil {
		log.Fatal(err)
	}
	res, err := client.Query(context.Background(), "05ebbd1b1f0bdb9f", `
from(bucket: "room_monitoring") 
|> range(start: -5m) 
|> filter(fn: (r) => r["_measurement"] == "air") 
|> filter(fn: (r) => r["_field"] == "temp") 
|> drop(columns: ["location"])`)

	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Time\tTemp\tSensor\n")
	for res.NextTable() && res.Err() == nil {
		for res.NextRow() {
			// read values
			timestamp := res.ValueByName("_time").(time.Time)
			temp := res.ValueByName("_value").(float64)
			sensor := res.ValueByName("sensor").(string)
			fmt.Printf("%s\t%.2f\t%s\n", timestamp.String(), temp, sensor)
		}
	}
	if res.Err() != nil {
		log.Fatal(res.Err())
	}

}
