package influxclient_test

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb-client-go/influxclient"
	influxdata "github.com/influxdata/influxdb/v2/models"
	"github.com/stretchr/testify/require"
)

func genLines(num int) []string {
	lines := make([]string, num)
	rand.Seed(321)

	t := time.Now()
	for i := 0; i < len(lines); i++ {
		lines[i] = fmt.Sprintf("test,hostname=host_%d,id=rack_%d,vendor=AWS temperature=%f,disk_free=%f,disk_total=%di,mem_total=%di,mem_free=%di %d",
			i%10, i%100, rand.Float64()*80.0, rand.Float64()*1000.0, (i/10+1)*1000000, (i/100+1)*10000000, rand.Int63(), t.UnixNano())
		if i%10 == 0 {
			t = t.Add(time.Second)
		}
	}
	return lines
}

func genPoints(num int) []influxdata.Point {
	points := make([]influxdata.Point, num)
	rand.Seed(321)

	t := time.Now()
	for i := 0; i < len(points); i++ {
		points[i] = influxdata.MustNewPoint(
			"test",
			influxdata.NewTags(map[string]string{
				"id":       fmt.Sprintf("rack_%v", i%10),
				"vendor":   "AWS",
				"hostname": fmt.Sprintf("host_%v", i%100),
			}),
			map[string]interface{}{
				"temperature": rand.Float64() * 80.0,
				"disk_free":   rand.Float64() * 1000.0,
				"disk_total":  (i/10 + 1) * 1000000,
				"mem_total":   (i/100 + 1) * 10000000,
				"mem_free":    rand.Int63(),
			},
			t)
		if i%10 == 0 {
			t = t.Add(time.Second)
		}
	}
	return points
}

func TestExactWrite(t *testing.T) {
	lines := genLines(10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		var message string
		ok := false
		query := r.URL.Query()
		if ok = r.Method == http.MethodPost; !ok {
			message = "invalid method"
		} else if ok = r.URL.Path == "/api/v2/write"; !ok {
			message = "invalid url"
		} else if ok = len(query) == 3 && query["org"][0] == "o" && query["bucket"][0] == "b" && query["precision"][0] == "ns"; !ok {
			message = "invalid query"
		} else {
			if body, err := ioutil.ReadAll(r.Body); err == nil {
				points := strings.Split(strings.TrimSpace(string(body)), "\n")
				if ok = len(points) == len(lines); !ok {
					fmt.Printf("%v\n%v\n", lines, points)
					message = "unequal points"
				} else {
					eq := true
					for i := range lines {
						if lines[i] != points[i] {
							eq = false
							fmt.Printf("%v\n%v\n", lines[i], points[i])
							message = fmt.Sprintf("invalid %d line", i)
							break
						}
					}
					ok = eq
				}
			} else {
				message = err.Error()
			}

		}
		if ok {
			w.WriteHeader(204)
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(400)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"code": "invalid", "message":"%s"}`, message)))
		}
	}))
	c, err := influxclient.NewClient(influxclient.Params{ServerURL: server.URL, AuthToken: "123"})

	require.NoError(t, err)
	require.NotNil(t, c)

	points, err := influxdata.ParsePointsString(strings.Join(lines, "\n"))
	require.NoError(t, err)

	err = c.WritePoints("o", "b", points)
	require.NoError(t, err)
}
