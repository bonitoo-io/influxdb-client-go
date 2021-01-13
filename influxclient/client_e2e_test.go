// +build e2e

package influxclient_test

import (
	"os"
	"testing"

	"github.com/influxdata/influxdb-client-go/influxclient"
	"github.com/stretchr/testify/require"
)

var (
	authToken string
	serverURL string
)

func init() {
	authToken = getEnvValue("INFLUXDB2_TOKEN", "my-token")
	serverURL = getEnvValue("INFLUXDB2_URL", "http://localhost:8086")
}

func getEnvValue(key, defVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	} else {
		return defVal
	}
}

func TestWrite(t *testing.T) {
	points := genPoints(5)
	c, err := influxclient.NewClient(influxclient.Params{ServerURL: serverURL, AuthToken: authToken})
	require.NoError(t, err)
	require.NotNil(t, c)
	err = c.WritePoints("my-org", "my-bucket", points)
	require.NoError(t, err)
}
