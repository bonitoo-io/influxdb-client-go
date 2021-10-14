// Copyright 2021 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package influxclient

import (
	"fmt"
	"runtime"
)

// Version defines current version
const Version = "3.0.0alpha1"

// UserAgent header value
var UserAgent string

func init() {
	UserAgent = fmt.Sprintf("influxdb-client-go/%s  (%s; %s)", Version, runtime.GOOS, runtime.GOARCH)
}
