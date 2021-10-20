// Copyright 2021 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package influxclient

import (
	"runtime"
)

// Version defines current version
const Version = "3.0.0alpha1"

// userAgent header value
const userAgent = "influxdb-client-go/" + Version + " (" + runtime.GOOS + "; " + runtime.GOARCH + ")"
