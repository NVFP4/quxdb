package version

import (
	"fmt"
	"runtime"
)

var (
	AppName  = "quxdb"
	Version  = "0.1.0"
	Revision = "HEAD"
)

var (
	// Short is the short version string.
	Short = fmt.Sprintf("%s (%s)", Version, Revision)

	// ShortWithApp is the short version string with the application name.
	ShortWithApp = fmt.Sprintf("%s %s", AppName, Short)

	// Detailed is the detailed version string.
	Detailed = fmt.Sprintf("%s (%s; %s; %s/%s)", Version, Revision, runtime.Version(), runtime.GOOS, runtime.GOARCH)

	// DetailedWithApp is the detailed version string with the application name.
	DetailedWithApp = fmt.Sprintf("%s %s", AppName, Detailed)
)
