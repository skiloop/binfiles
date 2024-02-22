package version

import "fmt"

var (
	Version    = "dev"
	CommitHash = "n/a"
	BuildTime  = "n/a"
)

func BuildVersion() string {
	return fmt.Sprintf("%s-%s (%s)", Version, CommitHash, BuildTime)
}
