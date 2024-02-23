package version

import "fmt"

var (
	Version    = "dev"
	CommitHash = "n/a"
	BuildTime  = "n/a"
)

func BuildVersion() string {
	return fmt.Sprintf("Version    : %s\nCommit Hash: %s\nBuilt Time : %s", Version, CommitHash, BuildTime)
}
