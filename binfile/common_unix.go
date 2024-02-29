//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd

package binfile

import (
	"os"
)

const writerFileFlag = os.O_CREATE | os.O_APPEND | os.O_WRONLY
