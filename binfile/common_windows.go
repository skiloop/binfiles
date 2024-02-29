//go:build windows

package binfile

import (
	"os"
)

const writerFileFlag = os.O_CREATE | os.O_APPEND
