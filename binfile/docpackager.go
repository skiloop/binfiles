package binfile

import "sync"

type packageHeader struct {
	totalCount   int64
	docCount     int
	compressType int
	lock         sync.RWMutex
}

//type docPackage struct {
//}

type DocPackageWriter struct {
	binWriter
	header *packageHeader
}
