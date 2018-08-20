package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"github.com/skiloop/gbinutil/binfile"
)

var (
	command      = flag.String("c", "c", "command: count[c], readat[r,ra]")
	input        = flag.String("i", "", "input filename")
	compressType = flag.String("t", "gzip", "value compression type")
	output       = flag.String("o", "", "output filename, empty for stdin")
	position     = flag.Int64("p", 0, "position of input file")
)

func countBin() {
	var writer io.Writer
	var err error
	if *output != "" {
		writer, err = os.Open(*output)
		if err != nil {
			os.Stderr.WriteString("output file open error")
			return
		}
	}
	ct, ok := binfile.CompressTypes[*compressType]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown compression type %s\n", *compressType)
		return
	}

	br := binfile.NewBinReader(*input, ct)
	if br != nil {
		br.Count(*position, writer)
		return
	}
	fmt.Fprintf(os.Stderr, "file not found: %s", *input)
}

func readBinAt() {
	ct, ok := binfile.CompressTypes[*compressType]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown compression type %s\n", *compressType)
		return
	}
	br := binfile.NewBinReader(*input, ct)
	if br == nil {
		fmt.Fprintf(os.Stderr, "file not found: %s", *input)
		return
	}
	doc, err := br.ReadAt(*position, true)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println(doc.Content)
		fmt.Println(doc.Key)
	}
}

func main() {
	flag.Parse()

	cmd := flag.Arg(0)
	if cmd == "" {
		cmd = *command
	}
	switch cmd {
	case "count":
	case "c":
		countBin()
		break
	case "readat":
	case "ra":
	case "r":
		readBinAt()
		break
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		flag.Usage()
	}

}
