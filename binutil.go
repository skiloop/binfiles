package main

import (
	"fmt"
	"github.com/alecthomas/kong"
	"github.com/skiloop/binfiles/binfile"
	"os"
)

type ListCmd struct {
	Input   string `arg:"" help:"input file name"`
	Offset  int64  `arg:"" optional:"" help:"start document position" default:"0"`
	KeyOnly bool   `short:"k" help:"list key only" default:"false"`
	Limit   int32  `short:"l" help:"limit of list number, 0 means unlimited" default:"0"`
}

type ReadCmd struct {
	Input  string `arg:"" help:"input file name"`
	Offset int64  `arg:"" optional:"" help:"start position" default:"0"`
	Limit  int32  `short:"l" help:"number of documents to read, 0 means read all" default:"1"`
}

type CountCmd struct {
	Input  string `arg:"" help:"input file name"`
	Offset int64  `arg:"" optional:"" help:"start position" default:"0"`
}

type SearchCmd struct {
	Input  string `arg:"" help:"input file name"`
	Key    string `arg:"" help:"key to search, regex supported"`
	Offset int64  `arg:"" optional:"" help:"position to search from" default:"0"`
}
type SeekCmd struct {
	Input  string `arg:"" help:"input file name"`
	Offset int64  `arg:"" optional:"" help:"position to search from" default:"0"`
}

var client struct {
	CompressType string    `short:"z" help:"compression type, options are gzip, bz2 and zip, default is gzip, none if do not want to compress" enum:"gzip,bz2,zip,none" default:"gzip"`
	Verbose      bool      `short:"v" help:"verbose" default:"false"`
	Debug        bool      `short:"d" help:"debug" default:"false"`
	KeySizeLimit int32     `help:"max size of document key in bytes" default:"100"`
	Step         int32     `short:"s" help:"how many docs to skip before next doc is processed, for count command means verbose step" default:"0"`
	List         ListCmd   `cmd:"" aliases:"l,ls" help:"List documents from position."`
	Read         ReadCmd   `cmd:"" aliases:"r,ra" help:"Read documents from position"`
	Count        CountCmd  `cmd:"" aliases:"c" help:"count document file in bin file from position"`
	Search       SearchCmd `cmd:"" aliases:"s" help:"search document by key"`
	Seek         SeekCmd   `cmd:"" aliases:"k,sk" help:"seek for next document from position"`
}

func listDocs() {
	ct, ok := binfile.CompressTypes[client.CompressType]
	if !ok {
		_, _ = fmt.Fprintf(os.Stderr, "unknown compression type %s\n", client.CompressType)
		return
	}

	br := binfile.NewBinReader(client.List.Input, ct)
	if nil != br {
		defer br.Close()
		opt := binfile.ReadOption{
			Offset: client.List.Offset,
			Limit:  client.List.Limit,
			Step:   client.Step,
		}
		br.List(&opt, client.List.KeyOnly)
		return
	}
	_, _ = fmt.Fprintf(os.Stderr, "file not found: %s\n", client.List.Input)
}

func readDocs() {
	ct, ok := binfile.CompressTypes[client.CompressType]
	if !ok {
		_, _ = fmt.Fprintf(os.Stderr, "unknown compression type %s\n", client.CompressType)
		return
	}
	br := binfile.NewBinReader(client.Read.Input, ct)
	if br == nil {
		_, _ = fmt.Fprintf(os.Stderr, "file not found: %s\n", client.Read.Input)
		return
	}
	defer br.Close()
	opt := binfile.ReadOption{
		Offset: client.Read.Offset,
		Limit:  client.Read.Limit,
		Step:   client.Step,
	}
	br.ReadDocs(&opt)
}

func countDocs() {
	ct, ok := binfile.CompressTypes[client.CompressType]
	if !ok {
		_, _ = fmt.Fprintf(os.Stderr, "unknown compression type %s\n", client.CompressType)
		return
	}
	br := binfile.NewBinReader(client.Count.Input, ct)
	if br == nil {
		_, _ = fmt.Fprintf(os.Stderr, "file not found: %s\n", client.Count.Input)
		return
	}
	defer br.Close()
	var step uint32
	if client.Step < 0 {
		step = 0
	} else {
		step = uint32(client.Step)
	}
	count, err := br.Count(client.Count.Offset, step)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "file read error: %v\n", err)
	} else {
		fmt.Printf("%d\n", count)
	}
}
func searchDocs() {
	ct, ok := binfile.CompressTypes[client.CompressType]
	if !ok {
		_, _ = fmt.Fprintf(os.Stderr, "unknown compression type %s\n", client.CompressType)
		return
	}
	br := binfile.NewBinReader(client.Search.Input, ct)
	if br == nil {
		_, _ = fmt.Fprintf(os.Stderr, "file not found: %s\n", client.Search.Input)
		return
	}
	defer br.Close()

	pos := br.Search(client.Search.Key, client.Search.Offset)
	if pos < 0 {
		_, _ = fmt.Fprintf(os.Stderr, "document with key %s not found", client.Search.Key)
		return
	}
	doc, err := br.ReadAt(pos, true)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "document found at %d but read error: %v\n", pos, err)
	}
	fmt.Printf("%10d\t%s\n", pos, doc.Content)
}

func seekDoc() {
	ct, ok := binfile.CompressTypes[client.CompressType]
	if !ok {
		_, _ = fmt.Fprintf(os.Stderr, "unknown compression type %s\n", client.CompressType)
		return
	}
	br := binfile.NewBinReader(client.Seek.Input, ct)
	if br == nil {
		_, _ = fmt.Fprintf(os.Stderr, "file not found: %s\n", client.Search.Input)
		return
	}
	defer br.Close()
	next, doc := br.Next(client.Seek.Offset)
	if next < 0 {
		fmt.Printf("no document found")
		return
	}
	if doc == nil {
		fmt.Printf("position %d found nil document\n", next)
		return
	}
	fmt.Printf("%10d\t%s\n", next, doc.Content)
}

func main() {
	ctx := kong.Parse(&client)
	binfile.Verbose = client.Verbose
	binfile.Debug = client.Debug
	binfile.KeySizeLimit = client.KeySizeLimit
	switch ctx.Command() {
	case "list <input>", "list <input> <offset>":
		listDocs()
		break
	case "read <input>", "read <input> <offset>":
		readDocs()
		break
	case "count <input>", "count <input> <offset>":
		countDocs()
		break
	case "seek <input>", "seek <input> <offset>":
		seekDoc()
		break
	case "search <input> <key>", "search <input> <key> <offset>":
		searchDocs()
		break
	default:
		_, _ = fmt.Fprintf(os.Stderr, "unknown command: %s\n", ctx.Command())
		_ = ctx.PrintUsage(true)
	}
}
