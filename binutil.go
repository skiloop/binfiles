package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/alecthomas/kong"
	"github.com/skiloop/binfiles/binfile"
	"github.com/skiloop/binfiles/version"
	"os"
	"runtime"
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
	Input       string `arg:"" help:"input file name"`
	Offset      int64  `arg:"" optional:"" help:"start position" default:"0"`
	WorkerCount int    `short:"w" help:"number of workers, when 0 or negative number of system processors will be used" default:"0"`
}

type SearchCmd struct {
	Input  string `arg:"" help:"input file name"`
	Key    string `arg:"" help:"key to search, regex supported"`
	Pretty bool   `short:"p" help:"value is a json, and pretty output when found" default:"false"`
	Offset int64  `arg:"" optional:"" help:"position to search from" default:"0"`
}

type SeekCmd struct {
	Input  string `arg:"" help:"input file name"`
	Offset int64  `arg:"" optional:"" help:"position to search from" default:"0"`
}

type PackageCmd struct {
	Output            string `arg:"" help:"output bin file path"`
	Path              string `arg:"" help:"input path where source files are"`
	InputCompressType string `short:"c" help:"input file compression type" enum:"gzip,bz2,xz,br,brotli,lz4,none" default:"none"`
	Pattern           string `short:"p" help:"source file pattern, the matched will be packaged, all files package if empty" default:""`
	WorkerCount       int    `short:"w" help:"number of workers, when 0 or negative number of system processors will be used" default:"0"`
}
type VersionCmd struct {
}

var client struct {
	CompressType string            `short:"z" help:"compression type, none if do not want to compress" enum:"gzip,xz,br,lz4,bz2,none" default:"gzip"`
	Verbose      bool              `short:"v" help:"verbose" default:"false"`
	Debug        bool              `short:"d" help:"debug" default:"false"`
	KeySizeLimit int32             `help:"max size of document key in bytes" default:"1000"`
	Step         int32             `short:"s" help:"how many docs to skip before next doc is processed, for count command means verbose step" default:"0"`
	Version      VersionCmd        `cmd:"" help:"print version" default:"withargs"`
	List         ListCmd           `cmd:"" aliases:"l,ls" help:"List documents from position."`
	Read         ReadCmd           `cmd:"" aliases:"r,ra" help:"Read documents from position"`
	Count        CountCmd          `cmd:"" aliases:"c" help:"count document file in bin file from position"`
	Search       SearchCmd         `cmd:"" aliases:"s" help:"search document by key"`
	Seek         SeekCmd           `cmd:"" aliases:"k,sk" help:"seek for next document from position"`
	Package      PackageCmd        `cmd:"" aliases:"p" help:"package files into bin file"`
	Repack       binfile.RepackCmd `cmd:"" aliases:"a" help:"repack bin file into other bin format"`
}

func newReader(filename string, compress string) binfile.BinReader {
	ct, ok := binfile.CompressTypes[compress]
	if !ok {
		_, _ = fmt.Fprintf(os.Stderr, "unknown compression type %s\n", client.CompressType)
		return nil
	}
	return binfile.NewBinReader(filename, ct)
}

func newWriter(filename string, compress string) binfile.BinWriter {
	ct, ok := binfile.CompressTypes[compress]
	if !ok {
		_, _ = fmt.Fprintf(os.Stderr, "unknown compression type %s\n", client.CompressType)
		return nil
	}
	return binfile.NewBinWriter(filename, ct)
}

func listDocs(br binfile.BinReader) {
	opt := binfile.ReadOption{
		Offset: client.List.Offset,
		Limit:  client.List.Limit,
		Step:   client.Step,
	}
	br.List(&opt, client.List.KeyOnly)
}

func readDocs(br binfile.BinReader) {
	defer br.Close()
	opt := binfile.ReadOption{
		Offset: client.Read.Offset,
		Limit:  client.Read.Limit,
		Step:   client.Step,
	}
	br.ReadDocs(&opt)
}

func countDocs(br binfile.BinReader) {

	var step uint32
	if client.Step < 0 {
		step = 0
	} else {
		step = uint32(client.Step)
	}
	count := br.Count(client.Count.Offset, client.Count.WorkerCount, step)
	if count >= 0 {
		fmt.Printf("%d\n", count)
	}
}

func JsonPrettify(content []byte) (error, *bytes.Buffer) {
	var prettyJSON bytes.Buffer
	err := json.Indent(&prettyJSON, content, "", "\t")
	if err != nil {
		return err, nil
	}
	return nil, &prettyJSON
}

func searchDocs(br binfile.BinReader) {
	opt := binfile.SearchOption{Key: client.Search.Key, Offset: client.Search.Offset, Number: int(client.Step)}
	if binfile.Verbose {
		fmt.Printf("Key   : %s\n", opt.Key)
		fmt.Printf("Offset: %d\n", opt.Offset)
		fmt.Printf("Number: %d\n", opt.Number)
	}
	pos := br.Search(opt)
	if pos < 0 {
		_, _ = fmt.Fprintf(os.Stderr, "document with key %s not found", client.Search.Key)
		return
	}
	doc, err := br.ReadAt(pos, true)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "document found at %d but read error: %v\n", pos, err)
		return
	}
	if !client.Search.Pretty {
		fmt.Printf("%10d\t%s\n", pos, doc.Content)
		return
	}
	err, buf := JsonPrettify([]byte(doc.Content))
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "json prettify failed: %v\n", err)
		return
	}
	_, _ = buf.WriteTo(os.Stdout)
}

func seekDoc(br binfile.BinReader) {
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

func packageDocs(bw binfile.BinWriter) {
	ct, ok := binfile.CompressTypes[client.Package.InputCompressType]
	if !ok {
		_, _ = fmt.Fprintf(os.Stderr, "unknown compression type %s\n", client.CompressType)
		return
	}

	opt := &binfile.PackageOption{
		Path:          client.Package.Path,
		Pattern:       client.Package.Pattern,
		InputCompress: ct,
		WorkerCount:   client.Package.WorkerCount,
	}
	if opt.WorkerCount <= 0 {
		opt.WorkerCount = runtime.NumCPU()
	}
	err := binfile.Package(opt, bw)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "package error: %v\n", err)
	}
}

func execReadCmd(filename string, worker func(reader binfile.BinReader)) {
	br := newReader(filename, client.CompressType)
	if br == nil {
		_, _ = fmt.Fprintf(os.Stderr, "file not found: %s\n", filename)
		return
	}
	defer br.Close()
	worker(br)
}
func execWriteCmd(filename string, worker func(reader binfile.BinWriter)) {
	bw := newWriter(filename, client.CompressType)
	if bw == nil {
		_, _ = fmt.Fprintf(os.Stderr, "file not found: %s\n", filename)
		return
	}
	defer bw.Close()
	worker(bw)
}

func main() {
	ctx := kong.Parse(&client)
	binfile.Debug = client.Debug
	binfile.Verbose = client.Verbose || client.Debug

	binfile.KeySizeLimit = client.KeySizeLimit
	switch ctx.Command() {
	case "list <input>", "list <input> <offset>":
		execReadCmd(client.List.Input, listDocs)
		break
	case "read <input>", "read <input> <offset>":
		execReadCmd(client.Read.Input, readDocs)
		break
	case "count <input>", "count <input> <offset>":
		execReadCmd(client.Count.Input, countDocs)
		break
	case "seek <input>", "seek <input> <offset>":
		execReadCmd(client.Seek.Input, seekDoc)
		break
	case "search <input> <key>", "search <input> <key> <offset>":
		execReadCmd(client.Search.Input, searchDocs)
		break
	case "package <output> <path>":
		execWriteCmd(client.Package.Output, packageDocs)
		break
	case "repack <source> <target>":
		err := binfile.Repack(client.Repack)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "repack error: %v", err)
		}
		break
	default:
		fmt.Println(version.BuildVersion())
	}
}
