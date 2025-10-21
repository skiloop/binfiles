package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"runtime"

	"github.com/alecthomas/kong"

	"github.com/skiloop/binfiles/binfile"
	"github.com/skiloop/binfiles/version"
)

type ListCmd struct {
	KeyOnly   bool   `short:"k" help:"list key only" default:"false"`
	SkipError bool   `help:"skip error docs and continue reading" default:"false"`
	Limit     int32  `short:"l" help:"limit of list number, 0 means unlimited" default:"0"`
	Offset    int64  `arg:"" optional:"" help:"start document position" default:"0"`
	Input     string `arg:"" help:"input file name"`
}

type ReadCmd struct {
	SkipError bool   `help:"skip error docs and continue reading" default:"false"`
	Limit     int32  `short:"l" help:"number of documents to read, 0 means read all" default:"1"`
	Offset    int64  `arg:"" optional:"" help:"start position" default:"0"`
	Input     string `arg:"" help:"input file name"`
	Output    string `short:"o" help:"output file name, empty to std output" default:""`
	OutType   string `short:"c" help:"output compression type, only works when output not empty" enum:"gzip,bz2,xz,br,brotli,lz4,none" default:"none"`
}

type CountCmd struct {
	KeyOnly     bool   `short:"k" help:"count without decode content" default:"false"`
	SkipError   bool   `help:"skip error docs and continue reading" default:"false"`
	WorkerCount int    `short:"w" help:"number of workers, when 0 or negative number of system processors will be used" default:"0"`
	Offset      int64  `arg:"" optional:"" help:"start position" default:"0"`
	Input       string `arg:"" help:"input file name"`
}

type SearchCmd struct {
	NoSkipError bool   `help:"continue searching when encounter invalid doc" default:"false"`
	Pretty      bool   `short:"p" help:"value is a json, and pretty output when found" default:"false"`
	Offset      int64  `arg:"" optional:"" help:"position to search from" default:"0"`
	Input       string `arg:"" help:"input file name"`
	Key         string `arg:"" help:"key to search, regex supported"`
}

type SeekCmd struct {
	Input  string `arg:"" help:"input file name"`
	Offset int64  `arg:"" optional:"" help:"position to search from" default:"0"`
}

type PackageCmd struct {
	WorkerCount       int    `short:"w" help:"number of workers, when 0 or negative number of system processors will be used" default:"0"`
	Output            string `arg:"" help:"output bin file path"`
	Path              string `arg:"" help:"input path or tar file path"`
	InputCompressType string `short:"c" help:"input file compression type" enum:"gzip,bz2,xz,br,brotli,lz4,none" default:"none"`
	TarCompressType   string `short:"t" help:"tar file compression type" enum:"gzip,bz2,xz,br,brotli,lz4,none" default:"gzip"`
	Pattern           string `short:"p" help:"source file pattern, the matched will be packaged, all files package if empty" default:""`
}
type VersionCmd struct {
}

type ListTarCmd struct {
	Limit  int32  `short:"l" help:"limit of list number, 0 means unlimited" default:"0"`
	Input  string `arg:"" help:"input file name"`
	Format string `short:"f" help:"compression format" enum:"auto,gzip,xz,bzip2,zlib,none" default:"auto"`
}

var client struct {
	Verbose      bool              `short:"v" help:"verbose" default:"false"`
	Debug        bool              `short:"d" help:"debug" default:"false"`
	KeySizeLimit int32             `short:"L" help:"max size of document key in bytes" default:"1000"`
	Step         int32             `short:"s" help:"how many docs to skip before next doc is processed, for count command means verbose step" default:"0"`
	LogLevel     string            `help:"log level" enum:"debug,info,warn,error,fatal" default:"info"`
	CompressType string            `short:"z" help:"compression type, none if do not want to compress" enum:"gzip,xz,br,lz4,bz2,none" default:"gzip"`
	Version      VersionCmd        `cmd:"" help:"print version" default:"withargs"`
	List         ListCmd           `cmd:"" aliases:"l,ls" help:"List documents from position."`
	Read         ReadCmd           `cmd:"" aliases:"r,ra" help:"Read documents from position"`
	Count        CountCmd          `cmd:"" aliases:"c" help:"count document file in bin file from position"`
	Search       SearchCmd         `cmd:"" aliases:"s" help:"search document by key"`
	Seek         SeekCmd           `cmd:"" aliases:"k,sk" help:"seek for next document from position"`
	Package      PackageCmd        `cmd:"" aliases:"p" help:"package files into bin file"`
	Repack       binfile.RepackCmd `cmd:"" aliases:"a" help:"repack bin file into other bin format"`
	ListTar      ListTarCmd        `cmd:"" aliases:"t" help:"list tar archive"`
}

func newReader(filename string, compress string) binfile.BinReader {
	ct, ok := binfile.CompressTypes[compress]
	if !ok {
		binfile.LogError("unknown compression type %s\n", client.CompressType)
		return nil
	}
	r, err := binfile.NewBinReader(filename, ct)
	if err != nil {
		binfile.LogError("fail to init bin reader: %v\n", err)
		return nil
	}
	return r
}

func newWriter(filename string, compress string) binfile.BinWriter {
	ct, ok := binfile.CompressTypes[compress]
	if !ok {
		binfile.LogError("unknown compression type %s\n", client.CompressType)
		return nil
	}
	return binfile.NewBinWriter(filename, ct)
}

func listDocs(br binfile.BinReader) {
	opt := binfile.ReadOption{
		Offset:    client.List.Offset,
		Limit:     client.List.Limit,
		Step:      client.Step,
		SkipError: client.List.SkipError,
	}
	br.List(&opt, client.List.KeyOnly)
}

func readDocs(br binfile.BinReader) {
	defer br.Close()
	opt := binfile.ReadOption{
		Offset:      client.Read.Offset,
		Limit:       client.Read.Limit,
		Step:        client.Step,
		OutCompress: binfile.CompressTypes[client.Read.OutType],
		Output:      client.Read.Output,
		SkipError:   client.Read.SkipError,
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
	count := br.Count(client.Count.Offset, client.Count.WorkerCount, step, client.Count.SkipError)
	if count >= 0 {
		fmt.Printf("%d\n", count)
	}
}

func JsonPrettify(content []byte) (*bytes.Buffer, error) {
	var prettyJSON bytes.Buffer
	err := json.Indent(&prettyJSON, content, "", "\t")
	if err != nil {
		return nil, err
	}
	return &prettyJSON, nil
}

func searchDocs(br binfile.BinReader) {
	opt := binfile.SearchOption{
		Key:       client.Search.Key,
		Offset:    client.Search.Offset,
		Number:    int(client.Step),
		SkipError: !client.Search.NoSkipError,
	}
	if binfile.Verbose {
		binfile.LogInfo("Key   : %s\n", opt.Key)
		binfile.LogInfo("Offset: %d\n", opt.Offset)
		binfile.LogInfo("Number: %d\n", opt.Number)
	}
	pos := br.Search(opt)
	if pos < 0 {
		binfile.LogError("document with key %s not found", client.Search.Key)
		return
	}
	doc, err := br.Read(pos, true)
	if err != nil {
		binfile.LogError("document found at %d but read error: %v\n", pos, err)
		return
	}
	if !client.Search.Pretty {
		fmt.Printf("%10d\t%s\n", pos, doc.Content)
		return
	}
	buf, err := JsonPrettify([]byte(doc.Content))
	if err != nil {
		binfile.LogError("json prettify failed: %v\n", err)
		return
	}
	_, _ = buf.WriteTo(os.Stdout)
}

func seekDoc(br binfile.BinReader) {
	next, doc := br.Next(&binfile.SeekOption{
		Offset:  client.Seek.Offset,
		Pattern: "",
		KeySize: int(binfile.KeySizeLimit),
		DocSize: -1,
		End:     -1,
	})
	if next < 0 {
		binfile.LogInfo("no document found")
		return
	}
	if doc == nil {
		binfile.LogInfo("position %d found nil document\n", next)
		return
	}
	fmt.Printf("%10d\t%s\n", next, doc.Content)
}

func packageDocs(bw binfile.BinWriter) {
	ct, ok := binfile.CompressTypes[client.Package.InputCompressType]
	if !ok {
		binfile.LogError("unknown compression type %s\n", client.CompressType)
		return
	}

	opt := &binfile.PackageOption{
		Path:          client.Package.Path,
		Pattern:       client.Package.Pattern,
		InputCompress: ct,
		TarCompress:   client.Package.TarCompressType,
		WorkerCount:   client.Package.WorkerCount,
	}
	if opt.WorkerCount <= 0 {
		opt.WorkerCount = runtime.NumCPU()
	}
	err := binfile.Package(opt, bw)
	if err != nil {
		binfile.LogError("package error: %v\n", err)
	}
}

func execReadCmd(filename string, worker func(reader binfile.BinReader)) {
	br := newReader(filename, client.CompressType)
	if br == nil {
		binfile.LogError("file not found: %s\n", filename)
		return
	}
	defer br.Close()
	worker(br)
}
func execWriteCmd(filename string, worker func(reader binfile.BinWriter)) {
	bw := newWriter(filename, client.CompressType)
	if bw == nil {
		binfile.LogError("file not found: %s\n", filename)
		return
	}
	defer func() {
		_ = bw.Close()
	}()
	worker(bw)
}

func main() {
	ctx := kong.Parse(&client)
	binfile.Debug = client.Debug
	binfile.Verbose = client.Verbose || client.Debug

	binfile.SetGlobalLogLevel(binfile.LogLevelToEnum(client.LogLevel))

	binfile.KeySizeLimit = client.KeySizeLimit
	switch ctx.Command() {
	case "list <input>", "list <input> <offset>":
		execReadCmd(client.List.Input, listDocs)
	case "read <input>", "read <input> <offset>":
		execReadCmd(client.Read.Input, readDocs)
	case "count <input>", "count <input> <offset>":
		execReadCmd(client.Count.Input, countDocs)
	case "seek <input>", "seek <input> <offset>":
		execReadCmd(client.Seek.Input, seekDoc)
	case "search <input> <key>", "search <input> <key> <offset>":
		execReadCmd(client.Search.Input, searchDocs)
	case "package <output> <path>":
		execWriteCmd(client.Package.Output, packageDocs)
	case "repack <source> <target>":
		err := binfile.Repack(client.Repack)
		if err != nil {
			binfile.LogError("repack error: %v", err)
		}
	case "list-tar <input>":
		binfile.ListTar(client.ListTar.Input, binfile.CompressionFormat(client.ListTar.Format), int(client.ListTar.Limit))
	default:
		binfile.LogInfo("%s\n", version.BuildVersion())
	}
}
