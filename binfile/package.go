package binfile

type RepackCmd struct {
	Source              string `arg:"" help:"source bin file name"`
	Target              string `arg:"" help:"target bin file name"`
	Workers             int    `short:"w" help:"number of workers" default:"1"`
	Split               int    `short:"s" help:"split target into small parts if positive every specified number of docs, 0 means not to split" default:"0"`
	TargetCompressType  string `short:"t" help:"compression type for docs in target file, none for no compression" enum:"gzip,bz2,none" default:"none"`
	PackageCompressType string `short:"c" help:"compression type after whole target completed, none for no compression" enum:"gzip,bz2,none" default:"none"`
}

// Repack bin file
func Repack(opt *RepackCmd) error {
	return nil
}
