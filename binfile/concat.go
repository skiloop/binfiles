package binfile

//func FilesConcat(dst string, files ...string) error {
//if len(files) <= 1 {
//	return nil
//}
//target, err := os.OpenFile(dst, os.O_CREATE|os.O_APPEND, 0644)
//if err != nil {
//	return err
//}
//offset, _ := target.Seek(0, 2)
//for _, file := range files {
//	src, err := os.OpenFile(file, os.O_RDONLY, 0644)
//	if err != nil {
//		LogError("read file %s error: %v\n", file, err)
//		continue
//	}
//
//}
//return nil
//}
