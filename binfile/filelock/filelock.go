package filelock

import "os"

func Lock(f os.File) error {
	return lock(f, writeLock)
}

//func RLock(f os.File) error {
//	return lock(f, readLock)
//}

func UnLock(f os.File) error {
	return unlock(f)
}
