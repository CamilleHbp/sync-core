//go:build unix

package privateclient

import (
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
)

// Android SELinux forbids hard links in app data directories. Serialize atomic
// rename under an advisory lock instead; the OS releases the lock after a crash.
func publishBinding(temporary, destination string) error {
	lock, err := os.OpenFile(filepath.Join(filepath.Dir(destination), ".connection.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX); err != nil {
		return err
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	if _, err = os.Lstat(destination); err == nil {
		return os.ErrExist
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Rename(temporary, destination)
}
