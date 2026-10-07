package privateclient

import "os"

// NTFS hard links publish without replacing an existing account binding.
func publishBinding(temporary, destination string) error { return os.Link(temporary, destination) }
