package script

import (
	_ "embed"
)

var (
	// mount pod
	//go:embed shell/mountpoint.sh
	SHELL_MOUNT_POINT string
)
