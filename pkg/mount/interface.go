package mount

import (
	"context"

	k8sMount "k8s.io/utils/mount"

	config "github.com/jackblack369/dingofs-csi/pkg/config"
)

type MntInterface interface {
	k8sMount.Interface
	DMount(ctx context.Context, appInfo *config.AppInfo, dfsSetting *config.DfsSetting) error
	// DCreateVolume(ctx context.Context, jfsSetting *jfsConfig.JfsSetting) error
	// DDeleteVolume(ctx context.Context, jfsSetting *jfsConfig.JfsSetting) error
	// GetMountRef(ctx context.Context, target, podName string) (int, error) // podName is only used by podMount
	// UmountTarget(ctx context.Context, target, podName string) error       // podName is only used by podMount
	// DUmount(ctx context.Context, target, podName string) error            // podName is only used by podMount
	// AddRefOfMount(ctx context.Context, target string, podName string) error
	// CleanCache(ctx context.Context, image string, id string, volumeId string, cacheDirs []string) error
}
