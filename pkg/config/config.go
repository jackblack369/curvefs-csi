package config

import (
	"time"

	corev1 "k8s.io/api/core/v1"
)

var (
	DriverName               = "csi.dingofs.com"
	NodeName                 = ""
	Namespace                = ""
	PodName                  = ""
	HostIp                   = ""
	KubeletPort              = ""
	ReconcileTimeout         = 5 * time.Minute
	ReconcilerInterval       = 5
	SecretReconcilerInterval = 1 * time.Hour

	CSIPod = corev1.Pod{}
)

const (
	PodTypeKey           = "app.kubernetes.io/name"
	PodTypeValue         = "dingofs-mount"
	PodUniqueIdLabelKey  = "volume-id"
	PodJuiceHashLabelKey = "dingofs-hash"

	DingoFSID = "dingfs-fsid"
	UniqueId  = "dingofs-uniqueid"

	DeleteDelayTimeKey = "dingofs-delete-delay"
	DeleteDelayAtKey   = "dingofs-delete-at"

	PodMountBase = "/dfs"

	PodInfoName         = "csi.storage.k8s.io/pod.name"
	PodInfoNamespace    = "csi.storage.k8s.io/pod.namespace"
	DefaultCheckTimeout = 2 * time.Second

	MountContainerName = "dfs-mount"
	MountPointPath     = "/var/lib/dingofs/volume"

	// default value
	DefaultMountPodCpuLimit   = "2000m"
	DefaultMountPodMemLimit   = "5Gi"
	DefaultMountPodCpuRequest = "1000m"
	DefaultMountPodMemRequest = "1Gi"

	DefaultMountImage = "dingodatabase/dingofs-csi:latest" // mount pod image, override by ENV

	// config in pv
	MountPodCpuLimitKey    = "dingofs/mount-cpu-limit"
	MountPodMemLimitKey    = "dingofs/mount-memory-limit"
	MountPodCpuRequestKey  = "dingofs/mount-cpu-request"
	MountPodMemRequestKey  = "dingofs/mount-memory-request"
	MountPodLabelKey       = "dingofs/mount-labels"
	MountPodAnnotationKey  = "dingofs/mount-annotations"
	MountPodServiceAccount = "dingofs/mount-service-account"
	MountPodImageKey       = "dingofs/mount-image"
)
