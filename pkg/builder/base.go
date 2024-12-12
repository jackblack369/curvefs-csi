/*
Copyright 2022 Juicedata Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package builder

import (
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/jackblack369/dingofs-csi/pkg/config"
	"github.com/jackblack369/dingofs-csi/pkg/util"
)

const (
	JfsDirName          = "jfs-dir"
	UpdateDBDirName     = "updatedb"
	UpdateDBCfgFile     = "/etc/updatedb.conf"
	JfsFuseFdPathName   = "jfs-fuse-fd"
	JfsFuseFsPathInPod  = "/tmp"
	JfsFuseFsPathInHost = "/var/run/juicefs-csi"
	JfsCommEnv          = "JFS_SUPER_COMM"
)

type BaseBuilder struct {
	dfsSetting *config.DfsSetting
	capacity   int64
}

// genPodTemplate generates a pod template from csi pod
func (r *BaseBuilder) genPodTemplate(baseCnGen func() corev1.Container) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.dfsSetting.Attr.Namespace,
			Labels: map[string]string{
				config.PodTypeKey:          config.PodTypeValue,
				config.PodUniqueIdLabelKey: r.dfsSetting.UniqueId,
			},
			Annotations: make(map[string]string),
		},
		Spec: corev1.PodSpec{
			Containers:         []corev1.Container{baseCnGen()},
			NodeName:           config.NodeName,
			HostNetwork:        r.dfsSetting.Attr.HostNetwork,
			HostAliases:        r.dfsSetting.Attr.HostAliases,
			HostPID:            r.dfsSetting.Attr.HostPID,
			HostIPC:            r.dfsSetting.Attr.HostIPC,
			DNSConfig:          r.dfsSetting.Attr.DNSConfig,
			DNSPolicy:          r.dfsSetting.Attr.DNSPolicy,
			ServiceAccountName: r.dfsSetting.Attr.ServiceAccountName,
			ImagePullSecrets:   r.dfsSetting.Attr.ImagePullSecrets,
			PreemptionPolicy:   r.dfsSetting.Attr.PreemptionPolicy,
			Tolerations:        r.dfsSetting.Attr.Tolerations,
		},
	}
}

// genCommonJuicePod generates a pod with common settings
func (r *BaseBuilder) genCommonJuicePod(cnGen func() corev1.Container) *corev1.Pod {
	// gen again to update the mount pod spec
	if err := config.GenPodAttrWithCfg(r.dfsSetting, nil); err != nil {
		klog.Error(err, "genCommonJuicePod gen pod attr failed, mount pod may not be the expected config")
	}
	pod := r.genPodTemplate(cnGen)
	// labels & annotations
	pod.ObjectMeta.Labels, pod.ObjectMeta.Annotations = r._genMetadata()
	pod.Spec.ServiceAccountName = r.dfsSetting.Attr.ServiceAccountName
	pod.Spec.PriorityClassName = config.DFSMountPriorityName
	pod.Spec.RestartPolicy = corev1.RestartPolicyAlways
	pod.Spec.Hostname = r.dfsSetting.VolumeId
	gracePeriod := int64(10)
	if r.dfsSetting.Attr.TerminationGracePeriodSeconds != nil {
		gracePeriod = *r.dfsSetting.Attr.TerminationGracePeriodSeconds
	}
	pod.Spec.TerminationGracePeriodSeconds = &gracePeriod
	controllerutil.AddFinalizer(pod, config.Finalizer)

	volumes, volumeMounts := r._genJuiceVolumes()
	pod.Spec.Volumes = volumes
	pod.Spec.Containers[0].VolumeMounts = volumeMounts
	pod.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{
		SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: r.dfsSetting.SecretName,
			},
		},
	}}
	pod.Spec.Containers[0].Env = r.dfsSetting.Attr.Env
	pod.Spec.Containers[0].Resources = r.dfsSetting.Attr.Resources
	// if image support passFd from csi, do not set umount preStop
	// if r.dfsSetting.Attr.Lifecycle == nil {
	// 	if !util.SupportFusePass(pod.Spec.Containers[0].Image) || config.Webhook {
	// 		pod.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{
	// 			PreStop: &corev1.LifecycleHandler{
	// 				Exec: &corev1.ExecAction{Command: []string{"sh", "-c", "+e", fmt.Sprintf(
	// 					"umount %s -l; rmdir %s; exit 0", r.dfsSetting.MountPath, r.dfsSetting.MountPath)}},
	// 			},
	// 		}
	// 	}
	// } else {
	// 	pod.Spec.Containers[0].Lifecycle = r.dfsSetting.Attr.Lifecycle
	// }
	if r.dfsSetting.Attr.Lifecycle != nil {
		pod.Spec.Containers[0].Lifecycle = r.dfsSetting.Attr.Lifecycle
	}

	pod.Spec.Containers[0].StartupProbe = r.dfsSetting.Attr.StartupProbe
	pod.Spec.Containers[0].LivenessProbe = r.dfsSetting.Attr.LivenessProbe
	pod.Spec.Containers[0].ReadinessProbe = r.dfsSetting.Attr.ReadinessProbe

	// if r.dfsSetting.Attr.HostNetwork || !r.dfsSetting.IsCe {
	// 	// When using hostNetwork, the MountPod will use a random port for metrics.
	// 	// Before inducing any auxiliary method to detect that random port, the
	// 	// best way is to avoid announcing any port about that.
	// 	// Enterprise edition does not have metrics port.
	// 	pod.Spec.Containers[0].Ports = []corev1.ContainerPort{}
	// } else {
	// 	pod.Spec.Containers[0].Ports = []corev1.ContainerPort{
	// 		{Name: "metrics", ContainerPort: r.genMetricsPort()},
	// 	}
	// }
	return pod
}

// genMountCommand generates mount command
func (r *BaseBuilder) genMountCommand() string {
	cmd := ""
	options := r.dfsSetting.Options

	klog.Info("ceMount", "source", util.StripPasswd(r.dfsSetting.Source), "mountPath", r.dfsSetting.MountPath)
	mountArgs := []string{"exec", config.CliPath, "${metaurl}", util.EscapeBashStr(r.dfsSetting.MountPath)}
	if !util.ContainsPrefix(options, "metrics=") {
		if r.dfsSetting.Attr.HostNetwork {
			// Pick up a random (useable) port for hostNetwork MountPods.
			options = append(options, "metrics=0.0.0.0:0")
		} else {
			options = append(options, "metrics=0.0.0.0:9567")
		}
	}
	mountArgs = append(mountArgs, "-o", util.EscapeBashStr(strings.Join(options, ",")))
	cmd = strings.Join(mountArgs, " ")

	return util.QuoteForShell(cmd)
}

// genInitCommand generates init command
func (r *BaseBuilder) genInitCommand() string {
	formatCmd := r.dfsSetting.FormatCmd
	if r.dfsSetting.EncryptRsaKey != "" {
		formatCmd = formatCmd + " --encrypt-rsa-key=/root/.rsa/rsa-key.pem"
	}
	if r.dfsSetting.InitConfig != "" {
		confPath := filepath.Join(config.ROConfPath, r.dfsSetting.Name+".conf")
		args := []string{"cp", confPath, r.dfsSetting.ClientConfPath}
		formatCmd = strings.Join(args, " ")
	}
	return formatCmd
}

func (r *BaseBuilder) getQuotaPath() string {
	quotaPath := r.dfsSetting.SubPath
	var subdir string
	for _, o := range r.dfsSetting.Options {
		pair := strings.Split(o, "=")
		if len(pair) != 2 {
			continue
		}
		if pair[0] == "subdir" {
			subdir = path.Join("/", pair[1])
		}
	}
	targetPath := path.Join(subdir, quotaPath)
	return targetPath
}

func (r *BaseBuilder) overwriteSubdirWithSubPath() {
	if r.dfsSetting.SubPath != "" {
		options := make([]string, 0)
		subdir := r.dfsSetting.SubPath
		for _, option := range r.dfsSetting.Options {
			if strings.HasPrefix(option, "subdir=") {
				s := strings.Split(option, "=")
				if len(s) != 2 {
					continue
				}
				if s[0] == "subdir" {
					subdir = path.Join(s[1], r.dfsSetting.SubPath)
				}
				continue
			}
			options = append(options, option)
		}
		r.dfsSetting.Options = append(options, fmt.Sprintf("subdir=%s", subdir))
	}
}

// genJobCommand generates job command
func (r *BaseBuilder) getJobCommand() string {
	var cmd string
	options := util.StripReadonlyOption(r.dfsSetting.Options)
	args := []string{config.CliPath, "${metaurl}", "/mnt/jfs"}
	if len(options) != 0 {
		args = append(args, "-o", util.EscapeBashStr(strings.Join(options, ",")))
	}
	cmd = strings.Join(args, " ")
	return util.QuoteForShell(cmd)
}

// genMetricsPort generates metrics port
func (r *BaseBuilder) genMetricsPort() int32 {
	port := int64(9567)
	options := r.dfsSetting.Options

	for _, option := range options {
		if strings.HasPrefix(option, "metrics=") {
			re := regexp.MustCompile(`metrics=.*:([0-9]{1,6})`)
			match := re.FindStringSubmatch(option)
			if len(match) > 0 {
				port, _ = strconv.ParseInt(match[1], 10, 32)
			}
		}
	}

	return int32(port)
}

// _genMetadata generates labels & annotations
func (r *BaseBuilder) _genMetadata() (labels map[string]string, annotations map[string]string) {
	labels = map[string]string{
		config.PodTypeKey:          config.PodTypeValue,
		config.PodUniqueIdLabelKey: r.dfsSetting.UniqueId,
	}
	annotations = map[string]string{}

	for k, v := range r.dfsSetting.Attr.Labels {
		labels[k] = v
	}
	for k, v := range r.dfsSetting.Attr.Annotations {
		annotations[k] = v
	}
	if r.dfsSetting.DeletedDelay != "" {
		annotations[config.DeleteDelayTimeKey] = r.dfsSetting.DeletedDelay
	}
	annotations[config.DingoFSID] = r.dfsSetting.FSID
	annotations[config.UniqueId] = r.dfsSetting.UniqueId
	if r.dfsSetting.CleanCache {
		annotations[config.CleanCache] = "true"
	}
	return
}

// _genJuiceVolumes generates volumes & volumeMounts
// 1. if encrypt_rsa_key is set, mount secret to /root/.rsa
// 2. if initconfig is set, mount secret to /etc/juicefs
// 3. configs in secret
func (r *BaseBuilder) _genJuiceVolumes() ([]corev1.Volume, []corev1.VolumeMount) {
	volumes := []corev1.Volume{}
	volumeMounts := []corev1.VolumeMount{}
	secretName := r.dfsSetting.SecretName

	if r.dfsSetting.EncryptRsaKey != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "rsa-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: secretName,
					Items: []corev1.KeyToPath{{
						Key:  "encrypt_rsa_key",
						Path: "rsa-key.pem",
					}},
				},
			},
		})
		volumeMounts = append(volumeMounts,
			corev1.VolumeMount{
				Name:      "rsa-key",
				MountPath: "/root/.rsa",
			},
		)
	}
	if r.dfsSetting.InitConfig != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "init-config",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: secretName,
				Items: []corev1.KeyToPath{{
					Key:  "initconfig",
					Path: r.dfsSetting.Name + ".conf",
				}},
			}},
		})
		volumeMounts = append(volumeMounts,
			corev1.VolumeMount{
				Name:      "init-config",
				MountPath: config.ROConfPath,
			},
		)
	}
	i := 1
	for k, v := range r.dfsSetting.Configs {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      fmt.Sprintf("config-%v", i),
			MountPath: v,
		})
		volumes = append(volumes, corev1.Volume{
			Name: fmt.Sprintf("config-%v", i),
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: k,
				},
			},
		})
		i++
	}
	return volumes, volumeMounts
}
