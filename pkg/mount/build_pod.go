/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mount

import (
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"strings"

	config "github.com/jackblack369/dingofs-csi/pkg/config"
	"github.com/jackblack369/dingofs-csi/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// NewMountPod generates a pod with juicefs client
func NewMountPod(podName string, dfsSetting *config.DfsSetting) (*corev1.Pod, error) {
	container := genCommonContainer(dfsSetting)
	pod := genCommonPod(container, dfsSetting)

	pod.Name = podName
	mountCmd := genMountCommand(dfsSetting)
	cmd := mountCmd
	initCmd := genInitCommand(dfsSetting)
	if initCmd != "" {
		cmd = strings.Join([]string{initCmd, mountCmd}, "\n")
	}
	pod.Spec.Containers[0].Command = []string{"sh", "-c", cmd}
	pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{
		Name:  "JFS_FOREGROUND",
		Value: "1",
	})

	// inject fuse fd
	// if podName != "" && util.SupportFusePass(pod.Spec.Containers[0].Image) {
	// 	fdAddress, err := fuse.GlobalFds.GetFdAddress(context.TODO(), dfsSetting.HashVal)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{
	// 		Name:  config.DfsCommEnv,
	// 		Value: fdAddress,
	// 	})
	// }

	// generate volumes and volumeMounts only used in mount pod
	volumes, volumeMounts := genPodVolumes(dfsSetting)
	pod.Spec.Volumes = append(pod.Spec.Volumes, volumes...)
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, volumeMounts...)

	// add cache-dir hostpath & PVC volume
	cacheVolumes, cacheVolumeMounts := genCacheDirVolumes(dfsSetting)
	pod.Spec.Volumes = append(pod.Spec.Volumes, cacheVolumes...)
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, cacheVolumeMounts...)

	// add mount path host path volume
	mountVolumes, mountVolumeMounts := genHostPathVolumes(dfsSetting)
	pod.Spec.Volumes = append(pod.Spec.Volumes, mountVolumes...)
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, mountVolumeMounts...)

	// add users custom volumes, volumeMounts, volumeDevices
	if dfsSetting.Attr.Volumes != nil {
		pod.Spec.Volumes = append(pod.Spec.Volumes, dfsSetting.Attr.Volumes...)
	}
	if dfsSetting.Attr.VolumeMounts != nil {
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, dfsSetting.Attr.VolumeMounts...)
	}
	if dfsSetting.Attr.VolumeDevices != nil {
		pod.Spec.Containers[0].VolumeDevices = append(pod.Spec.Containers[0].VolumeDevices, dfsSetting.Attr.VolumeDevices...)
	}

	return pod, nil
}

// genCommonPod generates a pod with common settings
func genCommonPod(container corev1.Container, dfsSetting *config.DfsSetting) *corev1.Pod {
	// gen again to update the mount pod spec
	if err := GenPodAttrWithCfg(dfsSetting, nil); err != nil {
		klog.Error(err, "genCommonPod gen pod attr failed, mount pod may not be the expected config")
	}
	pod := genPodTemplate(container, dfsSetting)
	// labels & annotations
	pod.ObjectMeta.Labels, pod.ObjectMeta.Annotations = genMetadata(dfsSetting)
	pod.Spec.ServiceAccountName = dfsSetting.Attr.ServiceAccountName
	pod.Spec.PriorityClassName = config.DFSMountPriorityName
	pod.Spec.RestartPolicy = corev1.RestartPolicyAlways
	pod.Spec.Hostname = dfsSetting.VolumeId
	gracePeriod := int64(10)
	if dfsSetting.Attr.TerminationGracePeriodSeconds != nil {
		gracePeriod = *dfsSetting.Attr.TerminationGracePeriodSeconds
	}
	pod.Spec.TerminationGracePeriodSeconds = &gracePeriod
	controllerutil.AddFinalizer(pod, config.Finalizer)

	volumes, volumeMounts := genDingofsVolumes(dfsSetting)
	pod.Spec.Volumes = volumes
	pod.Spec.Containers[0].VolumeMounts = volumeMounts
	pod.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{
		SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: dfsSetting.SecretName,
			},
		},
	}}
	pod.Spec.Containers[0].Env = dfsSetting.Attr.Env
	pod.Spec.Containers[0].Resources = dfsSetting.Attr.Resources
	// if image support passFd from csi, do not set umount preStop
	// if dfsSetting.Attr.Lifecycle == nil {
	// 	if !util.SupportFusePass(pod.Spec.Containers[0].Image) || config.Webhook {
	// 		pod.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{
	// 			PreStop: &corev1.Handler{
	// 				Exec: &corev1.ExecAction{Command: []string{"sh", "-c", "+e", fmt.Sprintf(
	// 					"umount %s -l; rmdir %s; exit 0", dfsSetting.MountPath, dfsSetting.MountPath)}},
	// 			},
	// 		}
	// 	}
	// } else {
	// 	pod.Spec.Containers[0].Lifecycle = dfsSetting.Attr.Lifecycle
	// }

	pod.Spec.Containers[0].StartupProbe = dfsSetting.Attr.StartupProbe
	pod.Spec.Containers[0].LivenessProbe = dfsSetting.Attr.LivenessProbe
	pod.Spec.Containers[0].ReadinessProbe = dfsSetting.Attr.ReadinessProbe

	//if dfsSetting.Attr.HostNetwork || !dfsSetting.IsCe {
	//	// When using hostNetwork, the MountPod will use a random port for metrics.
	//	// Before inducing any auxiliary method to detect that random port, the
	//	// best way is to avoid announcing any port about that.
	//	// Enterprise edition does not have metrics port.
	//	pod.Spec.Containers[0].Ports = []corev1.ContainerPort{}
	//} else {
	//	pod.Spec.Containers[0].Ports = []corev1.ContainerPort{
	//		{Name: "metrics", ContainerPort: r.genMetricsPort()},
	//	}
	//}

	return pod
}

// genPodTemplate generates a pod template from csi pod
func genPodTemplate(container corev1.Container, dfsSetting *config.DfsSetting) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: dfsSetting.Attr.Namespace,
			Labels: map[string]string{
				config.PodTypeKey:          config.PodTypeValue,
				config.PodUniqueIdLabelKey: dfsSetting.UniqueId,
			},
			Annotations: make(map[string]string),
		},
		Spec: corev1.PodSpec{
			Containers:         []corev1.Container{container},
			NodeName:           config.NodeName,
			HostNetwork:        dfsSetting.Attr.HostNetwork,
			HostAliases:        dfsSetting.Attr.HostAliases,
			HostPID:            dfsSetting.Attr.HostPID,
			HostIPC:            dfsSetting.Attr.HostIPC,
			DNSConfig:          dfsSetting.Attr.DNSConfig,
			DNSPolicy:          dfsSetting.Attr.DNSPolicy,
			ServiceAccountName: dfsSetting.Attr.ServiceAccountName,
			ImagePullSecrets:   dfsSetting.Attr.ImagePullSecrets,
			PreemptionPolicy:   dfsSetting.Attr.PreemptionPolicy,
			Tolerations:        dfsSetting.Attr.Tolerations,
		},
	}
}

// genCommonContainer: generate common privileged container
func genCommonContainer(dfsSetting *config.DfsSetting) corev1.Container {
	isPrivileged := true
	rootUser := int64(0)
	return corev1.Container{
		Name:  config.MountContainerName,
		Image: dfsSetting.Attr.Image,
		SecurityContext: &corev1.SecurityContext{
			Privileged: &isPrivileged,
			RunAsUser:  &rootUser,
		},
		Env: []corev1.EnvVar{
			{
				Name:  config.DfsInsideContainer,
				Value: "1",
			},
		},
	}
}

func GenPodAttrWithCfg(setting *config.DfsSetting, volCtx map[string]string) error {
	var err error
	var attr *config.PodAttr
	if setting.Attr != nil {
		attr = setting.Attr
	} else {
		attr = &config.PodAttr{
			Namespace:          config.Namespace,
			MountPointPath:     config.MountPointPath,
			HostNetwork:        config.CSIPod.Spec.HostNetwork,
			HostAliases:        config.CSIPod.Spec.HostAliases,
			HostPID:            config.CSIPod.Spec.HostPID,
			HostIPC:            config.CSIPod.Spec.HostIPC,
			DNSConfig:          config.CSIPod.Spec.DNSConfig,
			DNSPolicy:          config.CSIPod.Spec.DNSPolicy,
			ImagePullSecrets:   config.CSIPod.Spec.ImagePullSecrets,
			Tolerations:        config.CSIPod.Spec.Tolerations,
			PreemptionPolicy:   config.CSIPod.Spec.PreemptionPolicy,
			ServiceAccountName: config.CSIPod.Spec.ServiceAccountName,
			Resources:          getDefaultResource(),
			Labels:             make(map[string]string),
			Annotations:        make(map[string]string),
		}
		attr.Image = config.DefaultMountImage
		setting.Attr = attr
	}

	if volCtx != nil {
		if v, ok := volCtx[config.MountPodImageKey]; ok && v != "" {
			attr.Image = v
		}
		if v, ok := volCtx[config.MountPodServiceAccount]; ok && v != "" {
			attr.ServiceAccountName = v
		}
		cpuLimit := volCtx[config.MountPodCpuLimitKey]
		memoryLimit := volCtx[config.MountPodMemLimitKey]
		cpuRequest := volCtx[config.MountPodCpuRequestKey]
		memoryRequest := volCtx[config.MountPodMemRequestKey]
		attr.Resources, err = util.ParsePodResources(cpuLimit, memoryLimit, cpuRequest, memoryRequest)
		if err != nil {
			klog.Error("Parse resource error: %v", err)
			return err
		}
		if v, ok := volCtx[config.MountPodLabelKey]; ok && v != "" {
			ctxLabel := make(map[string]string)
			if err := util.ParseYamlOrJson(v, &ctxLabel); err != nil {
				return err
			}
			for k, v := range ctxLabel {
				attr.Labels[k] = v
			}
		}
		if v, ok := volCtx[config.MountPodAnnotationKey]; ok && v != "" {
			ctxAnno := make(map[string]string)
			if err := util.ParseYamlOrJson(v, &ctxAnno); err != nil {
				return err
			}
			for k, v := range ctxAnno {
				attr.Annotations[k] = v
			}
		}
	}
	setting.Attr = attr
	// apply config patch
	ApplyConfigPatch(setting)

	return nil
}

func getDefaultResource() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(config.DefaultMountPodCpuLimit),
			corev1.ResourceMemory: resource.MustParse(config.DefaultMountPodMemLimit),
		},
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(config.DefaultMountPodCpuRequest),
			corev1.ResourceMemory: resource.MustParse(config.DefaultMountPodMemRequest),
		},
	}
}

// genMountCommand generates mount command
func genMountCommand(dfsSetting *config.DfsSetting) string {
	cmd := ""
	// options := dfsSetting.Options

	klog.Infof("setting source:%s, mountPath:%s", util.StripPasswd(dfsSetting.Source), dfsSetting.MountPath)
	// TODO mount command
	//mountArgs := []string{"exec", config.CeMountPath, "${metaurl}", security.EscapeBashStr(dfsSetting.MountPath)}
	//if !util.ContainsPrefix(options, "metrics=") {
	//	if dfsSetting.Attr.HostNetwork {
	//		// Pick up a random (useable) port for hostNetwork MountPods.
	//		options = append(options, "metrics=0.0.0.0:0")
	//	} else {
	//		options = append(options, "metrics=0.0.0.0:9567")
	//	}
	//}
	//mountArgs = append(mountArgs, "-o", security.EscapeBashStr(strings.Join(options, ",")))
	//cmd = strings.Join(mountArgs, " ")

	return util.QuoteForShell(cmd)
}

// genMetadata generates labels & annotations
func genMetadata(dfsSetting *config.DfsSetting) (labels map[string]string, annotations map[string]string) {
	labels = map[string]string{
		config.PodTypeKey:          config.PodTypeValue,
		config.PodUniqueIdLabelKey: dfsSetting.UniqueId,
	}
	annotations = map[string]string{}

	for k, v := range dfsSetting.Attr.Labels {
		labels[k] = v
	}
	for k, v := range dfsSetting.Attr.Annotations {
		annotations[k] = v
	}
	if dfsSetting.DeletedDelay != "" {
		annotations[config.DeleteDelayTimeKey] = dfsSetting.DeletedDelay
	}
	annotations[config.DingoFSID] = dfsSetting.FSID
	annotations[config.UniqueId] = dfsSetting.UniqueId
	if dfsSetting.CleanCache {
		annotations[config.CleanCache] = "true"
	}
	return
}

// genDingofsVolumes generates volumes & volumeMounts
// 1. if encrypt_rsa_key is set, mount secret to /root/.rsa
// 2. if initconfig is set, mount secret to /etc/dingofs
// 3. configs in secret
func genDingofsVolumes(dfsSetting *config.DfsSetting) ([]corev1.Volume, []corev1.VolumeMount) {
	volumes := []corev1.Volume{}
	volumeMounts := []corev1.VolumeMount{}
	secretName := dfsSetting.SecretName

	if dfsSetting.EncryptRsaKey != "" {
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
	if dfsSetting.InitConfig != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "init-config",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: secretName,
				Items: []corev1.KeyToPath{{
					Key:  "initconfig",
					Path: dfsSetting.Name + ".conf",
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
	for k, v := range dfsSetting.Configs {
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

// genCacheDirVolumes: generate cache-dir hostpath & PVC volume
func genCacheDirVolumes(dfsSetting *config.DfsSetting) ([]corev1.Volume, []corev1.VolumeMount) {
	cacheVolumes := []corev1.Volume{}
	cacheVolumeMounts := []corev1.VolumeMount{}

	hostPathType := corev1.HostPathDirectoryOrCreate

	for idx, cacheDir := range dfsSetting.CacheDirs {
		name := fmt.Sprintf("cachedir-%d", idx)

		hostPath := corev1.HostPathVolumeSource{
			Path: cacheDir,
			Type: &hostPathType,
		}
		hostPathVolume := corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				HostPath: &hostPath,
			},
		}
		cacheVolumes = append(cacheVolumes, hostPathVolume)

		volumeMount := corev1.VolumeMount{
			Name:      name,
			MountPath: cacheDir,
		}
		cacheVolumeMounts = append(cacheVolumeMounts, volumeMount)
	}

	for i, cache := range dfsSetting.CachePVCs {
		name := fmt.Sprintf("cachedir-pvc-%d", i)
		pvcVolume := corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: cache.PVCName,
					ReadOnly:  false,
				},
			},
		}
		cacheVolumes = append(cacheVolumes, pvcVolume)
		volumeMount := corev1.VolumeMount{
			Name:      name,
			ReadOnly:  false,
			MountPath: cache.Path,
		}
		cacheVolumeMounts = append(cacheVolumeMounts, volumeMount)
	}

	if dfsSetting.CacheEmptyDir != nil {
		name := "cachedir-empty-dir"
		emptyVolume := corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium:    corev1.StorageMedium(dfsSetting.CacheEmptyDir.Medium),
					SizeLimit: &dfsSetting.CacheEmptyDir.SizeLimit,
				},
			},
		}
		cacheVolumes = append(cacheVolumes, emptyVolume)
		volumeMount := corev1.VolumeMount{
			Name:      name,
			ReadOnly:  false,
			MountPath: dfsSetting.CacheEmptyDir.Path,
		}
		cacheVolumeMounts = append(cacheVolumeMounts, volumeMount)
	}

	if dfsSetting.CacheInlineVolumes != nil {
		for i, inlineVolume := range dfsSetting.CacheInlineVolumes {
			name := fmt.Sprintf("cachedir-inline-volume-%d", i)
			cacheVolumes = append(cacheVolumes, corev1.Volume{
				Name:         name,
				VolumeSource: corev1.VolumeSource{CSI: inlineVolume.CSI},
			})
			volumeMount := corev1.VolumeMount{
				Name:      name,
				ReadOnly:  false,
				MountPath: inlineVolume.Path,
			}
			cacheVolumeMounts = append(cacheVolumeMounts, volumeMount)
		}
	}

	return cacheVolumes, cacheVolumeMounts
}

// genPodVolumes: generate volumes for mount pod
// 1. jfs dir: mount point used to propagate the mount point in the mount container to host
// 2. update db dir: mount updatedb.conf from host to mount pod
// 3. jfs fuse fd path: mount fuse fd pass socket to mount pod
func genPodVolumes(dfsSetting *config.DfsSetting) ([]corev1.Volume, []corev1.VolumeMount) {
	dir := corev1.HostPathDirectoryOrCreate
	file := corev1.HostPathFileOrCreate
	mp := corev1.MountPropagationBidirectional
	volumes := []corev1.Volume{
		{
			Name: config.DfsDirName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: config.MountPointPath,
					Type: &dir,
				},
			},
		},
		{
			Name: config.DfsFuseFdPathName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: path.Join(config.DfsFuseFsPathInHost, dfsSetting.HashVal),
					Type: &dir,
				},
			},
		},
	}
	volumeMounts := []corev1.VolumeMount{
		{
			Name:             config.DfsDirName,
			MountPath:        config.PodMountBase,
			MountPropagation: &mp,
		},
		{
			Name:      config.DfsFuseFdPathName,
			MountPath: config.DfsFuseFsPathInPod,
		},
	}

	if !config.Immutable {
		volumes = append(volumes, corev1.Volume{
			Name: config.UpdateDBDirName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: config.UpdateDBCfgFile,
					Type: &file,
				},
			}},
		)
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      config.UpdateDBDirName,
			MountPath: config.UpdateDBCfgFile,
		})
	}

	return volumes, volumeMounts
}

// genHostPathVolumes: generate host path volumes
func genHostPathVolumes(dfsSetting *config.DfsSetting) (volumes []corev1.Volume, volumeMounts []corev1.VolumeMount) {
	volumes = []corev1.Volume{}
	volumeMounts = []corev1.VolumeMount{}
	if len(dfsSetting.HostPath) == 0 {
		return
	}
	for idx, hostPath := range dfsSetting.HostPath {
		name := fmt.Sprintf("hostpath-%d", idx)
		volumes = append(volumes, corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: hostPath,
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      name,
			MountPath: hostPath,
		})
	}
	return
}

// genInitCommand generates init command
func genInitCommand(dfsSetting *config.DfsSetting) string {
	formatCmd := dfsSetting.FormatCmd
	if dfsSetting.EncryptRsaKey != "" {
		formatCmd = formatCmd + " --encrypt-rsa-key=/root/.rsa/rsa-key.pem"
	}
	if dfsSetting.InitConfig != "" {
		confPath := filepath.Join(config.ROConfPath, dfsSetting.Name+".conf")
		args := []string{"cp", confPath, dfsSetting.ClientConfPath}
		formatCmd = strings.Join(args, " ")
	}
	return formatCmd
}

func ApplyConfigPatch(setting *config.DfsSetting) {
	attr := setting.Attr
	// overwrite by mountpod patch
	patch := GenMountPodPatch(setting)
	if patch.Image != "" {
		attr.Image = patch.Image
	}
	if patch.HostNetwork != nil {
		attr.HostNetwork = *patch.HostNetwork
	}
	if patch.HostPID != nil {
		attr.HostPID = *patch.HostPID
	}
	for k, v := range patch.Labels {
		attr.Labels[k] = v
	}
	for k, v := range patch.Annotations {
		attr.Annotations[k] = v
	}
	if patch.Resources != nil {
		attr.Resources = *patch.Resources
	}
	attr.Lifecycle = patch.Lifecycle
	attr.LivenessProbe = patch.LivenessProbe
	attr.ReadinessProbe = patch.ReadinessProbe
	attr.StartupProbe = patch.StartupProbe
	attr.TerminationGracePeriodSeconds = patch.TerminationGracePeriodSeconds
	attr.VolumeDevices = patch.VolumeDevices
	attr.VolumeMounts = patch.VolumeMounts
	attr.Volumes = patch.Volumes
	attr.Env = patch.Env

	// merge or overwrite setting options
	if setting.Options == nil {
		setting.Options = make([]string, 0)
	}
	for _, option := range patch.MountOptions {
		for i, o := range setting.Options {
			if strings.Split(o, "=")[0] == option {
				setting.Options = append(setting.Options[:i], setting.Options[i+1:]...)
			}
		}
		setting.Options = append(setting.Options, option)
	}
}

// GenMountPodPatch generate mount pod patch from jfsSettting
// 1. match pv selector
// 2. parse template value
// 3. return the merged mount pod patch
func GenMountPodPatch(setting *config.DfsSetting) config.MountPodPatch {
	patch := &config.MountPodPatch{
		Labels:      map[string]string{},
		Annotations: map[string]string{},
	}

	// merge each patch
	// for _, mp := range mountPodPatch {
	// 	if mp.IsMatch(setting.PVC) {
	// 		patch.Merge(mp.DeepCopy())
	// 	}
	// }

	patch.Image = patch.MountImage

	data, _ := json.Marshal(patch)
	strData := string(data)
	strData = strings.ReplaceAll(strData, "${MOUNT_POINT}", setting.MountPath)
	strData = strings.ReplaceAll(strData, "${VOLUME_ID}", setting.VolumeId)
	strData = strings.ReplaceAll(strData, "${VOLUME_NAME}", setting.Name)
	strData = strings.ReplaceAll(strData, "${SUB_PATH}", setting.SubPath)
	_ = json.Unmarshal([]byte(strData), patch)
	klog.V(1).Info("volume using patch", "volumeId", setting.VolumeId, "patch", patch)
	return *patch
}

func GenAndValidOptions(dfsSetting *config.DfsSetting, options []string) error {
	mountOptions := []string{}
	for _, option := range options {
		mountOption := strings.TrimSpace(option)
		ops := strings.Split(mountOption, "=")
		if len(ops) > 2 {
			return fmt.Errorf("invalid mount option: %s", mountOption)
		}
		if len(ops) == 2 {
			mountOption = fmt.Sprintf("%s=%s", strings.TrimSpace(ops[0]), strings.TrimSpace(ops[1]))
		}
		if mountOption == "writeback" {
			klog.Info("writeback is not suitable in CSI, please do not use it.", "volumeId", dfsSetting.VolumeId)
		}
		if len(ops) == 2 && ops[0] == "buffer-size" {
			memLimit := dfsSetting.Attr.Resources.Limits[corev1.ResourceMemory]
			memLimitByte := memLimit.Value()

			// buffer-size is in MiB, turn to byte
			bufferSize, err := util.ParseToBytes(ops[1])
			if err != nil {
				return fmt.Errorf("invalid mount option: %s", mountOption)
			}
			if bufferSize > uint64(memLimitByte) {
				return fmt.Errorf("buffer-size %s MiB is greater than pod memory limit %s", ops[1], memLimit.String())
			}
		}
		mountOptions = append(mountOptions, mountOption)
	}
	dfsSetting.Options = mountOptions
	return nil
}
