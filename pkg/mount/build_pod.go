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
	"context"
	"fmt"
	"strings"

	config "github.com/jackblack369/dingofs-csi/pkg/config"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
)

// NewMountPod generates a pod with juicefs client
func NewMountPod(podName string) (*corev1.Pod, error) {
	pod := r.genCommonJuicePod(r.genCommonContainer)

	pod.Name = podName
	mountCmd := r.genMountCommand()
	cmd := mountCmd
	initCmd := r.genInitCommand()
	if initCmd != "" {
		cmd = strings.Join([]string{initCmd, mountCmd}, "\n")
	}
	pod.Spec.Containers[0].Command = []string{"sh", "-c", cmd}
	pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{
		Name:  "JFS_FOREGROUND",
		Value: "1",
	})

	// inject fuse fd
	if podName != "" && util.SupportFusePass(pod.Spec.Containers[0].Image) {
		fdAddress, err := fuse.GlobalFds.GetFdAddress(context.TODO(), r.jfsSetting.HashVal)
		if err != nil {
			return nil, err
		}
		pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{
			Name:  JfsCommEnv,
			Value: fdAddress,
		})
	}

	// generate volumes and volumeMounts only used in mount pod
	volumes, volumeMounts := r.genPodVolumes()
	pod.Spec.Volumes = append(pod.Spec.Volumes, volumes...)
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, volumeMounts...)

	// add cache-dir hostpath & PVC volume
	cacheVolumes, cacheVolumeMounts := r.genCacheDirVolumes()
	pod.Spec.Volumes = append(pod.Spec.Volumes, cacheVolumes...)
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, cacheVolumeMounts...)

	// add mount path host path volume
	mountVolumes, mountVolumeMounts := r.genHostPathVolumes()
	pod.Spec.Volumes = append(pod.Spec.Volumes, mountVolumes...)
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, mountVolumeMounts...)

	// add users custom volumes, volumeMounts, volumeDevices
	if r.jfsSetting.Attr.Volumes != nil {
		pod.Spec.Volumes = append(pod.Spec.Volumes, r.jfsSetting.Attr.Volumes...)
	}
	if r.jfsSetting.Attr.VolumeMounts != nil {
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, r.jfsSetting.Attr.VolumeMounts...)
	}
	if r.jfsSetting.Attr.VolumeDevices != nil {
		pod.Spec.Containers[0].VolumeDevices = append(pod.Spec.Containers[0].VolumeDevices, r.jfsSetting.Attr.VolumeDevices...)
	}

	return pod, nil
}

// genCommonContainer: generate common privileged container
func genCommonContainer() corev1.Container {
	isPrivileged := true
	rootUser := int64(0)
	return corev1.Container{
		Name:  config.MountContainerName,
		Image: r.BaseBuilder.jfsSetting.Attr.Image,
		SecurityContext: &corev1.SecurityContext{
			Privileged: &isPrivileged,
			RunAsUser:  &rootUser,
		},
		Env: []corev1.EnvVar{
			{
				Name:  config.JfsInsideContainer,
				Value: "1",
			},
		},
	}
}

// genCommonJuicePod generates a pod with common settings
func genCommonJuicePod(cnGen func() corev1.Container) *corev1.Pod {
	// gen again to update the mount pod spec
	if err := config.GenPodAttrWithCfg(r.jfsSetting, nil); err != nil {
		klog.Error("genCommonJuicePod gen pod attr failed, mount pod may not be the expected config, %v", err)
	}
	pod := r.genPodTemplate(cnGen)
	// labels & annotations
	pod.ObjectMeta.Labels, pod.ObjectMeta.Annotations = r._genMetadata()
	pod.Spec.ServiceAccountName = r.jfsSetting.Attr.ServiceAccountName
	pod.Spec.PriorityClassName = config.JFSMountPriorityName
	pod.Spec.RestartPolicy = corev1.RestartPolicyAlways
	pod.Spec.Hostname = r.jfsSetting.VolumeId
	gracePeriod := int64(10)
	if r.jfsSetting.Attr.TerminationGracePeriodSeconds != nil {
		gracePeriod = *r.jfsSetting.Attr.TerminationGracePeriodSeconds
	}
	pod.Spec.TerminationGracePeriodSeconds = &gracePeriod
	controllerutil.AddFinalizer(pod, config.Finalizer)

	volumes, volumeMounts := r._genJuiceVolumes()
	pod.Spec.Volumes = volumes
	pod.Spec.Containers[0].VolumeMounts = volumeMounts
	pod.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{
		SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: r.jfsSetting.SecretName,
			},
		},
	}}
	pod.Spec.Containers[0].Env = r.jfsSetting.Attr.Env
	pod.Spec.Containers[0].Resources = r.jfsSetting.Attr.Resources
	// if image support passFd from csi, do not set umount preStop
	if r.jfsSetting.Attr.Lifecycle == nil {
		if !util.SupportFusePass(pod.Spec.Containers[0].Image) || config.Webhook {
			pod.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{
				PreStop: &corev1.Handler{
					Exec: &corev1.ExecAction{Command: []string{"sh", "-c", "+e", fmt.Sprintf(
						"umount %s -l; rmdir %s; exit 0", r.jfsSetting.MountPath, r.jfsSetting.MountPath)}},
				},
			}
		}
	} else {
		pod.Spec.Containers[0].Lifecycle = r.jfsSetting.Attr.Lifecycle
	}

	pod.Spec.Containers[0].StartupProbe = r.jfsSetting.Attr.StartupProbe
	pod.Spec.Containers[0].LivenessProbe = r.jfsSetting.Attr.LivenessProbe
	pod.Spec.Containers[0].ReadinessProbe = r.jfsSetting.Attr.ReadinessProbe

	if r.jfsSetting.Attr.HostNetwork || !r.jfsSetting.IsCe {
		// When using hostNetwork, the MountPod will use a random port for metrics.
		// Before inducing any auxiliary method to detect that random port, the
		// best way is to avoid announcing any port about that.
		// Enterprise edition does not have metrics port.
		pod.Spec.Containers[0].Ports = []corev1.ContainerPort{}
	} else {
		pod.Spec.Containers[0].Ports = []corev1.ContainerPort{
			{Name: "metrics", ContainerPort: r.genMetricsPort()},
		}
	}
	return pod
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
		attr.Resources, err = ParsePodResources(cpuLimit, memoryLimit, cpuRequest, memoryRequest)
		if err != nil {
			klog.Error("Parse resource error: %v", err)
			return err
		}
		if v, ok := volCtx[config.MountPodLabelKey]; ok && v != "" {
			ctxLabel := make(map[string]string)
			if err := parseYamlOrJson(v, &ctxLabel); err != nil {
				return err
			}
			for k, v := range ctxLabel {
				attr.Labels[k] = v
			}
		}
		if v, ok := volCtx[config.MountPodAnnotationKey]; ok && v != "" {
			ctxAnno := make(map[string]string)
			if err := parseYamlOrJson(v, &ctxAnno); err != nil {
				return err
			}
			for k, v := range ctxAnno {
				attr.Annotations[k] = v
			}
		}
	}
	setting.Attr = attr
	// apply config patch
	applyConfigPatch(setting)

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
