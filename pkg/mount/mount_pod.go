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
	"os"
	"time"

	config "github.com/jackblack369/dingofs-csi/pkg/config"
	"github.com/jackblack369/dingofs-csi/pkg/k8sclient"
	"github.com/jackblack369/dingofs-csi/pkg/util"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	k8sMount "k8s.io/utils/mount"
)

type PodMount struct {
	k8sMount.SafeFormatAndMount
	K8sClient *k8sclient.K8sClient
}

func (p *PodMount) Mount(ctx context.Context, appInfo *config.AppInfo, dfsSetting *config.DfsSetting) error {
	hashVal := util.GenHashOfSetting(*dfsSetting)
	dfsSetting.HashVal = hashVal
	klog.Infof("config.dfsSetting:%v", *dfsSetting)
	var mountPodName string
	var err error

	if err = func() error {
		lock := util.GetPodLock(hashVal)
		lock.Lock()
		defer lock.Unlock()

		mountPodName, err = util.GenMountPodName(ctx, p.K8sClient, dfsSetting)
		if err != nil {
			return err
		}

		// set mount pod name in app pod
		if appInfo != nil && appInfo.Name != "" && appInfo.Namespace != "" {
			err = util.SetMountLabel(ctx, p.K8sClient, dfsSetting.UniqueId, mountPodName, appInfo.Name, appInfo.Namespace)
			if err != nil {
				return err
			}
		}

		err = p.createOrAddRef(ctx, mountPodName, dfsSetting, appInfo) // bootstrap mount pod
		if err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}

	err = p.waitUtilMountReady(ctx, dfsSetting, mountPodName)
	if err != nil {
		return err
	}
	// TODO set fsid as annotation in mount pod
	//if dfsSetting.FSID == "" {
	//	// need set fsid as label in mount pod for clean cache
	//	fsid, err := GetDfsID(ctx, dfsSetting)
	//	if err != nil {
	//		return err
	//	}
	//	err = util.SetFSIDAnnotation(ctx, mountPodName, fsid)
	//	if err != nil {
	//		return err
	//	}
	//}
	return nil
}

func (p *PodMount) createOrAddRef(ctx context.Context, podName string, dfsSetting *config.DfsSetting, appinfo *config.AppInfo) (err error) {
	klog.Info("check mount pod existed or not", "podName", podName)
	dfsSetting.MountPath = dfsSetting.MountPath + podName[len(podName)-7:] // e.g. /dfs/pvc-7175fc74-d52d-46bc-94b3-ad9296b726cd-alypal
	dfsSetting.SecretName = fmt.Sprintf("dingofs-%s-secret", dfsSetting.UniqueId)
	// mkdir mountpath
	err = util.DoWithTimeout(ctx, 3*time.Second, func() error {
		exist, _ := k8sMount.PathExists(dfsSetting.MountPath)
		if !exist {
			return os.MkdirAll(dfsSetting.MountPath, 0777)
		}
		return nil
	})
	if err != nil {
		return
	}

	// r := builder.NewPodBuilder(dfsSetting, 0)
	// secret := r.NewSecret()
	// builder.SetPVAsOwner(&secret, dfsSetting.PV)

	key := util.GetReferenceKey(dfsSetting.TargetPath)

	waitCtx, waitCancel := context.WithTimeout(ctx, 60*time.Second)
	defer waitCancel()
	for {
		// wait for old pod deleted
		oldPod, err := p.K8sClient.GetPod(waitCtx, podName, config.Namespace)
		klog.Info("oldPod:", oldPod)
		if err == nil && oldPod.DeletionTimestamp != nil {
			klog.V(1).Info("wait for old mount pod deleted.", "podName", podName)
			time.Sleep(time.Millisecond * 500)
			continue
		} else if err != nil {
			if k8serrors.IsNotFound(err) {
				// pod not exist, create
				klog.Info("Need to create pod", "podName", podName)
				newPod, err := NewMountPod(podName)
				if err != nil {
					klog.Error(err, "Make new mount pod error", "podName", podName)
					return err
				}
				newPod.Annotations[key] = dfsSetting.TargetPath
				newPod.Labels[config.PodJuiceHashLabelKey] = dfsSetting.HashVal

				nodeSelector := map[string]string{
					"kubernetes.io/hostname": newPod.Spec.NodeName,
				}
				nodes, err := p.K8sClient.ListNode(ctx, &metav1.LabelSelector{MatchLabels: nodeSelector})
				if err != nil || len(nodes) != 1 || nodes[0].Name != newPod.Spec.NodeName {
					klog.Info("cannot select node by label selector", "nodeName", newPod.Spec.NodeName, "error", err)
				} else {
					newPod.Spec.NodeName = ""
					newPod.Spec.NodeSelector = nodeSelector
					if appinfo != nil && appinfo.Name != "" {
						appPod, err := p.K8sClient.GetPod(ctx, appinfo.Name, appinfo.Namespace)
						if err != nil {
							klog.Info("get app pod", "namespace", appinfo.Namespace, "name", appinfo.Name, "error", err)
						} else {
							newPod.Spec.Affinity = appPod.Spec.Affinity
							newPod.Spec.SchedulerName = appPod.Spec.SchedulerName
							newPod.Spec.Tolerations = appPod.Spec.Tolerations
						}
					}
				}

				//if err := util.CreateOrUpdateSecret(ctx, p.K8sClient, &secret); err != nil {
				//	return err
				//}
				_, err = p.K8sClient.CreatePod(ctx, newPod)
				if err != nil {
					klog.Error(err, "Create pod err", "podName", podName)
				}
				return err
			} else if k8serrors.IsTimeout(err) {
				return fmt.Errorf("mount %v failed: mount pod %s deleting timeout", dfsSetting.VolumeId, podName)
			}
			// unexpect error
			klog.Error(err, "Get pod err", "podName", podName)
			return err
		}
		// pod exist, add refs
		// if err := util.CreateOrUpdateSecret(ctx, p.K8sClient, &secret); err != nil {
		// 	return err
		// }
		return p.AddRefOfMount(ctx, dfsSetting.TargetPath, podName)
	}
}

func (p *PodMount) AddRefOfMount(ctx context.Context, target string, podName string) error {
	klog.Infof("Add target ref in mount pod, podName:%s ,target:%s", podName, target)
	// add volumeId ref in pod annotation
	key := util.GetReferenceKey(target)

	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		exist, err := p.K8sClient.GetPod(ctx, podName, config.Namespace)
		if err != nil {
			return err
		}
		if exist.DeletionTimestamp != nil {
			return fmt.Errorf("addRefOfMount: Mount pod [%s] has been deleted", podName)
		}
		annotation := exist.Annotations
		if _, ok := annotation[key]; ok {
			klog.Info("Target ref in pod already exists, target:%s, podName:%s", target, podName)
			return nil
		}
		if annotation == nil {
			annotation = make(map[string]string)
		}
		annotation[key] = target
		// delete deleteDelayAt when there ars refs
		delete(annotation, config.DeleteDelayAtKey)
		return util.ReplacePodAnnotation(ctx, p.K8sClient, exist, annotation)
	})
	if err != nil {
		klog.Error("Add target ref in mount pod[%s]error, %v ", podName, err)
		return err
	}
	return nil
}

func (p *PodMount) waitUtilMountReady(ctx context.Context, dfsSetting *config.DfsSetting, podName string) error {
	err := util.WaitUtilMountReady(ctx, podName, dfsSetting.MountPath, config.DefaultCheckTimeout)
	if err == nil {
		return nil
	}
	// mountpoint not ready, get mount pod log for detail
	log, err := util.GetErrContainerLog(ctx, p.K8sClient, podName)
	if err != nil {
		klog.Error(err, "Get pod log error", "podName", podName)
		return fmt.Errorf("mount %v at %v failed: mount isn't ready in 30 seconds", util.StripPasswd(dfsSetting.Source), dfsSetting.MountPath)
	}
	return fmt.Errorf("mount %v at %v failed, mountpod: %s, failed log: %v", util.StripPasswd(dfsSetting.Source), dfsSetting.MountPath, podName, log)
}
