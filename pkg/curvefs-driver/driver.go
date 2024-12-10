/*
Copyright 2022 The Curve Authors

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

package curvefsdriver

import (
	"context"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/jackblack369/dingofs-csi/pkg/config"
	"github.com/jackblack369/dingofs-csi/pkg/csicommon"
	k8s "github.com/jackblack369/dingofs-csi/pkg/k8sclient"
	"github.com/jackblack369/dingofs-csi/pkg/util"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/utils/mount"
)

const DriverName = "csi.dingofs.com"

type CurvefsDriver struct {
	*csicommon.CSIDriver
	ids      *identityServer
	cs       *controllerServer
	ns       *nodeServer
	endpoint string
}

// NewDriver create a new curvefs driver
func NewDriver(endpoint string, nodeID string) (*CurvefsDriver, error) {
	csiDriver := csicommon.NewCSIDriver(DriverName, util.GetVersion(), nodeID)
	csiDriver.AddControllerServiceCapabilities(
		[]csi.ControllerServiceCapability_RPC_Type{
			csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		})
	csiDriver.AddVolumeCapabilityAccessModes(
		[]csi.VolumeCapability_AccessMode_Mode{
			csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
			csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
			csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		})
	return &CurvefsDriver{CSIDriver: csiDriver, endpoint: endpoint}, nil
}

// NewControllerServer create a new controller server
func NewControllerServer(d *CurvefsDriver) *controllerServer {
	config, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("Failed to create k8s config: %v", err)
	}
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to create k8s client: %v", err)
	}
	return &controllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d.CSIDriver),
		driver:                  d,
		kubeClient:              clientSet,
	}
}

// NewNodeServer create a new node server
func NewNodeServer(d *CurvefsDriver) *nodeServer {
	parseNodeConfig()
	return &nodeServer{
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d.CSIDriver),
		mounter:           mount.New(""),
		mountRecord:       map[string]map[string]string{},
	}
}

// Run start a new node server
func (d *CurvefsDriver) Run() {
	csicommon.RunControllerandNodePublishServer(
		d.endpoint,
		d.CSIDriver,
		NewControllerServer(d),
		NewNodeServer(d),
	)
}

func parseNodeConfig() {
	if os.Getenv("DRIVER_NAME") != "" {
		config.DriverName = os.Getenv("DRIVER_NAME")
	}

	config.NodeName = os.Getenv("NODE_NAME")
	config.Namespace = os.Getenv("JUICEFS_MOUNT_NAMESPACE")
	config.PodName = os.Getenv("POD_NAME")

	k8sclient, err := k8s.NewClient()
	if err != nil {
		klog.Error(err, "Can't get k8s client")
		os.Exit(1)
	}
	pod, err := k8sclient.GetPod(context.TODO(), config.PodName, config.Namespace)
	if err != nil {
		klog.Error(err, "Can't get pod", "pod", config.PodName)
		os.Exit(1)
	}

	config.CSIPod = *pod

	// err = fuse.InitGlobalFds(context.TODO(), "/tmp")
	// if err != nil {
	// 	log.Error(err, "Init global fds error")
	// 	os.Exit(1)
	// }
}
