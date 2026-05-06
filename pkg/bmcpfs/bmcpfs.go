/*
Copyright 2019 The Kubernetes Authors.

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

package bmcpfs

import (
	"time"

	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/cloud/metadata"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/common"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils"
	"k8s.io/klog/v2"
)

const (
	driverName = "bmcpfsplugin.csi.alibabacloud.com"

	// keys in volume context or publish context
	_vpcMountTarget = "vpcMountTarget"
	_vscMountTarget = "vscMountTarget"
	_vscID          = "vscId"
	_networkType    = "networkType"
	_path           = "path"
	_mpAutoSwitch   = "mountpointAutoSwitch"

	// CommonNodeIDPrefix is the prefix for common node IDs
	CommonNodeIDPrefix = "common:"
	// LingjunNodeIDPrefix is the prefix for lingjun node IDs
	LingjunNodeIDPrefix = "lingjun:"

	// network types of CPFS mount targets
	networkTypeVPC = "vpc"
	networkTypeVSC = "vsc"

	volumeHandleDelimiter = "+"

	FILESET_DESCRIBE_TIMEOUT = 5 * time.Minute
)

func NewDriver(meta *metadata.Metadata, endpoint string, serviceType utils.ServiceType) *common.Servers {
	var servers common.Servers
	servers.IdentityServer = newIdentityServer()

	if serviceType&utils.Controller != 0 {
		cs, err := newControllerServer(metadata.MustGet(meta, metadata.RegionID))
		if err != nil {
			klog.Fatalf("Init controller server: %v", err)
		}
		servers.ControllerServer = cs
	}
	if serviceType&utils.Node != 0 {
		ns, err := newNodeServer()
		if err != nil {
			klog.Fatalf("Init node server: %v", err)
		}
		servers.NodeServer = ns
	}

	return &servers
}
