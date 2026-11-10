/*
Copyright 2025 The Kubernetes Authors.

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

package disk

import (
	"context"
	"fmt"
	"time"

	eflo_controller20221215 "github.com/alibabacloud-go/eflo-controller-20221215/v3/client"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/cloud"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/cloud/metadata"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/common"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils/ttlcache"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// nodeInfoCacheTTL is how long the disk quantity of an instance type is cached.
// Instance types are immutable, the TTL only bounds the effect of a failed or
// outdated OpenAPI response.
const nodeInfoCacheTTL = 24 * time.Hour

// nodeInfoResolver answers ControllerGetNodeInfo for an arbitrary instance of the
// cluster, without relying on the instance metadata service of that instance.
type nodeInfoResolver struct {
	ecs      cloud.ECSv2Interface
	eflo     cloud.EFLOInterface // nil if the driver is not configured for Lingjun
	regionID string
	// quantity caches the disk quantity by instance type (or Lingjun node type),
	// as most clusters only use a handful of them.
	quantity *ttlcache.TTLCache[string, int32]
}

func newNodeInfoResolver(ecs cloud.ECSv2Interface, eflo cloud.EFLOInterface, regionID string) *nodeInfoResolver {
	return &nodeInfoResolver{
		ecs:      ecs,
		eflo:     eflo,
		regionID: regionID,
		quantity: ttlcache.NewTTLCache[string, int32](nodeInfoCacheTTL),
	}
}

// instanceInfo is the part of a node's identity that only the cloud API knows.
type instanceInfo struct {
	zoneID   string
	quantity int32
	lingjun  bool
	hpnZone  string
}

func (r *nodeInfoResolver) GetNodeInfo(ctx context.Context, instanceID string) (*csi.ControllerGetNodeInfoResponse, error) {
	if instanceID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id must not be empty")
	}
	logger := klog.FromContext(ctx)
	logger.V(2).Info("resolving node info", "instanceID", instanceID)

	// MAX_VOLUMES_PERNODE is a manual reservation: it replaces everything derived
	// from the cloud API, including the non-CSI attachments, like it does in the
	// node-side flow.
	maxVolumes, err := parseVolumeCountEnv()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	info, err := r.instanceInfo(ctx, instanceID, maxVolumes == 0)
	if err != nil {
		return nil, err
	}
	if maxVolumes > 0 {
		return &csi.ControllerGetNodeInfoResponse{
			MaxVolumesPerNode:  int64(maxVolumes),
			AccessibleTopology: &csi.Topology{Segments: r.topology(instanceID, info)},
		}, nil
	}
	published, err := GetAttachedCloudDisks(ctx, r.ecs, instanceID, r.regionID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list disks of instance %s: %v", instanceID, err)
	}

	resp := &csi.ControllerGetNodeInfoResponse{
		MaxVolumesPerNode:  int64(info.quantity),
		AccessibleTopology: &csi.Topology{Segments: r.topology(instanceID, info)},
	}
	for _, diskID := range published {
		resp.PublishedVolumes = append(resp.PublishedVolumes, &csi.ControllerGetNodeInfoResponse_PublishedVolume{
			VolumeId: diskID,
		})
	}
	logger.V(2).Info("resolved node info", "instanceID", instanceID,
		"maxVolumes", info.quantity, "publishedVolumes", len(published), "lingjun", info.lingjun)
	return resp, nil
}

func (r *nodeInfoResolver) topology(instanceID string, info *instanceInfo) map[string]string {
	return topologySegments(&topologyInfo{
		InstanceID: instanceID,
		ZoneID:     info.zoneID,
		RegionID:   r.regionID,
		Lingjun:    info.lingjun,
		HpnZone:    info.hpnZone,
	})
}

// instanceInfo resolves the zone and the disk quantity of an instance. ECS
// instances are resolved through OpenAPI, Lingjun nodes through EFLO: they are
// not ECS instances and DescribeInstances does not find them.
func (r *nodeInfoResolver) instanceInfo(ctx context.Context, instanceID string, wantQuantity bool) (*instanceInfo, error) {
	instance, ecsErr := metadata.NewOpenAPIMetadata(ctx, r.ecs, instanceID)
	if ecsErr == nil {
		zoneID, err := instance.ZoneID()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "get zone of instance %s: %v", instanceID, err)
		}
		instanceType, err := instance.InstanceType()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "get instance type of instance %s: %v", instanceID, err)
		}
		if !wantQuantity {
			return &instanceInfo{zoneID: zoneID}, nil
		}
		quantity, err := r.quantity.Get(ctx, instanceType, func() (int32, error) {
			return effectiveDiskQuantity(ctx, r.ecs, instanceType, instance.HighDensityMode())
		})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "get disk quantity of instance type %s: %v", instanceType, err)
		}
		if quantity <= 0 {
			return nil, status.Errorf(codes.Internal, "instance type %s reports no attachable disk", instanceType)
		}
		return &instanceInfo{zoneID: zoneID, quantity: quantity}, nil
	}

	if r.eflo == nil {
		return nil, status.Errorf(codes.NotFound, "describe instance %s: %v", instanceID, ecsErr)
	}
	logger := klog.FromContext(ctx)
	logger.V(3).Info("DescribeInstances did not find the instance, trying EFLO", "instanceID", instanceID, "err", ecsErr)

	node, err := r.eflo.DescribeNode(&eflo_controller20221215.DescribeNodeRequest{NodeId: ptr.To(instanceID)})
	if err != nil || node == nil || node.Body == nil {
		return nil, status.Errorf(codes.NotFound, "describe instance %s with ECS (%v) or EFLO (%v): not found", instanceID, ecsErr, err)
	}
	if !wantQuantity {
		return &instanceInfo{
			zoneID:  ptr.Deref(node.Body.ZoneId, ""),
			lingjun: true,
			hpnZone: ptr.Deref(node.Body.HpnZone, ""),
		}, nil
	}
	nodeType := ptr.Deref(node.Body.NodeType, "")
	if nodeType == "" {
		// NodeGetInfo reports no limit for these Lingjun nodes either.
		return nil, status.Errorf(codes.FailedPrecondition, "Lingjun node %s has no node type", instanceID)
	}
	quantity, err := r.quantity.Get(ctx, "lingjun/"+nodeType, func() (int32, error) {
		nodeTypeResp, err := r.eflo.DescribeNodeType(&eflo_controller20221215.DescribeNodeTypeRequest{NodeType: ptr.To(nodeType)})
		if err != nil {
			return 0, err
		}
		if nodeTypeResp == nil || nodeTypeResp.Body == nil {
			return 0, fmt.Errorf("DescribeNodeType(%s) returned no body", nodeType)
		}
		return ptr.Deref(nodeTypeResp.Body.DiskQuantity, 0), nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get disk quantity of Lingjun node type %s: %v", nodeType, err)
	}
	if quantity <= 0 {
		return nil, status.Errorf(codes.Internal, "Lingjun node type %s reports no attachable disk", nodeType)
	}
	return &instanceInfo{
		zoneID:   ptr.Deref(node.Body.ZoneId, ""),
		quantity: quantity,
		lingjun:  true,
		hpnZone:  ptr.Deref(node.Body.HpnZone, ""),
	}, nil
}

// effectiveDiskQuantity returns the number of disks attachable to an instance of
// the given type, taking HighDensityDisk mode into account. It mirrors
// metadata.EffectiveDiskQuantity, which cannot be used here because the instance
// is not the local one.
func effectiveDiskQuantity(ctx context.Context, ecs cloud.ECSv2Interface, instanceType string, highDensityMode bool) (int32, error) {
	instanceTypeMetadata, err := metadata.NewEcsInstanceTypeMetadata(ctx, ecs, instanceType)
	if err != nil {
		return 0, err
	}
	if density := instanceTypeMetadata.DiskQuantityHighDensity(); highDensityMode && density > 0 {
		return density, nil
	}
	return instanceTypeMetadata.DiskQuantity(), nil
}

// topologySegments returns the topology of a node, as reported by NodeGetInfo.
// Both flows must report the same keys: existing PVs have node affinity rules
// that reference them.
func topologySegments(info *topologyInfo) map[string]string {
	segments := map[string]string{
		common.ECSInstanceIDTopologyKey: info.InstanceID,
		// TopologyZoneKey is always defined for existing persistent volumes
		TopologyZoneKey:         info.ZoneID,
		RegionalDiskTopologyKey: info.RegionID,
	}
	if !GlobalConfigVar.PrivateTopologyKey {
		segments[v1.LabelTopologyZone] = info.ZoneID
	}
	if info.Lingjun {
		segments[metadata.LingjunWorkerLabel] = "true"
		if info.HpnZone != "" {
			segments[metadata.LingjunHpnZoneLabel] = info.HpnZone
		}
	}
	return segments
}

type topologyInfo struct {
	InstanceID string
	ZoneID     string
	RegionID   string
	Lingjun    bool
	HpnZone    string
}
