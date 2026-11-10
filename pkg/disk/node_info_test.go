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
	"errors"
	"testing"

	ecs20140526 "github.com/alibabacloud-go/ecs-20140526/v7/client"
	eflo_controller20221215 "github.com/alibabacloud-go/eflo-controller-20221215/v3/client"
	"github.com/golang/mock/gomock"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/cloud"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

const (
	testInstanceID   = "i-2ze0yyw7rf00yz9fttpg"
	testInstanceType = "ecs.g7.large"
	testRegion       = "cn-beijing"
	testZone         = "cn-beijing-i"
)

func describeInstances(id, zone, instanceType string, highDensity bool) *ecs20140526.DescribeInstancesResponse {
	return &ecs20140526.DescribeInstancesResponse{
		Body: &ecs20140526.DescribeInstancesResponseBody{
			Instances: &ecs20140526.DescribeInstancesResponseBodyInstances{
				Instance: []*ecs20140526.DescribeInstancesResponseBodyInstancesInstance{
					{
						InstanceId:   ptr.To(id),
						ZoneId:       ptr.To(zone),
						InstanceType: ptr.To(instanceType),
						AdditionalInfo: &ecs20140526.DescribeInstancesResponseBodyInstancesInstanceAdditionalInfo{
							EnableHighDensityMode: ptr.To(highDensity),
						},
					},
				},
			},
		},
	}
}

func describeInstanceTypes(instanceType string, quantity int32, densityDiskQuantity int32) *ecs20140526.DescribeInstanceTypesResponse {
	instance := &ecs20140526.DescribeInstanceTypesResponseBodyInstanceTypesInstanceType{
		InstanceTypeId: ptr.To(instanceType),
		DiskQuantity:   ptr.To(quantity),
	}
	if densityDiskQuantity > 0 {
		instance.Attributes = &ecs20140526.DescribeInstanceTypesResponseBodyInstanceTypesInstanceTypeAttributes{
			Attribute: []*ecs20140526.DescribeInstanceTypesResponseBodyInstanceTypesInstanceTypeAttributesAttribute{
				{Name: ptr.To("DensityDiskQuantity"), Value: ptr.To("97")},
			},
		}
	}
	return &ecs20140526.DescribeInstanceTypesResponse{
		Body: &ecs20140526.DescribeInstanceTypesResponseBody{
			InstanceTypes: &ecs20140526.DescribeInstanceTypesResponseBodyInstanceTypes{
				InstanceType: []*ecs20140526.DescribeInstanceTypesResponseBodyInstanceTypesInstanceType{instance},
			},
		},
	}
}

func describeDisks(ids ...string) *ecs20140526.DescribeDisksResponse {
	resp := &ecs20140526.DescribeDisksResponse{
		Body: &ecs20140526.DescribeDisksResponseBody{
			Disks: &ecs20140526.DescribeDisksResponseBodyDisks{},
		},
	}
	for _, id := range ids {
		resp.Body.Disks.Disk = append(resp.Body.Disks.Disk, &ecs20140526.DescribeDisksResponseBodyDisksDisk{
			DiskId:   ptr.To(id),
			Category: ptr.To("cloud_essd"),
			Status:   ptr.To("In_use"),
		})
	}
	return resp
}

func newTestNodeInfoResolver(t *testing.T) (*nodeInfoResolver, *cloud.MockECSv2Interface) {
	ctrl := gomock.NewController(t)
	ecs := cloud.NewMockECSv2Interface(ctrl)
	return newNodeInfoResolver(ecs, nil, testRegion), ecs
}

func TestNodeInfoResolver(t *testing.T) {
	t.Run("topology, limit and attached volumes", func(t *testing.T) {
		r, ecs := newTestNodeInfoResolver(t)
		ecs.EXPECT().DescribeInstancesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(describeInstances(testInstanceID, testZone, testInstanceType, false), nil)
		ecs.EXPECT().DescribeInstanceTypesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(describeInstanceTypes(testInstanceType, 17, 0), nil)
		// The system disk is reported as well: it counts towards the instance limit.
		ecs.EXPECT().DescribeDisksWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(describeDisks("d-system", "d-manual"), nil)

		resp, err := r.GetNodeInfo(t.Context(), testInstanceID)
		require.NoError(t, err)
		assert.EqualValues(t, 17, resp.MaxVolumesPerNode)
		assert.Equal(t, map[string]string{
			"alibabacloud.com/ecs-instance-id": testInstanceID,
			TopologyZoneKey:                    testZone,
			RegionalDiskTopologyKey:            testRegion,
			v1.LabelTopologyZone:               testZone,
		}, resp.AccessibleTopology.GetSegments())
		var published []string
		for _, vol := range resp.PublishedVolumes {
			published = append(published, vol.GetVolumeId())
		}
		assert.Equal(t, []string{"d-system", "d-manual"}, published)
	})

	t.Run("private topology key", func(t *testing.T) {
		orig := GlobalConfigVar.PrivateTopologyKey
		t.Cleanup(func() { GlobalConfigVar.PrivateTopologyKey = orig })
		GlobalConfigVar.PrivateTopologyKey = true

		r, ecs := newTestNodeInfoResolver(t)
		ecs.EXPECT().DescribeInstancesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(describeInstances(testInstanceID, testZone, testInstanceType, false), nil)
		ecs.EXPECT().DescribeInstanceTypesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(describeInstanceTypes(testInstanceType, 9, 0), nil)
		ecs.EXPECT().DescribeDisksWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(describeDisks(), nil)

		resp, err := r.GetNodeInfo(t.Context(), testInstanceID)
		require.NoError(t, err)
		assert.NotContains(t, resp.AccessibleTopology.GetSegments(), v1.LabelTopologyZone)
		assert.Empty(t, resp.PublishedVolumes)
	})

	t.Run("high density mode uses the density disk quantity", func(t *testing.T) {
		r, ecs := newTestNodeInfoResolver(t)
		ecs.EXPECT().DescribeInstancesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(describeInstances(testInstanceID, testZone, testInstanceType, true), nil)
		ecs.EXPECT().DescribeInstanceTypesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(describeInstanceTypes(testInstanceType, 17, 97), nil)
		ecs.EXPECT().DescribeDisksWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(describeDisks("d-system"), nil)

		resp, err := r.GetNodeInfo(t.Context(), testInstanceID)
		require.NoError(t, err)
		assert.EqualValues(t, 97, resp.MaxVolumesPerNode)
	})

	t.Run("high density quantity is ignored when the mode is off", func(t *testing.T) {
		r, ecs := newTestNodeInfoResolver(t)
		ecs.EXPECT().DescribeInstancesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(describeInstances(testInstanceID, testZone, testInstanceType, false), nil)
		ecs.EXPECT().DescribeInstanceTypesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(describeInstanceTypes(testInstanceType, 17, 97), nil)
		ecs.EXPECT().DescribeDisksWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(describeDisks(), nil)

		resp, err := r.GetNodeInfo(t.Context(), testInstanceID)
		require.NoError(t, err)
		assert.EqualValues(t, 17, resp.MaxVolumesPerNode)
	})

	t.Run("capacity is cached by instance type", func(t *testing.T) {
		r, ecs := newTestNodeInfoResolver(t)
		ecs.EXPECT().DescribeInstancesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(describeInstances(testInstanceID, testZone, testInstanceType, false), nil).Times(2)
		// Only once, the second call is served from the cache.
		ecs.EXPECT().DescribeInstanceTypesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(describeInstanceTypes(testInstanceType, 17, 0), nil).Times(1)
		ecs.EXPECT().DescribeDisksWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(describeDisks(), nil).Times(2)

		for range 2 {
			_, err := r.GetNodeInfo(t.Context(), testInstanceID)
			require.NoError(t, err)
		}
	})
}

func TestNodeInfoResolverErrors(t *testing.T) {
	testcases := []struct {
		name     string
		nodeID   string
		mockECS  func(*cloud.MockECSv2Interface)
		wantCode codes.Code
		wantErr  string
	}{
		{
			name:     "empty node id",
			nodeID:   "",
			wantCode: codes.InvalidArgument,
			wantErr:  "node_id must not be empty",
		},
		{
			name:   "instance not found",
			nodeID: testInstanceID,
			mockECS: func(ecs *cloud.MockECSv2Interface) {
				ecs.EXPECT().DescribeInstancesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(&ecs20140526.DescribeInstancesResponse{
						Body: &ecs20140526.DescribeInstancesResponseBody{
							Instances: &ecs20140526.DescribeInstancesResponseBodyInstances{},
						},
					}, nil)
			},
			wantCode: codes.NotFound,
			wantErr:  "not found",
		},
		{
			name:   "describe instances fails",
			nodeID: testInstanceID,
			mockECS: func(ecs *cloud.MockECSv2Interface) {
				ecs.EXPECT().DescribeInstancesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil, errors.New("throttled"))
			},
			wantCode: codes.NotFound,
			wantErr:  "throttled",
		},
		{
			name:   "instance type has no disk quantity",
			nodeID: testInstanceID,
			mockECS: func(ecs *cloud.MockECSv2Interface) {
				ecs.EXPECT().DescribeInstancesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(describeInstances(testInstanceID, testZone, testInstanceType, false), nil)
				ecs.EXPECT().DescribeInstanceTypesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(describeInstanceTypes(testInstanceType, 0, 0), nil)
			},
			wantCode: codes.Internal,
			wantErr:  "reports no attachable disk",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			r, ecs := newTestNodeInfoResolver(t)
			if tc.mockECS != nil {
				tc.mockECS(ecs)
			}
			_, err := r.GetNodeInfo(t.Context(), tc.nodeID)
			require.Error(t, err)
			assert.Equal(t, tc.wantCode, status.Code(err))
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestEffectiveDiskQuantity(t *testing.T) {
	testcases := []struct {
		name        string
		highDensity bool
		quantity    int32
		density     int32
		want        int32
	}{
		{name: "no high density support", quantity: 17, want: 17},
		{name: "high density supported but off", quantity: 17, density: 97, want: 17},
		{name: "high density on", highDensity: true, quantity: 17, density: 97, want: 97},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ecs := cloud.NewMockECSv2Interface(ctrl)
			ecs.EXPECT().DescribeInstanceTypesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(describeInstanceTypes(testInstanceType, tc.quantity, tc.density), nil)
			got, err := effectiveDiskQuantity(t.Context(), ecs, testInstanceType, tc.highDensity)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestNodeInfoResolverLingjun(t *testing.T) {
	ctrl := gomock.NewController(t)
	ecs := cloud.NewMockECSv2Interface(ctrl)
	eflo := cloud.NewMockEFLOInterface(ctrl)
	r := newNodeInfoResolver(ecs, eflo, testRegion)

	// Lingjun nodes are not ECS instances.
	ecs.EXPECT().DescribeInstancesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&ecs20140526.DescribeInstancesResponse{
			Body: &ecs20140526.DescribeInstancesResponseBody{
				Instances: &ecs20140526.DescribeInstancesResponseBodyInstances{},
			},
		}, nil)
	eflo.EXPECT().DescribeNode(gomock.Any()).DoAndReturn(
		func(req *eflo_controller20221215.DescribeNodeRequest) (*eflo_controller20221215.DescribeNodeResponse, error) {
			assert.Equal(t, testInstanceID, ptr.Deref(req.NodeId, ""))
			return &eflo_controller20221215.DescribeNodeResponse{
				Body: &eflo_controller20221215.DescribeNodeResponseBody{
					NodeType: ptr.To("ebs-enhanced"),
					HpnZone:  ptr.To("A3"),
					ZoneId:   ptr.To(testZone),
				},
			}, nil
		})
	eflo.EXPECT().DescribeNodeType(gomock.Any()).DoAndReturn(
		func(req *eflo_controller20221215.DescribeNodeTypeRequest) (*eflo_controller20221215.DescribeNodeTypeResponse, error) {
			assert.Equal(t, "ebs-enhanced", ptr.Deref(req.NodeType, ""))
			return &eflo_controller20221215.DescribeNodeTypeResponse{
				Body: &eflo_controller20221215.DescribeNodeTypeResponseBody{DiskQuantity: ptr.To(int32(10))},
			}, nil
		})
	ecs.EXPECT().DescribeDisksWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(describeDisks("d-system"), nil)

	resp, err := r.GetNodeInfo(t.Context(), testInstanceID)
	require.NoError(t, err)
	assert.EqualValues(t, 10, resp.MaxVolumesPerNode)
	assert.Equal(t, map[string]string{
		"alibabacloud.com/ecs-instance-id": testInstanceID,
		TopologyZoneKey:                    testZone,
		RegionalDiskTopologyKey:            testRegion,
		v1.LabelTopologyZone:               testZone,
		"alibabacloud.com/lingjun-worker":  "true",
		"alibabacloud.com/lingjun-hpnzone": "A3",
	}, resp.AccessibleTopology.GetSegments())
}

func TestNodeInfoResolverLingjunWithoutNodeType(t *testing.T) {
	ctrl := gomock.NewController(t)
	ecs := cloud.NewMockECSv2Interface(ctrl)
	eflo := cloud.NewMockEFLOInterface(ctrl)
	r := newNodeInfoResolver(ecs, eflo, testRegion)

	ecs.EXPECT().DescribeInstancesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&ecs20140526.DescribeInstancesResponse{
			Body: &ecs20140526.DescribeInstancesResponseBody{
				Instances: &ecs20140526.DescribeInstancesResponseBodyInstances{},
			},
		}, nil)
	eflo.EXPECT().DescribeNode(gomock.Any()).Return(&eflo_controller20221215.DescribeNodeResponse{
		Body: &eflo_controller20221215.DescribeNodeResponseBody{},
	}, nil)

	_, err := r.GetNodeInfo(t.Context(), testInstanceID)
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "has no node type")
}

func TestNodeInfoResolverMaxVolumesOverride(t *testing.T) {
	t.Setenv("MAX_VOLUMES_PERNODE", "3")
	r, ecs := newTestNodeInfoResolver(t)
	ecs.EXPECT().DescribeInstancesWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(describeInstances(testInstanceID, testZone, testInstanceType, false), nil)
	// No DescribeInstanceTypes/DescribeDisks call: the override replaces the
	// cloud API derived limit and the non-CSI deduction.
	resp, err := r.GetNodeInfo(t.Context(), testInstanceID)
	require.NoError(t, err)
	assert.EqualValues(t, 3, resp.MaxVolumesPerNode)
	assert.Empty(t, resp.PublishedVolumes)
	assert.Equal(t, testZone, resp.AccessibleTopology.GetSegments()[TopologyZoneKey])
}
