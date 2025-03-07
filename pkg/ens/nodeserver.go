package ens

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/common"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/disk"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils"
	utilsio "github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils/io"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	k8smount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
)

const (
	// MAX_VOLUMES_PERNODE defines default max volumes count pernode
	MAX_VOLUMES_PERNODE = 15
	// MkfsOptions tag
	MKFS_OPTIONS = "mkfsOptions"
	// NOUUID is xfs fs mount opts
	NOUUID = "nouuid"
	// SysConfigTag tag
	SYS_CONFIG_TAG = "sysConfig"
	// INPUT_OUTPUT_ERR tag
	INPUT_OUTPUT_ERR = "input/output error"
)

var (
	// BLOCK_VOLUME_PREFIX block volume mount prefix
	BLOCK_VOLUME_PREFIX = filepath.Join(utils.KubeletRootDir, "/plugins/kubernetes.io/csi/volumeDevices/publish")
)

type nodeServer struct {
	MaxVolumePerNode int64
	mounter          utils.Mounter
	k8smounter       k8smount.Interface
	podCgroup        *utils.PodCGroup
	common.GenericNodeServer
}

func NewNodeServer() csi.NodeServer {

	var maxVolumesNum int64 = MAX_VOLUMES_PERNODE
	volumeNum := os.Getenv("MAX_VOLUMES_PERNODE")
	if "" != volumeNum {
		num, err := strconv.ParseInt(volumeNum, 10, 64)
		if err != nil {
			klog.Fatalf("NewNodeServer: MAX_VOLUMES_PERNODE must be int64, but get: %s", volumeNum)
		} else {
			if num < 0 || num > 64 {
				klog.Errorf("NewNodeServer: MAX_VOLUMES_PERNODE must between 0-15, but get: %s", volumeNum)
			} else {
				maxVolumesNum = num
				klog.Infof("NewNodeServer: MAX_VOLUMES_PERNODE is set to(not default): %d", maxVolumesNum)
			}
		}
	}

	_ = os.MkdirAll(VolumeDir, os.FileMode(0755))
	_ = os.MkdirAll(VolumeDirRemove, os.FileMode(0755))

	podCgroup, err := utils.NewPodCGroup()
	if err != nil {
		klog.Fatalf("Failed to initialize pod cgroup: %v", err)
	}

	return &nodeServer{
		MaxVolumePerNode: maxVolumesNum,
		mounter:          utils.NewMounter(),
		podCgroup:        podCgroup,
		k8smounter:       k8smount.New(""),
		GenericNodeServer: common.GenericNodeServer{
			NodeID: GlobalConfigVar.InstanceID,
		},
	}
}

func (ns *nodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	// currently there is a single NodeServer capability according to the spec
	nscap := &csi.NodeServiceCapability{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
			},
		},
	}
	// nscap2 := &csi.NodeServiceCapability{
	// 	Type: &csi.NodeServiceCapability_Rpc{
	// 		Rpc: &csi.NodeServiceCapability_RPC{
	// 			Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
	// 		},
	// 	},
	// }

	// Disk Metric enable config
	nodeSvcCap := []*csi.NodeServiceCapability{nscap}

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: nodeSvcCap,
	}, nil
}

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	// check target mount path
	sourcePath := req.StagingTargetPath
	// running in runc/runv mode
	isBlock := req.GetVolumeCapability().GetBlock() != nil
	if isBlock {
		sourcePath = filepath.Join(req.StagingTargetPath, req.VolumeId)
	}
	targetPath := req.GetTargetPath()
	klog.Infof("NodePublishVolume: Starting Mount Volume %s, source %s > target %s", req.VolumeId, sourcePath, targetPath)
	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "NodePublishVolume: Staging Target Path must be provided")
	}
	// check if block volume
	if isBlock {
		notMounted, err := ns.k8smounter.IsLikelyNotMountPoint(targetPath)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, status.Errorf(codes.Internal, "failed to check if %s is a mount point: %v", targetPath, err)
		}
		if notMounted {
			if err := ns.mounter.EnsureBlock(targetPath); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			options := []string{"bind"}
			if err := ns.mounter.MountBlock(sourcePath, targetPath, options...); err != nil {
				return nil, err
			}
		}
		klog.Infof("NodePublishVolume: Mount Successful Block Volume: %s, from source %s to target %v", req.VolumeId, sourcePath, targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if err := os.MkdirAll(targetPath, 0755); err != nil {
		klog.Errorf("NodePublishVolume: create volume %s path %s error: %v", req.VolumeId, targetPath, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	notmounted, err := ns.k8smounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		klog.Errorf("NodePublishVolume: check volume %s target path %s error: %v", req.VolumeId, targetPath, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !notmounted {
		klog.Infof("NodePublishVolume: VolumeId: %s, Path %s is already mounted", req.VolumeId, targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	sourceNotMounted, err := ns.k8smounter.IsLikelyNotMountPoint(sourcePath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	expectDevice := getVolumeDeviceName(req.GetVolumeId())
	if sourceNotMounted {
		klog.Infof("NodePublishVolume: globalmount path: %s is not mounted", sourcePath)
		if expectDevice != "" {
			if err := ns.mountDeviceToGlobal(req.VolumeCapability, req.VolumeContext, expectDevice, sourcePath); err != nil {
				klog.Errorf("NodePublishVolume: VolumeId: %s, remount disk to global %s error: %s", req.VolumeId, sourcePath, err.Error())
				return nil, status.Error(codes.Internal, "NodePublishVolume: VolumeId: %s, remount disk error "+err.Error())
			}
			klog.Infof("NodePublishVolume: SourcePath %s not mounted, and mounted again with device %s", sourcePath, expectDevice)
		} else {
			klog.Errorf("NodePublishVolume: VolumeId: %s, sourcePath %s is Not mounted and device cannot found", req.VolumeId, sourcePath)
			return nil, status.Error(codes.Internal, "NodePublishVolume: VolumeId: %s, sourcePath %s is Not mounted "+sourcePath)
		}
	}

	// start to mount
	mnt := req.VolumeCapability.GetMount()
	options := append(mnt.MountFlags, "bind")
	if req.Readonly {
		options = append(options, "ro")
	}
	fsType := "ext4"
	if mnt.FsType != "" {
		fsType = mnt.FsType
	}

	// check device name available
	realDevice, _, err := mount.GetDeviceNameFromMount(ns.k8smounter, sourcePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "NodePublishVolume: get device name from mount %s error: %s", sourcePath, err.Error())
	}
	if realDevice == "" {
		opts := append(mnt.MountFlags, "shared")
		if err := ns.k8smounter.Mount(expectDevice, sourcePath, fsType, opts); err != nil {
			errMsg := fmt.Sprintf("NodePublishVolume: mount source error: %s, %s, %s", expectDevice, sourcePath, err.Error())
			klog.Error(errMsg)
			return nil, status.Error(codes.Internal, errMsg)
		}
		realDevice, _, err = mount.GetDeviceNameFromMount(ns.k8smounter, sourcePath)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "NodePublishVolume: get device name from mount %s error: %s", sourcePath, err.Error())
		}
	}
	if expectDevice != realDevice || realDevice == "" {
		err := fmt.Errorf("NodePublishVolume: Volume: %s, sourcePath: %s real Device: %s not same with expected: %s", req.VolumeId, sourcePath, realDevice, expectDevice)
		klog.Error(err.Error())
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Set volume IO Limit
	err = ns.podCgroup.ApplyConfig(realDevice, req)
	if err != nil {
		klog.Errorf("NodePublishVolume: Set Disk Volume(%s) IO Limit with Error: %s", req.VolumeId, err.Error())
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("NodePublishVolume: Starting mount volume %s with flags %v and fsType %s", req.VolumeId, options, fsType)
	if err = ns.k8smounter.Mount(sourcePath, targetPath, fsType, options); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("NodePublishVolume: Mount Successful Volume: %s, from source %s to target %v", req.VolumeId, sourcePath, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	klog.Infof("NodeUnpublishVolume: Starting to Unmount Volume %s, Target %v", req.VolumeId, targetPath)
	// Step 1: check folder exists
	if !utils.IsFileExisting(targetPath) {
		if err := ns.unmountDuplicateMountPoint(targetPath); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		klog.Infof("NodeUnpublishVolume: Volume %s Folder %s doesn't exist", req.VolumeId, targetPath)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	// Step 2: check mount point
	notmounted, err := ns.k8smounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if notmounted {
		if empty, _ := utils.IsDirEmpty(targetPath); empty {
			if err := ns.unmountDuplicateMountPoint(targetPath); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			klog.Infof("NodeUnpublishVolume: %s is unmounted and empty", targetPath)
			return &csi.NodeUnpublishVolumeResponse{}, nil
		}
		// Block device
		if !utils.IsDir(targetPath) && strings.HasPrefix(targetPath, BLOCK_VOLUME_PREFIX) {
			if removeErr := os.Remove(targetPath); removeErr != nil {
				return nil, status.Errorf(codes.Internal, "NodeUnpublishVolume: VolumeId: %s, Could not remove mount block target %s with error %v", req.VolumeId, targetPath, removeErr)
			}
			klog.Infof("NodeUnpublishVolume: %s is block volume and is removed successful", targetPath)
			return &csi.NodeUnpublishVolumeResponse{}, nil
		}
		klog.Errorf("NodeUnpublishVolume: VolumeId: %s, Path %s is unmounted, but not empty dir", req.VolumeId, targetPath)
		return nil, status.Errorf(codes.Internal, "NodeUnpublishVolume: VolumeId: %s, Path %s is unmounted, but not empty dir", req.VolumeId, targetPath)
	}

	// Step 3: umount target path
	err = ns.k8smounter.Unmount(targetPath)
	if err != nil {
		klog.Errorf("NodeUnpublishVolume: volumeId: %s, umount path: %s with error: %s", req.VolumeId, targetPath, err.Error())
		return nil, status.Error(codes.Internal, err.Error())
	}
	err = os.Remove(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "NodeUnpublishVolume: Cannot remove targetPath %s: %v", targetPath, err)
	}

	// below directory can not be umounted by kubelet in ack
	if err := ns.unmountDuplicateMountPoint(targetPath); err != nil {
		klog.Errorf("NodeUnpublishVolume: umount duplicate mountpoint %s with error: %v", targetPath, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	klog.Infof("NodeUnpublishVolume: Umount Successful for volume %s, target %v", req.VolumeId, targetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	klog.Infof("NodeStageVolume: Stage VolumeId: %s, Target Path: %s, VolumeContext: %v", req.GetVolumeId(), req.StagingTargetPath, req.VolumeContext)

	targetPath := req.StagingTargetPath
	// targetPath format: /var/lib/kubelet/plugins/kubernetes.io/csi/pv/pv-disk-1e7001e0-c54a-11e9-8f89-00163e0e78a0/globalmount

	isBlock := req.GetVolumeCapability().GetBlock() != nil
	if isBlock {
		targetPath = filepath.Join(targetPath, req.VolumeId)
		notMounted, err := ns.k8smounter.IsLikelyNotMountPoint(targetPath)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, status.Errorf(codes.Internal, "failed to check if %s is a mount point: %v", targetPath, err)
		}
		if !notMounted {
			klog.Infof("NodeStageVolume: Block Already Mounted: volumeId: %s target %s", req.VolumeId, targetPath)
			return &csi.NodeStageVolumeResponse{}, nil
		}
		if err := ns.mounter.EnsureBlock(targetPath); err != nil {
			klog.Errorf("NodeStageVolume: create block volume %s path %s error: %v", req.VolumeId, targetPath, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else {
		if err := os.MkdirAll(targetPath, 0755); err != nil {
			klog.Errorf("NodeStageVolume: create volume %s path %s error: %v", req.VolumeId, targetPath, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	// Step 2: check target path mounted
	notmounted, err := ns.k8smounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		klog.Errorf("NodeStageVolume: check volume %s path %s error: %v", req.VolumeId, targetPath, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !notmounted {
		// if target path is mounted tmpfs, return
		isTmpfs, err := utils.IsDirTmpfs(ns.k8smounter, req.StagingTargetPath)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "NodeStageVolume: failed to check %s for tmpfs: %v", req.StagingTargetPath, err)
		}
		if isTmpfs {
			klog.Infof("NodeStageVolume: TargetPath(%s) is mounted as tmpfs, not need mount again", req.StagingTargetPath)
			return &csi.NodeStageVolumeResponse{}, nil
		}

		// check device available
		deviceName, _, err := mount.GetDeviceNameFromMount(ns.k8smounter, targetPath)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "NodePublishVolume: get device name from mount %s error: %s", targetPath, err.Error())
		}
		if err := disk.CheckDeviceAvailable(deviceName, req.VolumeId, targetPath); err != nil {
			klog.Errorf("NodeStageVolume: mountPath is mounted %s, but check device available error: %s", targetPath, err.Error())
			return nil, status.Error(codes.Internal, err.Error())
		}
		klog.Infof("NodeStageVolume:  volumeId: %s, Path: %s is already mounted, device: %s", req.VolumeId, targetPath, deviceName)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	device := ""

	// Step 4 Attach volume
	if GlobalConfigVar.EnableAttachDetachController == "true" {
		device = getVolumeDeviceName(req.GetVolumeId())
		if err != nil {
			klog.Errorf("NodeStageVolume: ADController Enabled, but device can't be found in node: %s, error: %s", req.VolumeId, err.Error())
			return nil, status.Error(codes.Aborted, "NodeStageVolume: ADController Enabled, but device can't be found:"+req.VolumeId+err.Error())
		}
	} else {
		device, err = attachDisk(req.GetVolumeId(), GlobalConfigVar.InstanceID)
		if err != nil {
			fullErrorMessage := utils.FindSuggestionByErrorMessage(err.Error(), utils.DiskAttachDetach)
			klog.Errorf("NodeStageVolume: Attach volume: %s with error: %s", req.VolumeId, fullErrorMessage)
			return nil, status.Errorf(codes.Aborted, "NodeStageVolume: Attach volume: %s with error: %+v", req.VolumeId, err)
		}
	}

	if err := disk.CheckDeviceAvailable(device, req.VolumeId, targetPath); err != nil {
		klog.Errorf("NodeStageVolume: check device %s for volume %s with error: %s", device, req.VolumeId, err.Error())
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := saveVolumeConfig(req.VolumeId, device); err != nil {
		klog.Errorf("NodeStageVolume: saveVolumeConfig %s for volume %s with error: %s", device, req.VolumeId, err.Error())
		return nil, status.Error(codes.Aborted, "NodeStageVolume: saveVolumeConfig for ("+req.VolumeId+device+") error with: "+err.Error())
	}
	klog.Infof("NodeStageVolume: Volume Successful Attached: %s, to Node: %s, Device: %s", req.VolumeId, GlobalConfigVar.InstanceID, device)

	// sysConfig
	if value, ok := req.VolumeContext[SYS_CONFIG_TAG]; ok {
		configList := strings.Split(strings.TrimSpace(value), ",")
		for _, configStr := range configList {
			key, value, found := strings.Cut(configStr, "=")
			if !found {
				klog.Errorf("NodeStageVolume: Volume Block System Config with format error: %s", configStr)
				return nil, status.Error(codes.Aborted, "NodeStageVolume: Volume Block System Config with format error "+configStr)
			}
			base := fmt.Sprintf("/sys/block/%s/", filepath.Base(device))
			fileName := filepath.Clean(base + key)
			if !strings.HasPrefix(fileName, base) {
				// Note this cannot prevent user from access other device through e.g. /sys/block/vda/subsystem/vdb
				return nil, status.Errorf(codes.Aborted, "NodeStageVolume: invalid relative path in sysConfig: %s", key)
			}
			err := utilsio.WriteTrunc(unix.AT_FDCWD, fileName, []byte(value))
			if err != nil {
				return nil, status.Errorf(codes.Internal,
					"NodeStageVolume: Volume Block System Config failed, failed to write %s to %s: %v", value, fileName, err)
			}
			klog.Infof("NodeStageVolume: set sysConfig %s=%s", key, value)
		}
	}

	// Block volume not need to format
	if isBlock {
		notMounted, err := ns.k8smounter.IsLikelyNotMountPoint(targetPath)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to check if %s is a mount point: %v", targetPath, err)
		}
		if !notMounted {
			klog.Infof("NodeStageVolume: Block Already Mounted: volumeId: %s with target %s", req.VolumeId, targetPath)
			return &csi.NodeStageVolumeResponse{}, nil
		}
		options := []string{"bind"}
		if err := ns.mounter.MountBlock(device, targetPath, options...); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		klog.Infof("NodeStageVolume: Successfully Mount Device %s to %s with options: %v", device, targetPath, options)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	// Step 5 Start to format
	mnt := req.VolumeCapability.GetMount()
	options := append(mnt.MountFlags, "shared")
	fsType := "ext4"
	if mnt.FsType != "" {
		fsType = mnt.FsType
	}
	mountOptions := collectMountOptions(fsType, options)
	if err := os.MkdirAll(targetPath, 0755); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Set mkfs options for ext3, ext4
	mkfsOptions := make([]string, 0)
	if value, ok := req.VolumeContext[MKFS_OPTIONS]; ok {
		mkfsOptions = strings.Split(value, " ")
	}

	// do format-mount or mount
	diskMounter := &k8smount.SafeFormatAndMount{Interface: ns.k8smounter, Exec: utilexec.New()}
	if len(mkfsOptions) > 0 && (fsType == "ext4" || fsType == "ext3") {
		if err := utils.FormatAndMount(diskMounter, device, targetPath, fsType, mkfsOptions, mountOptions, false); err != nil {
			klog.Errorf("Mountdevice: FormatAndMount fail with mkfsOptions %s, %s, %s, %s, %s with error: %s", device, targetPath, fsType, mkfsOptions, mountOptions, err.Error())
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else {
		if err := diskMounter.FormatAndMount(device, targetPath, fsType, mountOptions); err != nil {
			klog.Errorf("NodeStageVolume: Volume: %s, Device: %s, FormatAndMount error: %s", req.VolumeId, device, err.Error())
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	klog.Infof("NodeStageVolume: Mount Successful: volumeId: %s target %v, device: %s, mkfsOptions: %v, options: %v", req.VolumeId, targetPath, device, mkfsOptions, mountOptions)
	return &csi.NodeStageVolumeResponse{}, nil
}

// target format: /var/lib/kubelet/plugins/kubernetes.io/csi/pv/pv-disk-1e7001e0-c54a-11e9-8f89-00163e0e78a0/globalmount
func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	klog.Infof("NodeUnstageVolume:: Starting to Unmount volume, volumeId: %s, target: %v", req.VolumeId, req.StagingTargetPath)

	// check block device mountpoint
	targetPath := req.GetStagingTargetPath()
	tmpPath := filepath.Join(req.GetStagingTargetPath(), req.VolumeId)
	if utils.IsFileExisting(tmpPath) {
		fileInfo, err := os.Lstat(tmpPath)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), INPUT_OUTPUT_ERR) {
				if err = utils.IsPathAvailable(targetPath); err != nil {
					if err = ns.k8smounter.Unmount(targetPath); err != nil {
						klog.Errorf("NodeUnstageVolume: umount target %s(input/output error) with error: %v", targetPath, err)
						return nil, status.Errorf(codes.InvalidArgument, "NodeUnstageVolume umount target %s with error: %v", targetPath, err)
					}
					klog.Warningf("NodeUnstageVolume: target path %s show input/output error: %v, umount it.", targetPath, err)
				}
			} else {
				klog.Errorf("NodeUnstageVolume: lstat mountpoint: %s with error: %s", tmpPath, err.Error())
				return nil, status.Error(codes.InvalidArgument, "NodeUnstageVolume: stat mountpoint error: "+err.Error())
			}
		} else if (fileInfo.Mode() & os.ModeDevice) != 0 {
			klog.Infof("NodeUnstageVolume: mountpoint %s, is block device", tmpPath)
			targetPath = tmpPath
		}
	}

	// Step 1: check folder exists and umount
	msgLog := ""
	if utils.IsFileExisting(targetPath) {
		notmounted, err := ns.k8smounter.IsLikelyNotMountPoint(targetPath)
		if err != nil {
			klog.Errorf("NodeUnstageVolume: VolumeId: %s, check mountPoint: %s error: %v", req.VolumeId, targetPath, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
		if !notmounted {
			err = ns.k8smounter.Unmount(targetPath)
			if err != nil {
				klog.Errorf("NodeUnstageVolume: VolumeId: %s, umount path: %s failed with: %v", req.VolumeId, targetPath, err)
				return nil, status.Error(codes.Internal, err.Error())
			}
			notMounted, err := ns.k8smounter.IsLikelyNotMountPoint(targetPath)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to check if %s is a mount point after umount: %v", targetPath, err)
			}
			if !notMounted {
				klog.Errorf("NodeUnstageVolume: TargetPath mounted yet, volumeId: %s, Target: %s", req.VolumeId, targetPath)
				return nil, status.Error(codes.Internal, "NodeUnstageVolume: TargetPath mounted yet with target"+targetPath)
			}
		} else {
			msgLog = fmt.Sprintf("NodeUnstageVolume: VolumeId: %s, mountpoint: %s not mounted, skipping and continue to detach", req.VolumeId, targetPath)
		}
	} else {
		msgLog = fmt.Sprintf("NodeUnstageVolume: VolumeId: %s, Path %s doesn't exist, continue to detach", req.VolumeId, targetPath)
	}

	if msgLog == "" {
		klog.Infof("NodeUnstageVolume: Unmount TargetPath successful, target %v, volumeId: %s", targetPath, req.VolumeId)
	} else {
		klog.Info(msgLog)
	}

	// Do detach if ADController disable
	if GlobalConfigVar.EnableAttachDetachController == "false" {
		// if DetachDisabled is set to true, return
		// if GlobalConfigVar.DetachDisabled {
		// klog.Infof("NodeUnstageVolume: ADController is Disable, Detach Flag Set to false, PV %s", req.VolumeId)
		// 	return &csi.NodeUnstageVolumeResponse{}, nil
		// }
		err := detachDisk(req.VolumeId, GlobalConfigVar.InstanceID)
		if err != nil {
			klog.Errorf("NodeUnstageVolume: VolumeId: %s, Detach failed with error %v", req.VolumeId, err.Error())
			return nil, err
		}
		_ = removeVolumeConfig(req.VolumeId)
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId:            GlobalConfigVar.InstanceID,
		MaxVolumesPerNode: ns.MaxVolumePerNode,

		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{
				TopologyRegionKey: GlobalConfigVar.RegionID,
			},
		},
	}, nil
}

func (ns *nodeServer) mountDeviceToGlobal(capability *csi.VolumeCapability, volumeContext map[string]string, device, sourcePath string) error {
	mnt := capability.GetMount()
	options := append(mnt.MountFlags, "shared")
	fsType := "ext4"
	if mnt.FsType != "" {
		fsType = mnt.FsType
	}
	mountOptions := collectMountOptions(fsType, options)
	if err := os.MkdirAll(sourcePath, 0755); err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	// Set mkfs options for ext3, ext4
	mkfsOptions := make([]string, 0)
	if value, ok := volumeContext[MKFS_OPTIONS]; ok {
		mkfsOptions = strings.Split(value, " ")
	}

	// do format-mount or mount
	diskMounter := &k8smount.SafeFormatAndMount{Interface: ns.k8smounter, Exec: utilexec.New()}
	if len(mkfsOptions) > 0 && (fsType == "ext4" || fsType == "ext3") {
		if err := utils.FormatAndMount(diskMounter, device, sourcePath, fsType, mkfsOptions, mountOptions, false); err != nil {
			klog.Errorf("mountDeviceToGlobal: FormatAndMount fail with mkfsOptions %s, %s, %s, %s, %s with error: %s", device, sourcePath, fsType, mkfsOptions, mountOptions, err.Error())
			return status.Error(codes.Internal, err.Error())
		}
	} else {
		if err := diskMounter.FormatAndMount(device, sourcePath, fsType, mountOptions); err != nil {
			klog.Errorf("mountDeviceToGlobal: Device: %s, FormatAndMount error: %s", device, err.Error())
			return status.Error(codes.Internal, err.Error())
		}
	}
	return nil
}

// hasMountOption return boolean value indicating whether the slice contains a mount option
func hasMountOption(options []string, opt string) bool {
	for _, o := range options {
		if o == opt {
			return true
		}
	}
	return false
}

// collectMountOptions returns array of mount options
func collectMountOptions(fsType string, mntFlags []string) (options []string) {
	for _, opt := range mntFlags {
		if !hasMountOption(options, opt) {
			options = append(options, opt)
		}
	}

	if fsType == "xfs" {
		if !hasMountOption(options, NOUUID) {
			options = append(options, NOUUID)
		}
	}
	return

}

func (ns *nodeServer) unmountDuplicateMountPoint(targetPath string) error {
	pathParts := strings.Split(targetPath, "/")
	partsLen := len(pathParts)
	if partsLen > 2 && pathParts[partsLen-1] == "mount" {
		globalPath2 := filepath.Join("/var/lib/container/kubelet/plugins/kubernetes.io/csi/pv/", pathParts[partsLen-2], "/globalmount")
		if utils.IsFileExisting(globalPath2) {
			// check globalPath2 is mountpoint
			notmounted, err := ns.k8smounter.IsLikelyNotMountPoint(globalPath2)
			if err == nil && !notmounted {
				// check device is used by others
				refs, err := ns.k8smounter.GetMountRefs(globalPath2)
				if err == nil && !ns.mounter.HasMountRefs(globalPath2, refs) {
					klog.Infof("NodeUnpublishVolume: VolumeId Unmount global path %s for ack with kubelet data disk", globalPath2)
					if err := ns.k8smounter.Unmount(globalPath2); err != nil {
						klog.Errorf("NodeUnpublishVolume: volumeId: unmount global path %s failed with err: %v", globalPath2, err)
						return status.Error(codes.Internal, err.Error())
					}
				} else {
					klog.Infof("Global Path %s is mounted by others: %v", globalPath2, refs)
				}
			} else {
				klog.Warningf("Global Path is not mounted: %s", globalPath2)
			}
		}
	} else {
		klog.Warningf("Target Path is illegal format: %s", targetPath)
	}
	return nil
}

// save diskID and volume name
func saveVolumeConfig(volumeID, devicePath string) error {
	if err := utils.CreateDest(VolumeDir); err != nil {
		return err
	}
	if err := utils.CreateDest(VolumeDirRemove); err != nil {
		return err
	}
	if err := removeVolumeConfig(volumeID); err != nil {
		return err
	}

	volumeFile := path.Join(VolumeDir, volumeID+".conf")
	if err := os.WriteFile(volumeFile, []byte(devicePath), 0644); err != nil {
		return err
	}
	return nil
}

// move config file to remove dir
func removeVolumeConfig(volumeID string) error {
	volumeFile := path.Join(VolumeDir, volumeID+".conf")
	if utils.IsFileExisting(volumeFile) {
		timeStr := time.Now().Format("2006-01-02-15:04:05")
		removeFile := path.Join(VolumeDirRemove, volumeID+"-"+timeStr+".conf")
		if err := os.Rename(volumeFile, removeFile); err != nil {
			return err
		}
	}
	return nil
}

func (ns *nodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	klog.Infof("NodeExpandVolume: node expand volume not support: %v", req)
	return &csi.NodeExpandVolumeResponse{}, nil
}
