package metric

import (
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils"
	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/utils/mountinfo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var (
	vgFreeBytesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(nodeNamespace, volumeSubSystem, "vg_free_bytes"),
		"Total amount of free bytes in LVM volume group",
		[]string{"vg_name"}, nil,
	)
	vgSizeBytesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(nodeNamespace, volumeSubSystem, "vg_size_bytes"),
		"Total size of LVM volume group in bytes",
		[]string{"vg_name"}, nil,
	)
)

func init() {
	registerCollector("local_stat", NewLocalStatCollector)
}

type localStatCollector struct {
}

type localVolumeInfo struct {
	pvName, source     string
	devMajor, devMinor int
}

type lvsOutput struct {
	Report []struct {
		LV []struct {
			VgName string `json:"vg_name"`
			VgFree string `json:"vg_free"`
			VgSize string `json:"vg_size"`
		} `json:"lv"`
	} `json:"report"`
}

type vgSize struct {
	free, size uint64
}

var ErrNotLV = errors.New("not a LVM logical volume")

func getVgSize(lv string) (string, vgSize, error) {
	lvs_out_bytes, err := utils.CommandOnNode("lvs", lv, "-o", "vg_name,vg_free,vg_size", "--reportformat", "json", "--units", "b", "--nosuffix").Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 5 {
			err = ErrNotLV
		}
		return "", vgSize{}, fmt.Errorf("lvs failed for LV %s, err: %w", lv, err)
	}
	var lvs_out lvsOutput
	err = json.Unmarshal(lvs_out_bytes, &lvs_out)
	if err != nil {
		return "", vgSize{}, err
	}

	r := lvs_out.Report[0].LV[0]
	size := vgSize{}
	size.size, err = strconv.ParseUint(r.VgSize, 10, 64)
	if err != nil {
		return "", size, err
	}
	size.free, err = strconv.ParseUint(r.VgFree, 10, 64)
	if err != nil {
		return "", size, err
	}

	return r.VgName, size, err
}

func (*localStatCollector) Update(ch chan<- prometheus.Metric) error {
	volJSONPaths, err := findVolJSON(podsRootPath)
	if err != nil {
		return err
	}

	candidateMnt := map[string]*localVolumeInfo{}
	for _, path := range volJSONPaths {
		//Get disk pvName
		pvName, _, err := getVolumeInfoByJSON(path, localDriverName)
		if err != nil {
			if err != ErrUnexpectedVolumeType {
				logrus.Errorf("Get volume info by path %s is failed, err:%s", path, err)
			}
			continue
		}
		candidateMnt[filepath.Join(filepath.Dir(path), "mount")] = &localVolumeInfo{
			pvName: pvName,
		}
	}

	mountinfo.GetMounts(func(i *mountinfo.Info) (skip bool, stop bool) {
		volumeInfo, ok := candidateMnt[i.Mountpoint]
		if !ok {
			return true, false
		}
		volumeInfo.devMajor = i.Major
		volumeInfo.devMinor = i.Minor
		volumeInfo.source = i.Source
		return true, false
	})

	vgSizes := map[string]vgSize{}
	for mnt, i := range candidateMnt {
		if strings.HasPrefix(i.source, "/dev/") {
			vgName, vgSize, err := getVgSize(i.source)
			if err != nil {
				if !errors.Is(err, ErrNotLV) {
					logrus.Errorf("Error while getting VG info for mountpoint %s: %v", mnt, err)
					continue
				}
			} else {
				vgSizes[vgName] = vgSize
			}
		}
		// TODO: support for other local volume type
	}

	for vgName, size := range vgSizes {
		ch <- prometheus.MustNewConstMetric(vgSizeBytesDesc, prometheus.GaugeValue, float64(size.size), vgName)
		ch <- prometheus.MustNewConstMetric(vgFreeBytesDesc, prometheus.GaugeValue, float64(size.free), vgName)
	}

	return nil
}

func NewLocalStatCollector() (Collector, error) {
	return &localStatCollector{}, nil
}
