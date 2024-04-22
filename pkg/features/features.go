package features

import (
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/component-base/featuregate"
)

const (
	DiskADController   featuregate.Feature = "DiskADController"
	DBFSMetricByPlugin featuregate.Feature = "DBFSMetricByPlugin"
	DBFSADController   featuregate.Feature = "DBFSADController"
)

var (
	FunctionalMutableFeatureGate = featuregate.NewFeatureGate()
	defaultDiskFeatureGate       = map[featuregate.Feature]featuregate.FeatureSpec{
		DiskADController: {Default: true, PreRelease: featuregate.Beta},
	}
	defaultDBFSFeatureGate = map[featuregate.Feature]featuregate.FeatureSpec{
		DBFSMetricByPlugin: {Default: false, PreRelease: featuregate.Alpha},
		DBFSADController:   {Default: false, PreRelease: featuregate.Alpha},
	}
)

func init() {
	runtime.Must(FunctionalMutableFeatureGate.Add(defaultDiskFeatureGate))
	runtime.Must(FunctionalMutableFeatureGate.Add(defaultDBFSFeatureGate))
}
