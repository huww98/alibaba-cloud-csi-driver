package metadata

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/kubernetes-sigs/alibaba-cloud-csi-driver/pkg/cloud"
	"github.com/sirupsen/logrus"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

type MetadataKey int

const (
	RegionID     MetadataKey = iota
	ZoneID       MetadataKey = iota
	InstanceID   MetadataKey = iota
	InstanceType MetadataKey = iota
	Runtime      MetadataKey = iota
)

// Possible values for Runtime
const (
	MixRuntimeMode  = "runc-runv"
	RuncRuntimeMode = "runc"
)

func (k MetadataKey) String() string {
	switch k {
	case RegionID:
		return "RegionID"
	case ZoneID:
		return "ZoneID"
	case InstanceID:
		return "InstanceID"
	case InstanceType:
		return "InstanceType"
	default:
		return fmt.Sprintf("MetadataKey(%d)", k)
	}
}

var ErrUnknownMetadataKey = errors.New("unknown metadata key")

const DISABLE_ECS_ENV = "ALIBABA_CLOUD_NO_ECS_METADATA"
const KUBE_NODE_NAME_ENV = "KUBE_NODE_NAME"

type MetadataProvider interface {
	Get(key MetadataKey) (string, error)
}

type Metadata struct {
	providers []MetadataProvider
}

type MetadataFetcher interface {
	FetchFor(key MetadataKey) (MetadataProvider, error)
}

type lazyInitProvider struct {
	provider MetadataProvider
	err      error
	initMu   sync.Mutex
	fetcher  MetadataFetcher
}

func (p *lazyInitProvider) Get(key MetadataKey) (string, error) {
	if p.provider == nil && p.err == nil {
		p.initMu.Lock()
		if p.provider == nil && p.err == nil {
			provider, err := p.fetcher.FetchFor(key)
			if err == ErrUnknownMetadataKey {
				p.initMu.Unlock()
				return "", err
			}
			if err != nil {
				err = fmt.Errorf("%T failed: %w", p.fetcher, err)
				// print a warning if we failed to get a value,
				// because the error is hide if other providers succeed
				logrus.Warn(err)
			}
			p.fetcher = nil
			p.provider = provider
			p.err = err
		}
		p.initMu.Unlock()
	}
	if p.err != nil {
		return "", p.err
	}
	return p.provider.Get(key)
}

type immutableProvider struct {
	provider MetadataProvider
	name     string
	mu       sync.Mutex
	values   map[MetadataKey]string
}

func (p *immutableProvider) Get(key MetadataKey) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if v, ok := p.values[key]; ok {
		return v, nil
	}
	v, err := p.provider.Get(key)
	if err != nil {
		return "", err
	}
	logrus.WithFields(logrus.Fields{
		"provider": p.name,
		"key":      key,
		"value":    v,
	}).Info("retrive metadata")
	p.values[key] = v
	return v, nil
}

func newImmutableProvider(provider MetadataProvider, name string) *immutableProvider {
	return &immutableProvider{
		provider: provider,
		name:     name,
		values:   map[MetadataKey]string{},
	}
}

func NewMetadata() *Metadata {
	return &Metadata{
		providers: []MetadataProvider{
			newImmutableProvider(&ENVMetadata{}, "env"),
		},
	}
}

func (m *Metadata) EnableEcs(httpRT http.RoundTripper) {
	if os.Getenv(DISABLE_ECS_ENV) != "" {
		logrus.Infof("ECS metadata is disabled by environment variable %s", DISABLE_ECS_ENV)
		return
	}
	m.providers = append(m.providers, &lazyInitProvider{
		fetcher: &EcsFetcher{httpRT: httpRT},
	})

}

func (m *Metadata) EnableKubernetes(nodeClient corev1.NodeInterface) {
	nodeName := os.Getenv(KUBE_NODE_NAME_ENV)
	if nodeName == "" {
		logrus.Warnf("%s environment variable is not set, skipping Kubernetes metadata", KUBE_NODE_NAME_ENV)
		return
	}
	m.providers = append(m.providers, &lazyInitProvider{
		fetcher: &KubernetesMetadataFetcher{
			client:   nodeClient,
			nodeName: nodeName,
		},
	})
}

func (m *Metadata) EnableOpenAPI(ecsClient cloud.ECSInterface) {
	mPre := Metadata{
		// use the previous providers to get region id and instance id,
		// do not recurse into ourselves
		providers: m.providers,
	}
	m.providers = append(m.providers, &lazyInitProvider{
		fetcher: &OpenAPIFetcher{
			client: ecsClient,
			mPre:   &mPre,
		},
	})
}

func (m *Metadata) Get(key MetadataKey) (string, error) {
	errors := []error{}
	for _, p := range m.providers {
		v, err := p.Get(key)
		if err == nil {
			return v, nil
		}
		if err == ErrUnknownMetadataKey {
			continue
		}
		errors = append(errors, err)
	}
	if len(errors) == 0 {
		return "", ErrUnknownMetadataKey
	}
	return "", utilerrors.NewAggregate(errors)
}

func MustGet(m MetadataProvider, key MetadataKey) string {
	value, err := m.Get(key)
	if err != nil {
		panic(err)
	}
	return value
}
