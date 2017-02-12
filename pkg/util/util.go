package util

import (
	"math"

	"github.com/gogo/protobuf/proto"
	"github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/mesosutil"

	"k8s.io/client-go/1.5/pkg/api/v1"
)

const (
	DefaultFrameworkName = "Kubernetes"
	DefaultExecutorCPUs  = 0.25  // initial CPU allocated for executor
	DefaultExecutorMem   = 128.0 // initial memory allocated for executor
	DefaultExecutorID    = "k8sm-executor"
	DefaultExecutorCMD   = "./k8sm-executor"
	DefaultExecutorName  = "k8sm-executor"
)

func IsGreater(ls, rs []*mesosproto.Resource) bool {
	for _, l := range ls {
		for _, r := range rs {
			if l.Name == r.Name && *r.Type == mesosproto.Value_SCALAR {
				lv := *l.Scalar.Value
				rv := *r.Scalar.Value
				if !(lv > rv || math.Abs(lv-rv) < 0.01) {
					return false
				}
			}
		}
	}

	return true
}

func GetPodResourceRequest(pod *v1.Pod) []*mesosproto.Resource {
	var cpu, mem int64
	for _, container := range pod.Spec.Containers {
		for rName, rQuantity := range container.Resources.Requests {
			switch rName {
			case v1.ResourceMemory:
				mem += rQuantity.Value()
			case v1.ResourceCPU:
				cpu += rQuantity.MilliValue()
			}
		}
	}

	// take max_resource(sum_pod, any_init_container)
	for _, container := range pod.Spec.InitContainers {
		for rName, rQuantity := range container.Resources.Requests {
			switch rName {
			case v1.ResourceMemory:
				if m := rQuantity.Value(); m > mem {
					mem = m
				}
			case v1.ResourceCPU:
				if c := rQuantity.MilliValue(); c > cpu {
					cpu = c
				}
			}
		}
	}

	return []*mesosproto.Resource{
		mesosutil.NewScalarResource("cpu", float64(cpu)/1000),
		mesosutil.NewScalarResource("mem", float64(mem)),
	}
}

func BuildExecutor(uri string) *mesosproto.ExecutorInfo {
	// Create mesos scheduler driver.
	return &mesosproto.ExecutorInfo{
		ExecutorId: mesosutil.NewExecutorID(DefaultExecutorID),
		Name:       proto.String(DefaultExecutorName),
		Source:     proto.String(DefaultFrameworkName),
		Command: &mesosproto.CommandInfo{
			Value: proto.String(DefaultExecutorCMD),
			Uris:  []*mesosproto.CommandInfo_URI{{Value: &uri, Executable: proto.Bool(true)}},
		},
		Resources: []*mesosproto.Resource{
			mesosutil.NewScalarResource("cpus", DefaultExecutorCPUs),
			mesosutil.NewScalarResource("mem", DefaultExecutorMem),
		},
	}
}
