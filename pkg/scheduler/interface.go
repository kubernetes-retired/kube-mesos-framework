/*
Copyright 2017 The Kubernetes Authors.

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

package scheduler

import (
	"k8s.io/client-go/1.5/pkg/api/v1"
)

// FailedNodesMap represents the filtered out nodes, with node names and failure messages
type FailedNodesMap map[string]string

// HostPriority represents the priority of scheduling to a particular host, higher priority is better.
type HostPriority struct {
	// Name of the host
	Host string
	// Score associated with the host
	Score int
}

type HostPriorityList []HostPriority

// SchedulerExtender is an interface for external processes to influence scheduling
// decisions made by Kubernetes. This is typically needed for resources not directly
// managed by Kubernetes.
type SchedulerExtender interface {
	// Filter based on extender-implemented predicate functions. The filtered list is
	// expected to be a subset of the supplied list. failedNodesMap optionally contains
	// the list of failed nodes and failure reasons.
	Filter(pod *v1.Pod, nodes []*v1.Node) (filteredNodes []*v1.Node, failedNodesMap FailedNodesMap, err error)

	// Prioritize based on extender-implemented priority functions. The returned scores & weight
	// are used to compute the weighted score for an extender. The weighted scores are added to
	// the scores computed  by Kubernetes scheduler. The total scores are used to do the host selection.
	Prioritize(pod *v1.Pod, nodes []*v1.Node) (hostPriorities *HostPriorityList, weight int, err error)
}

type Binder interface {
	Bind(pod *v1.Pod, host string) error
}
