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
	"time"

	"github.com/mesos/mesos-go/mesosproto"
	mutil "github.com/mesos/mesos-go/mesosutil"
	bindings "github.com/mesos/mesos-go/scheduler"

	"k8s.io/client-go/1.5/kubernetes"
	"k8s.io/client-go/1.5/pkg/api"
	"k8s.io/client-go/1.5/pkg/api/v1"
	"k8s.io/client-go/1.5/pkg/fields"
	"k8s.io/client-go/1.5/pkg/runtime"
	"k8s.io/client-go/1.5/pkg/watch"
	"k8s.io/client-go/1.5/tools/cache"
)

type Operation int

const (
	DELETE Operation = iota
	RECONCILE
)

type Event struct {
	Action Operation
	TaskID *mesosproto.TaskID
}

// Reconciler will monitor Pod in k8s, and then sync up with
// Mesos
type Reconciler interface {
	// Handle the event from Mesos, e.g. StatusUpdate, SlaveLost
	Handle(event *Event)

	Run(stop chan struct{})
}

type reconciler struct {
	driver    bindings.SchedulerDriver
	clientset *kubernetes.Clientset
	eventChan chan Event
}

func NewReconciler(driver bindings.SchedulerDriver, cs *kubernetes.Clientset) Reconciler {
	return &reconciler{
		driver:    driver,
		clientset: cs,
	}
}

func (r *reconciler) Run(stop chan struct{}) {
	// Watching Pending & Running Pods.
	selector := fields.ParseSelectorOrDie("status.phase!=" + string(v1.PodSucceeded) + ",status.phase!=" + string(v1.PodFailed))
	lw := cache.NewListWatchFromClient(r.clientset.CoreClient, "pods", v1.NamespaceAll, selector)

	podInformer := cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options api.ListOptions) (runtime.Object, error) {
				return lw.List(options)
			},
			WatchFunc: func(options api.ListOptions) (watch.Interface, error) {
				return lw.Watch(options)
			},
		},
		&v1.Pod{},
		1*time.Second,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)

	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			// ignore
		},
		UpdateFunc: func(old, obj interface{}) {
			// ignore
		},
		DeleteFunc: func(obj interface{}) {
			if pod, ok := obj.(*v1.Pod); ok {
				r.driver.KillTask(mutil.NewTaskID(string(pod.GetUID())))
			}
		},
	})

	podInformer.Run(stop)

	// Handle Mesos's request
	go func() {
		for event := range r.eventChan {
			switch event.Action {
			case DELETE:
				r.clientset.Pods("default").Delete(event.TaskID.GetValue(), nil)
			}
		}
	}()
}

func (r *reconciler) Handle(event *Event) {
	r.eventChan <- *event
}
