/*
Copyright 2015 The Kubernetes Authors.

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

package executor

import (
	"time"

	"github.com/golang/glog"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/util"
	msched "github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
	mutil "github.com/mesos/mesos-go/mesosutil"

	"k8s.io/client-go/1.5/kubernetes"
	"k8s.io/client-go/1.5/pkg/api"
	"k8s.io/client-go/1.5/pkg/api/v1"
	"k8s.io/client-go/1.5/pkg/fields"
	"k8s.io/client-go/1.5/pkg/runtime"
	"k8s.io/client-go/1.5/pkg/util/json"
	"k8s.io/client-go/1.5/pkg/watch"
	"k8s.io/client-go/1.5/tools/cache"
)

// KubernetesExecutor is an mesos executor that runs pods
// in a minion machine.
type Executor struct {
	kubeClient *kubernetes.Clientset
	driver     msched.ExecutorDriver
	hostname   string
	stop       chan struct{}
}

// Option is a functional option type for Executor
type Option func(*Executor)

// New creates a new kubernetes executor.
func NewExecutor(kubeClient *kubernetes.Clientset) *Executor {
	return &Executor{
		kubeClient: kubeClient,
		hostname:   mutil.GetHostname(""),
		stop:       make(chan struct{}),
	}
}

func (k *Executor) Start() error {
	glog.Infoln("Starting k8sm-executor")
	k.startPodInformer()

	dconfig := msched.DriverConfig{
		Executor: k,
	}

	driver, err := msched.NewMesosExecutorDriver(dconfig)

	if err != nil {
		return err
	}

	_, err = driver.Start()
	if err != nil {
		return err
	}

	glog.Infoln("Executor process has started and running.")

	_, err = driver.Join()
	if err != nil {
		return err
	}
	glog.Infoln("executor terminating")

	return nil
}

// Registered is called when the executor is successfully registered with the slave.
func (k *Executor) Registered(
	driver msched.ExecutorDriver,
	executorInfo *mesos.ExecutorInfo,
	frameworkInfo *mesos.FrameworkInfo,
	slaveInfo *mesos.SlaveInfo,
) {
	k.driver = driver

	glog.Infof(
		"Executor %v of framework %v registered with slave %v\n",
		executorInfo, frameworkInfo, slaveInfo,
	)
}

// Reregistered is called when the executor is successfully re-registered with the slave.
// This can happen when the slave fails over.
func (k *Executor) Reregistered(driver msched.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	k.driver = driver
}

// Disconnected is called when the executor is disconnected from the slave.
func (k *Executor) Disconnected(driver msched.ExecutorDriver) {

}

// LaunchTask is called when the executor receives a request to launch a task.
// The happens when the k8sm scheduler has decided to schedule the pod
// (which corresponds to a Mesos Task) onto the node where this executor
// is running, but the binding is not recorded in the Kubernetes store yet.
// This function is invoked to tell the executor to record the binding in the
// Kubernetes store and start the pod via the Kubelet.
func (k *Executor) LaunchTask(driver msched.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	glog.Infof("Launch task %v\n", taskInfo)

	var pod *v1.Pod
	if err := json.Unmarshal(taskInfo.Data, pod); err != nil {
		k.sendStatus(taskInfo.TaskId, mesos.TaskState_TASK_FAILED, err.Error())
		return
	}

	b := &v1.Binding{
		ObjectMeta: v1.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name},
		Target: v1.ObjectReference{
			Kind: "Node",
			Name: k.hostname,
		},
	}

	glog.V(3).Infof("Attempting to bind %v to %v", b.Name, b.Target.Name)
	if err := k.kubeClient.Pods(b.Namespace).Bind(b); err != nil {
		k.sendStatus(taskInfo.TaskId, mesos.TaskState_TASK_FAILED, err.Error())
		return
	}

	k.sendStatus(taskInfo.TaskId, mesos.TaskState_TASK_STARTING, "")
}

// KillTask is called when the executor receives a request to kill a task.
func (k *Executor) KillTask(driver msched.ExecutorDriver, taskId *mesos.TaskID) {
	k.sendStatus(taskId, mesos.TaskState_TASK_KILLED, "")
}

// FrameworkMessage is called when the framework sends some message to the executor
func (k *Executor) FrameworkMessage(driver msched.ExecutorDriver, message string) {

}

// Shutdown is called when the executor receives a shutdown request.
func (k *Executor) Shutdown(driver msched.ExecutorDriver) {
	glog.Infoln("Stopping pod informer")
	k.stop <- struct{}{}
	glog.Infoln("Shutdown pod informer")

	glog.Infoln("Stopping executor driver")
	_, err := driver.Stop()
	if err != nil {
		glog.Warningf("failed to stop executor driver: %v", err)
	}

	glog.Infoln("Shutdown the executor")
}

// Error is called when some error happens.
func (k *Executor) Error(driver msched.ExecutorDriver, message string) {
	glog.Errorln(message)
}

// ----------------> Private Methods <----------------
func (k *Executor) sendStatus(taskId *mesos.TaskID, status mesos.TaskState, msg string) {
	statusUpdate := mutil.NewTaskStatus(taskId, mesos.TaskState_TASK_FAILED)
	statusUpdate.Message = &msg
	k.driver.SendStatusUpdate(statusUpdate)
}

func (k *Executor) startPodInformer() {
	// Watching all Pods assigned to this host.
	lw := cache.NewListWatchFromClient(
		k.kubeClient.CoreClient,
		"pods",
		v1.NamespaceAll,
		fields.OneTermEqualSelector(api.PodHostField, k.hostname),
	)

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
		UpdateFunc: func(old_, obj_ interface{}) {
			old, ok := old_.(*v1.Pod)
			if !ok {
				return
			}

			obj, ok := obj_.(*v1.Pod)
			if !ok {
				return
			}

			if old.Status.Phase != obj.Status.Phase {
				switch obj.Status.Phase {
				case v1.PodPending:
					k.sendStatus(util.BuildTaskID(obj), mesos.TaskState_TASK_STARTING, "")
				case v1.PodRunning:
					k.sendStatus(util.BuildTaskID(obj), mesos.TaskState_TASK_RUNNING, "")
				case v1.PodSucceeded:
					k.sendStatus(util.BuildTaskID(obj), mesos.TaskState_TASK_FINISHED, "")
				case v1.PodFailed:
					k.sendStatus(util.BuildTaskID(obj), mesos.TaskState_TASK_FINISHED, "")
				case v1.PodUnknown:
					glog.Warningf("The status of pod %v/%v is unkonwn, waiting for scheduler to correct it", obj.Namespace, obj.Name)
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			// ignore;
			//   1. if it is non-terminated, k8sm-scheduler will send kill task
			//   2. if it is terminated, Mesos had been updated when status changed
		},
	})

	podInformer.Run(k.stop)
}
