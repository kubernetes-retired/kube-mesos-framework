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
	"fmt"
	"os"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/util"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"

	"k8s.io/client-go/1.5/kubernetes"
	"k8s.io/client-go/1.5/pkg/api/v1"
	"k8s.io/client-go/1.5/pkg/util/json"
)

type Framework interface {
	// Mesos Framework interfaces
	sched.Scheduler

	// k8s-scheduler HTTPExtender interfaces
	SchedulerExtender

	// bind Pod to hosts
	Binder

	Start()
}

type FrameworkStatus int

const (
	STAGING FrameworkStatus = iota
	RUNNING
	DISCONNECTED
)

type framework struct {
	mesosMaster   string
	mesosUser     string
	frameworkName string
	masterInfo    *mesos.MasterInfo
	driver        sched.SchedulerDriver // late initialization
	frameworkId   *mesos.FrameworkID
	status        FrameworkStatus
	executor      *mesos.ExecutorInfo
	offers        *Offers

	reconciler Reconciler
	clientset  *kubernetes.Clientset
	stop       chan struct{}
}

type Config struct {
	MesosMaster   string
	MesosUser     string
	FrameworkName string
	ExecutorURI   string
	KubeClient    *kubernetes.Clientset
}

// New creates a new Framework
func NewFramework(conf *Config) Framework {
	return &framework{
		status:        STAGING,
		offers:        &Offers{},
		clientset:     conf.KubeClient,
		stop:          make(chan struct{}),
		executor:      util.BuildExecutor(conf.ExecutorURI),
		mesosMaster:   conf.MesosMaster,
		mesosUser:     conf.MesosUser,
		frameworkName: conf.FrameworkName,
	}
}

func (k *framework) Start() {
	config := sched.DriverConfig{
		Scheduler: k,
		Framework: &mesos.FrameworkInfo{
			User: proto.String(k.mesosUser), // Mesos-go will fill in user.
			Name: proto.String(k.frameworkName),
		},
		Master: k.mesosMaster,
	}

	driver, err := sched.NewMesosSchedulerDriver(config)
	if err != nil {
		glog.Errorln("Unable to create a SchedulerDriver ", err.Error())
	}

	if stat, err := driver.Run(); err != nil {
		glog.Infof("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
		time.Sleep(2 * time.Second)
		os.Exit(1)
	}

	glog.Infof("framework terminating")
}

func (k *framework) Bind(pod *v1.Pod, host string) error {
	offers := k.offers.Filter(func(o *mesos.Offer) bool {
		return *o.Hostname == host
	})

	request := util.GetPodResourceRequest(pod)

	for _, offer := range offers {
		if util.IsGreater(offer.GetResources(), request) {
			taskInfo := mesosutil.NewTaskInfo(
				util.BuildTaskName(pod),
				util.BuildTaskID(pod),
				offer.SlaveId,
				request,
			)

			taskInfo.Executor = k.executor

			var err error
			if taskInfo.Data, err = json.Marshal(pod); err != nil {
				return err
			}

			if _, err = k.driver.LaunchTasks([]*mesos.OfferID{offer.Id}, []*mesos.TaskInfo{taskInfo}, nil); err != nil {
				return err
			}
			return nil
		}
	}

	return fmt.Errorf("Did not found offer on host %v", host)
}

// Filter based on extender-implemented predicate functions. The filtered list is
// expected to be a subset of the supplied list. failedNodesMap optionally contains
// the list of failed nodes and failure reasons.
func (k *framework) Filter(pod *v1.Pod, nodes []*v1.Node) (filteredNodes []*v1.Node, failedNodesMap FailedNodesMap, err error) {
	return nodes, nil, nil
}

// Prioritize based on extender-implemented priority functions. The returned scores & weight
// are used to compute the weighted score for an extender. The weighted scores are added to
// the scores computed  by Kubernetes scheduler. The total scores are used to do the host selection.
func (k *framework) Prioritize(pod *v1.Pod, nodes []*v1.Node) (hostPriorities *HostPriorityList, weight int, err error) {
	return nil, 0, nil
}

// Registered is called when the scheduler registered with the master successfully.
func (k *framework) Registered(drv sched.SchedulerDriver, fid *mesos.FrameworkID, mi *mesos.MasterInfo) {
	glog.Infof("Scheduler registered with the master: %v with frameworkId: %v\n", mi, fid)

	k.driver = drv
	k.frameworkId = fid
	k.masterInfo = mi
	k.status = RUNNING

	k.executor.FrameworkId = k.frameworkId

	k.reconciler = NewReconciler(k.driver, k.clientset)
	k.reconciler.Run(k.stop)
}

// Reregistered is called when the scheduler re-registered with the master successfully.
// This happens when the master fails over.
func (k *framework) Reregistered(drv sched.SchedulerDriver, mi *mesos.MasterInfo) {
	glog.Infof("Scheduler reregistered with the master: %v\n", mi)

	k.driver = drv
	k.masterInfo = mi
	k.status = RUNNING

	k.executor.FrameworkId = k.frameworkId

	k.reconciler = NewReconciler(k.driver, k.clientset)
	k.reconciler.Run(k.stop)

	k.reconciler.Handle(&Event{
		Action: RECONCILE,
	})
}

// Disconnected is called when the scheduler loses connection to the master.
func (k *framework) Disconnected(driver sched.SchedulerDriver) {
	glog.Infof("Master disconnected!\n")

	k.status = DISCONNECTED
}

// ResourceOffers is called when the scheduler receives some offers from the master.
func (k *framework) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	glog.V(2).Infof("Received offers %+v", offers)

	k.offers.AddAll(offers)
}

// OfferRescinded is called when the resources are recinded from the scheduler.
func (k *framework) OfferRescinded(driver sched.SchedulerDriver, offerId *mesos.OfferID) {
	glog.Infof("Offer rescinded %v\n", offerId)

	k.offers.Delete(offerId)
}

// StatusUpdate is called when a status update message is sent to the scheduler.
func (k *framework) StatusUpdate(driver sched.SchedulerDriver, taskStatus *mesos.TaskStatus) {
	if *taskStatus.State == mesos.TaskState_TASK_LOST {
		k.reconciler.Handle(&Event{
			Action: DELETE,
			TaskID: taskStatus.TaskId,
		})
	}
}

// FrameworkMessage is called when the scheduler receives a message from the executor.
func (k *framework) FrameworkMessage(
	driver sched.SchedulerDriver,
	executorId *mesos.ExecutorID,
	slaveId *mesos.SlaveID,
	message string,
) {
	glog.Infof("Received messages from executor %v of slave %v, %v\n", executorId, slaveId, message)
}

// SlaveLost is called when some slave is lost.
func (k *framework) SlaveLost(driver sched.SchedulerDriver, slaveId *mesos.SlaveID) {
	glog.Infof("Slave %v is lost\n", slaveId)

	k.offers.DeleteBy(func(o *mesos.Offer) bool {
		return o.SlaveId.GetValue() == slaveId.GetValue()
	})

	// Handler in StatusUpdate
}

// ExecutorLost is called when some executor is lost.
func (k *framework) ExecutorLost(
	driver sched.SchedulerDriver,
	executorId *mesos.ExecutorID,
	slaveId *mesos.SlaveID,
	status int,
) {
	// TODO (k82cn): remove related tasks.
	glog.Infof("Executor %v of slave %v is lost, status: %v\n", executorId, slaveId, status)
}

// Error is called when there is an unrecoverable error in the scheduler or scheduler driver.
// The driver should have been aborted before this is invoked.
func (k *framework) Error(driver sched.SchedulerDriver, message string) {
	glog.Fatalf("fatal scheduler error: %v\n", message)
}
