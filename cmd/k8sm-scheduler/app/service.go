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

package app

import (
	"github.com/kubernetes-incubator/kube-mesos-framework/cmd/k8sm-scheduler/app/options"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler"

	"k8s.io/client-go/1.5/kubernetes"
	"k8s.io/client-go/1.5/tools/clientcmd"
)

func Run(s *options.SchedulerServer) error {
	kubeConfig, err := clientcmd.BuildConfigFromFlags(s.ApiServerList[0], s.KubeConfig)
	if err != nil {
		return err
	}
	// Build KubeClient
	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return err
	}

	// Start MesosFramework
	framework := scheduler.NewFramework(&scheduler.Config{
		KubeClient: kubeClient,
	})

	framework.Start()

	return nil
}
