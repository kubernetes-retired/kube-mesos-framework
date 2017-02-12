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

package options

import (

	"github.com/spf13/pflag"
	"fmt"
)

const (
	DefaultMesosMaster = "localhost:5050"
	DefaultMesosUser = "root"
	DefaultFrameworkRoles = "*"
)

type SchedulerServer struct {
	ApiServerList []string
	KubeConfig    string
	Address       string
	Port          int32

	MesosMaster    string
	MesosUser      string
	FrameworkRoles []string
	FrameworkName  string

	ExecutorURI string
}

// NewSchedulerServer creates a new SchedulerServer with default parameters
func NewSchedulerServer() *SchedulerServer {
	return &SchedulerServer{}
}

func (s *SchedulerServer) ValidateFlags() error {
	if s.ApiServerList == nil || len (s.ApiServerList) == 0 {
		return fmt.Errorf("api-servers can not be empty.")
	}
	return nil
}

func (s *SchedulerServer) AddFlags(fs *pflag.FlagSet) {
	fs.Int32Var(&s.Port, "port", s.Port, "The port that the scheduler's http service runs on")
	fs.StringVar(&s.Address, "address", s.Address, "The IP address to serve on (set to 0.0.0.0 for all interfaces)")
	fs.StringSliceVar(&s.ApiServerList, "api-servers", nil, "List of Kubernetes API servers for publishing events, and reading pods and services. (ip:port), comma separated.")
	fs.StringVar(&s.KubeConfig, "kubeconfig", s.KubeConfig, "Path to kubeconfig file with authorization and master location information used by the scheduler.")
	fs.StringVar(&s.MesosMaster, "mesos-master", DefaultMesosMaster, "Location of the Mesos master. The format is a comma-delimited list of of hosts like zk://host1:port,host2:port/mesos. If using ZooKeeper, pay particular attention to the leading zk:// and trailing /mesos! If not using ZooKeeper, standard URLs like http://localhost are also acceptable.")
	fs.StringVar(&s.MesosUser, "mesos-user", DefaultMesosUser, "Mesos user for this framework, defaults to root.")
	fs.StringSliceVar(&s.FrameworkRoles, "mesos-framework-roles", []string{DefaultFrameworkRoles}, "Mesos framework roles that the scheduler receives offers for. Currently only \"*\" and optionally one additional role are supported.")
	fs.StringVar(&s.ExecutorURI, "executor-uri", s.ExecutorURI, "The URI of executor.")
}
