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

	"k8s.io/client-go/1.5/pkg/api/meta"
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

func (s *SchedulerServer) AddFlags(fs *pflag.FlagSet) {
	fs.IntVar(&s.port, "port", s.port, "The port that the scheduler's http service runs on")
	fs.IPVar(&s.address, "address", s.address, "The IP address to serve on (set to 0.0.0.0 for all interfaces)")
	fs.BoolVar(&s.enableProfiling, "profiling", s.enableProfiling, "Enable profiling via web interface host:port/debug/pprof/")
	fs.StringSliceVar(&s.apiServerList, "api-servers", s.apiServerList, "List of Kubernetes API servers for publishing events, and reading pods and services. (ip:port), comma separated.")
	fs.StringVar(&s.kubeconfig, "kubeconfig", s.kubeconfig, "Path to kubeconfig file with authorization and master location information used by the scheduler.")
	fs.Float32Var(&s.kubeAPIQPS, "kube-api-qps", s.kubeAPIQPS, "QPS to use while talking with kubernetes apiserver")
	fs.IntVar(&s.kubeAPIBurst, "kube-api-burst", s.kubeAPIBurst, "Burst to use while talking with kubernetes apiserver")
	fs.StringSliceVar(&s.etcdServerList, "etcd-servers", s.etcdServerList, "List of etcd servers to watch (http://ip:port), comma separated.")
	fs.BoolVar(&s.allowPrivileged, "allow-privileged", s.allowPrivileged, "Enable privileged containers in the kubelet (compare the same flag in the apiserver).")
	fs.StringVar(&s.clusterDomain, "cluster-domain", s.clusterDomain, "Domain for this cluster.  If set, kubelet will configure all containers to search this domain in addition to the host's search domains")
	fs.IPVar(&s.clusterDNS, "cluster-dns", s.clusterDNS, "IP address for a cluster DNS server. If set, kubelet will configure all containers to use this for DNS resolution in addition to the host's DNS servers")
	fs.StringVar(&s.staticPodsConfigPath, "static-pods-config", s.staticPodsConfigPath, "Path for specification of static pods. Path should point to dir containing the staticPods configuration files. Defaults to none.")

	fs.StringVar(&s.mesosMaster, "mesos-master", s.mesosMaster, "Location of the Mesos master. The format is a comma-delimited list of of hosts like zk://host1:port,host2:port/mesos. If using ZooKeeper, pay particular attention to the leading zk:// and trailing /mesos! If not using ZooKeeper, standard URLs like http://localhost are also acceptable.")
	fs.StringVar(&s.mesosUser, "mesos-user", s.mesosUser, "Mesos user for this framework, defaults to root.")
	fs.StringSliceVar(&s.frameworkRoles, "mesos-framework-roles", s.frameworkRoles, "Mesos framework roles that the scheduler receives offers for. Currently only \"*\" and optionally one additional role are supported.")
	fs.StringSliceVar(&s.defaultPodRoles, "mesos-default-pod-roles", s.defaultPodRoles, "Roles that will be used to launch pods having no "+meta.RolesKey+" label.")
	fs.StringVar(&s.mesosAuthPrincipal, "mesos-authentication-principal", s.mesosAuthPrincipal, "Mesos authentication principal.")
}
