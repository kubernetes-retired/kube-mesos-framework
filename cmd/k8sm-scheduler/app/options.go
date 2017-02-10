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

package app

import (
	"time"
	"sync"
	"net"
	"net/http"
	"strings"
	"fmt"

	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler/resources"
	"github.com/spf13/pflag"

	"k8s.io/client-go/1.5/pkg/api/resource"
	"k8s.io/client-go/1.5/pkg/master/ports"
)

type SchedulerServer struct {
	port                  int
	address               net.IP
	enableProfiling       bool
	kubeconfig            string
	kubeAPIQPS            float32
	kubeAPIBurst          int
	apiServerList         []string
	etcdServerList        []string
	allowPrivileged       bool
	executorPath          string
	proxyPath             string
	mesosMaster           string
	mesosUser             string
	frameworkRoles        []string
	defaultPodRoles       []string
	mesosAuthPrincipal    string
	mesosAuthSecretFile   string
	mesosCgroupPrefix     string
	mesosExecutorCPUs     resources.CPUShares
	mesosExecutorMem      resources.MegaBytes
	checkpoint            bool
	failoverTimeout       float64
	generateTaskDiscovery bool
	frameworkStoreURI     string

	executorLogV                   int
	executorBindall                bool
	executorSuicideTimeout         time.Duration
	launchGracePeriod              time.Duration
	kubeletEnableDebuggingHandlers bool

	runProxy        bool
	proxyBindall    bool
	proxyKubeconfig string
	proxyLogV       int
	proxyMode       string

	minionPathOverride    string
	minionLogMaxSize      resource.Quantity
	minionLogMaxBackups   int
	minionLogMaxAgeInDays int

	mesosAuthProvider              string
	driverPort                     uint
	hostnameOverride               string
	reconcileInterval              int64
	reconcileCooldown              time.Duration
	defaultContainerCPULimit       resources.CPUShares
	defaultContainerMemLimit       resources.MegaBytes
	schedulerConfigFileName        string
	graceful                       bool
	frameworkName                  string
	frameworkWebURI                string
	ha                             bool
	advertisedAddress              string
	serviceAddress                 net.IP
	haDomain                       string
	kmPath                         string
	clusterDNS                     net.IP
	clusterDomain                  string
	kubeletApiServerList           []string
	kubeletRootDirectory           string
	kubeletDockerEndpoint          string
	kubeletPodInfraContainerImage  string
	kubeletCadvisorPort            uint
	kubeletHostNetworkSources      string
	kubeletSyncFrequency           time.Duration
	kubeletNetworkPluginName       string
	kubeletKubeconfig              string
	staticPodsConfigPath           string
	dockerCfgPath                  string
	containPodResources            bool
	nodeRelistPeriod               time.Duration
	sandboxOverlay                 string
	conntrackMax                   int
	conntrackMaxPerCore            int
	conntrackTCPTimeoutEstablished int
	useHostPortEndpoints           bool

	executable  string // path to the binary running this service
	client      *clientset.Clientset
	driver      bindings.SchedulerDriver
	driverMutex sync.RWMutex
	mux         *http.ServeMux
}


// NewSchedulerServer creates a new SchedulerServer with default parameters
func NewSchedulerServer() *SchedulerServer {
	s := SchedulerServer{
		port:              ports.SchedulerPort,
		address:           net.ParseIP("127.0.0.1"),
		failoverTimeout:   time.Duration((1 << 62) - 1).Seconds(),
		frameworkStoreURI: "etcd://",
		kubeAPIQPS:        50.0,
		kubeAPIBurst:      100,

		runProxy:                 true,
		executorSuicideTimeout:   execcfg.DefaultSuicideTimeout,
		launchGracePeriod:        execcfg.DefaultLaunchGracePeriod,
		defaultContainerCPULimit: resources.DefaultDefaultContainerCPULimit,
		defaultContainerMemLimit: resources.DefaultDefaultContainerMemLimit,

		proxyMode: "userspace", // upstream default is "iptables" post-v1.1

		minionLogMaxSize:      minioncfg.DefaultLogMaxSize(),
		minionLogMaxBackups:   minioncfg.DefaultLogMaxBackups,
		minionLogMaxAgeInDays: minioncfg.DefaultLogMaxAgeInDays,

		mesosAuthProvider:              sasl.ProviderName,
		mesosCgroupPrefix:              minioncfg.DefaultCgroupPrefix,
		mesosMaster:                    defaultMesosMaster,
		mesosUser:                      defaultMesosUser,
		mesosExecutorCPUs:              defaultExecutorCPUs,
		mesosExecutorMem:               defaultExecutorMem,
		frameworkRoles:                 strings.Split(defaultFrameworkRoles, ","),
		defaultPodRoles:                strings.Split(defaultPodRoles, ","),
		reconcileInterval:              defaultReconcileInterval,
		reconcileCooldown:              defaultReconcileCooldown,
		checkpoint:                     true,
		frameworkName:                  defaultFrameworkName,
		ha:                             false,
		mux:                            http.NewServeMux(),
		kubeletCadvisorPort:            4194, // copied from github.com/GoogleCloudPlatform/kubernetes/blob/release-0.14/cmd/kubelet/app/server.go
		kubeletSyncFrequency:           10 * time.Second,
		kubeletEnableDebuggingHandlers: true,
		containPodResources:            true,
		nodeRelistPeriod:               defaultNodeRelistPeriod,
		conntrackTCPTimeoutEstablished: 0, // non-zero values may require hand-tuning other sysctl's on the host; do so with caution
		useHostPortEndpoints:           true,

		// non-zero values can trigger failures when updating /sys/module/nf_conntrack/parameters/hashsize
		// when kube-proxy is running in a non-root netns (init_net); setting this to a non-zero value will
		// impact connection tracking for the entire host on which kube-proxy is running. xref (k8s#19182)
		conntrackMax: 0,
		conntrackMaxPerCore: 0,
	}

	return &s
}

func (s *SchedulerServer) addCoreFlags(fs *pflag.FlagSet) {
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
	fs.StringVar(&s.mesosAuthSecretFile, "mesos-authentication-secret-file", s.mesosAuthSecretFile, "Mesos authentication secret file.")
	fs.StringVar(&s.mesosAuthProvider, "mesos-authentication-provider", s.mesosAuthProvider, fmt.Sprintf("Authentication provider to use, default is SASL that supports mechanisms: %+v", mech.ListSupported()))
	fs.StringVar(&s.dockerCfgPath, "dockercfg-path", s.dockerCfgPath, "Path to a dockercfg file that will be used by the docker instance of the minions.")
	fs.StringVar(&s.mesosCgroupPrefix, "mesos-cgroup-prefix", s.mesosCgroupPrefix, "The cgroup prefix concatenated with MESOS_DIRECTORY must give the executor cgroup set by Mesos")
	fs.Var(&s.mesosExecutorCPUs, "mesos-executor-cpus", "Initial CPU shares to allocate for each Mesos executor container.")
	fs.Var(&s.mesosExecutorMem, "mesos-executor-mem", "Initial memory (MB) to allocate for each Mesos executor container.")
	fs.BoolVar(&s.checkpoint, "checkpoint", s.checkpoint, "Enable/disable checkpointing for the kubernetes-mesos framework.")
	fs.Float64Var(&s.failoverTimeout, "failover-timeout", s.failoverTimeout, fmt.Sprint("Framework failover timeout, in sec."))
	fs.BoolVar(&s.generateTaskDiscovery, "mesos-generate-task-discovery", s.generateTaskDiscovery, "Enable/disable generation of DiscoveryInfo for Mesos tasks.")
	fs.UintVar(&s.driverPort, "driver-port", s.driverPort, "Port that the Mesos scheduler driver process should listen on.")
	fs.StringVar(&s.hostnameOverride, "hostname-override", s.hostnameOverride, "If non-empty, will use this string as identification instead of the actual hostname.")
	fs.Int64Var(&s.reconcileInterval, "reconcile-interval", s.reconcileInterval, "Interval at which to execute task reconciliation, in sec. Zero disables.")
	fs.DurationVar(&s.reconcileCooldown, "reconcile-cooldown", s.reconcileCooldown, "Minimum rest period between task reconciliation operations.")
	fs.StringVar(&s.schedulerConfigFileName, "scheduler-config", s.schedulerConfigFileName, "An ini-style configuration file with low-level scheduler settings.")
	fs.BoolVar(&s.graceful, "graceful", s.graceful, "Indicator of a graceful failover, intended for internal use only.")
	fs.BoolVar(&s.ha, "ha", s.ha, "Run the scheduler in high availability mode with leader election. All peers should be configured exactly the same.")
	fs.StringVar(&s.frameworkName, "framework-name", s.frameworkName, "The framework name to register with Mesos.")
	fs.StringVar(&s.frameworkStoreURI, "framework-store-uri", s.frameworkStoreURI, "Where the framework should store metadata, either in Zookeeper (zk://host:port/path) or in etcd (etcd://path).")
	fs.StringVar(&s.frameworkWebURI, "framework-weburi", s.frameworkWebURI, "A URI that points to a web-based interface for interacting with the framework.")
	fs.StringVar(&s.advertisedAddress, "advertised-address", s.advertisedAddress, "host:port address that is advertised to clients. May be used to construct artifact download URIs.")
	fs.IPVar(&s.serviceAddress, "service-address", s.serviceAddress, "The service portal IP address that the scheduler should register with (if unset, chooses randomly)")
	fs.Var(&s.defaultContainerCPULimit, "default-container-cpu-limit", "Containers without a CPU resource limit are admitted this much CPU shares")
	fs.Var(&s.defaultContainerMemLimit, "default-container-mem-limit", "Containers without a memory resource limit are admitted this much amount of memory in MB")
	fs.BoolVar(&s.containPodResources, "contain-pod-resources", s.containPodResources, "Reparent pod containers into mesos cgroups; disable if you're having strange mesos/docker/systemd interactions.")
	fs.DurationVar(&s.nodeRelistPeriod, "node-monitor-period", s.nodeRelistPeriod, "Period between relisting of all nodes from the apiserver.")
	fs.BoolVar(&s.useHostPortEndpoints, "host-port-endpoints", s.useHostPortEndpoints, "Map service endpoints to hostIP:hostPort instead of podIP:containerPort. Default true.")

	fs.IntVar(&s.executorLogV, "executor-logv", s.executorLogV, "Logging verbosity of spawned minion and executor processes.")
	fs.BoolVar(&s.executorBindall, "executor-bindall", s.executorBindall, "When true will set -address of the executor to 0.0.0.0.")
	fs.DurationVar(&s.executorSuicideTimeout, "executor-suicide-timeout", s.executorSuicideTimeout, "Executor self-terminates after this period of inactivity. Zero disables suicide watch.")
	fs.DurationVar(&s.launchGracePeriod, "mesos-launch-grace-period", s.launchGracePeriod, "Launch grace period after which launching tasks will be cancelled. Zero disables launch cancellation.")
	fs.StringVar(&s.sandboxOverlay, "mesos-sandbox-overlay", s.sandboxOverlay, "Path to an archive (tar.gz, tar.bz2 or zip) extracted into the sandbox.")

	fs.BoolVar(&s.proxyBindall, "proxy-bindall", s.proxyBindall, "When true pass -proxy-bindall to the executor.")
	fs.BoolVar(&s.runProxy, "run-proxy", s.runProxy, "Run the kube-proxy as a side process of the executor.")
	fs.StringVar(&s.proxyKubeconfig, "proxy-kubeconfig", s.proxyKubeconfig, "Path to kubeconfig file with authorization and master location information used by the proxy.")
	fs.IntVar(&s.proxyLogV, "proxy-logv", s.proxyLogV, "Logging verbosity of spawned minion proxy processes.")
	fs.StringVar(&s.proxyMode, "proxy-mode", s.proxyMode, "Which proxy mode to use: 'userspace' (older) or 'iptables' (faster). If the iptables proxy is selected, regardless of how, but the system's kernel or iptables versions are insufficient, this always falls back to the userspace proxy.")

	fs.StringVar(&s.minionPathOverride, "minion-path-override", s.minionPathOverride, "Override the PATH in the environment of the minion sub-processes.")
	fs.Var(resource.NewQuantityFlagValue(&s.minionLogMaxSize), "minion-max-log-size", "Maximum log file size for the executor and proxy before rotation")
	fs.IntVar(&s.minionLogMaxAgeInDays, "minion-max-log-age", s.minionLogMaxAgeInDays, "Maximum log file age of the executor and proxy in days")
	fs.IntVar(&s.minionLogMaxBackups, "minion-max-log-backups", s.minionLogMaxBackups, "Maximum log file backups of the executor and proxy to keep after rotation")

	fs.StringSliceVar(&s.kubeletApiServerList, "kubelet-api-servers", s.kubeletApiServerList, "List of Kubernetes API servers kubelet will use. (ip:port), comma separated. If unspecified it defaults to the value of --api-servers.")
	fs.StringVar(&s.kubeletRootDirectory, "kubelet-root-dir", s.kubeletRootDirectory, "Directory path for managing kubelet files (volume mounts,etc). Defaults to executor sandbox.")
	fs.StringVar(&s.kubeletDockerEndpoint, "kubelet-docker-endpoint", s.kubeletDockerEndpoint, "If non-empty, kubelet will use this for the docker endpoint to communicate with.")
	fs.StringVar(&s.kubeletPodInfraContainerImage, "kubelet-pod-infra-container-image", s.kubeletPodInfraContainerImage, "The image whose network/ipc namespaces containers in each pod will use.")
	fs.UintVar(&s.kubeletCadvisorPort, "kubelet-cadvisor-port", s.kubeletCadvisorPort, "The port of the kubelet's local cAdvisor endpoint")
	fs.StringVar(&s.kubeletHostNetworkSources, "kubelet-host-network-sources", s.kubeletHostNetworkSources, "Comma-separated list of sources from which the Kubelet allows pods to use of host network. For all sources use \"*\" [default=\"file\"]")
	fs.DurationVar(&s.kubeletSyncFrequency, "kubelet-sync-frequency", s.kubeletSyncFrequency, "Max period between synchronizing running containers and config")
	fs.StringVar(&s.kubeletNetworkPluginName, "kubelet-network-plugin", s.kubeletNetworkPluginName, "<Warning: Alpha feature> The name of the network plugin to be invoked for various events in kubelet/pod lifecycle")
	fs.BoolVar(&s.kubeletEnableDebuggingHandlers, "kubelet-enable-debugging-handlers", s.kubeletEnableDebuggingHandlers, "Enables kubelet endpoints for log collection and local running of containers and commands")
	fs.StringVar(&s.kubeletKubeconfig, "kubelet-kubeconfig", s.kubeletKubeconfig, "Path to kubeconfig file with authorization and master location information used by the kubelet.")
	fs.IntVar(&s.conntrackMax, "conntrack-max", s.conntrackMax, "Maximum number of NAT connections to track on agent nodes (0 to leave as-is)")
	fs.IntVar(&s.conntrackMaxPerCore, "conntrack-max-per-core", s.conntrackMaxPerCore,
		"Maximum number of NAT connections to track per CPU core (0 to leave as-is). This is only considered if conntrack-max is 0.")
	fs.IntVar(&s.conntrackTCPTimeoutEstablished, "conntrack-tcp-timeout-established", s.conntrackTCPTimeoutEstablished, "Idle timeout for established TCP connections on agent nodes (0 to leave as-is)")
}