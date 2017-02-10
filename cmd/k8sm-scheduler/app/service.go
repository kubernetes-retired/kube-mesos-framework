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
	"bufio"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	"github.com/pborman/uuid"
	"github.com/spf13/pflag"
	"golang.org/x/net/context"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/election"
	execcfg "github.com/kubernetes-incubator/kube-mesos-framework/pkg/executor/config"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/flagutil"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/hyperkube"
	minioncfg "github.com/kubernetes-incubator/kube-mesos-framework/pkg/minion/config"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/podutil"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/profile"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/runtime"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler/components"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler/components/algorithm/podschedulers"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler/components/framework"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler/components/framework/frameworkid"
	frameworkidEtcd "github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler/components/framework/frameworkid/etcd"
	frameworkidZk "github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler/components/framework/frameworkid/zk"
	schedcfg "github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler/config"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler/executorinfo"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler/ha"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler/meta"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler/metrics"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler/podtask"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler/podtask/hostport"
	"github.com/kubernetes-incubator/kube-mesos-framework/pkg/scheduler/resources"

	"k8s.io/client-go/1.5/pkg/api/resource"
	"k8s.io/client-go/1.5/pkg/master/ports"
	"cloud.google.com/go"
	"k8s.io/client-go/1.5/tools/clientcmd"
	"k8s.io/client-go/1.5/pkg/util/sets"
	"k8s.io/client-go/1.5/pkg/fields"
	"k8s.io/client-go/1.5/tools/record"
)

const (
	defaultMesosMaster           = "localhost:5050"
	defaultMesosUser             = "root" // should have privs to execute docker and iptables commands
	defaultFrameworkRoles        = "*"
	defaultPodRoles              = "*"
	defaultReconcileInterval     = 300 // 5m default task reconciliation interval
	defaultReconcileCooldown     = 15 * time.Second
	defaultNodeRelistPeriod      = 5 * time.Minute
	defaultFrameworkName         = "Kubernetes"
	defaultExecutorCPUs          = resources.CPUShares(0.25)  // initial CPU allocated for executor
	defaultExecutorMem           = resources.MegaBytes(128.0) // initial memory allocated for executor
	defaultExecutorInfoCacheSize = 10000
)


// useful for unit testing specific funcs
type schedulerProcessInterface interface {
	End() <-chan struct{}
	Failover() <-chan struct{}
	Terminal() <-chan struct{}
}


func (s *SchedulerServer) AddStandaloneFlags(fs *pflag.FlagSet) {
	s.addCoreFlags(fs)
	fs.StringVar(&s.executorPath, "executor-path", s.executorPath, "Location of the kubernetes executor executable")
}

func (s *SchedulerServer) AddHyperkubeFlags(fs *pflag.FlagSet) {
	s.addCoreFlags(fs)
	fs.StringVar(&s.kmPath, "km-path", s.kmPath, "Location of the km executable, may be a URI or an absolute file path; may be prefixed with 'file://' to specify the path to a pre-installed, agent-local km binary.")
}

// returns (downloadURI, basename(path))
func (s *SchedulerServer) serveFrameworkArtifact(path string) (string, string) {
	basename := filepath.Base(path)
	return s.serveFrameworkArtifactWithFilename(path, basename), basename
}

// returns downloadURI
func (s *SchedulerServer) serveFrameworkArtifactWithFilename(path string, filename string) string {
	serveFile := func(pattern string, filepath string) {
		s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filepath)
		})
	}

	serveFile("/"+filename, path)

	hostURI := ""
	if s.advertisedAddress != "" {
		hostURI = fmt.Sprintf("http://%s/%s", s.advertisedAddress, filename)
	} else if s.ha && s.haDomain != "" {
		hostURI = fmt.Sprintf("http://%s.%s:%d/%s", SCHEDULER_SERVICE_NAME, s.haDomain, ports.SchedulerPort, filename)
	} else {
		hostURI = fmt.Sprintf("http://%s:%d/%s", s.address.String(), s.port, filename)
	}
	log.V(2).Infof("Hosting artifact '%s' at '%s'", filename, hostURI)

	return hostURI
}

func (s *SchedulerServer) prepareExecutorInfo(hks hyperkube.Interface) (*mesos.ExecutorInfo, error) {
	ci := &mesos.CommandInfo{
		Shell: proto.Bool(false),
	}

	if s.executorPath != "" {
		uri, executorCmd := s.serveFrameworkArtifact(s.executorPath)
		ci.Uris = append(ci.Uris, &mesos.CommandInfo_URI{Value: proto.String(uri), Executable: proto.Bool(true)})
		ci.Value = proto.String(fmt.Sprintf("./%s", executorCmd))
		ci.Arguments = append(ci.Arguments, ci.GetValue())
	} else if !hks.FindServer(hyperkube.CommandMinion) {
		return nil, fmt.Errorf("either run this scheduler via km or else --executor-path is required")
	} else {
		if strings.Index(s.kmPath, "://") > 0 {
			if strings.HasPrefix(s.kmPath, "file://") {
				// If `kmPath` started with "file://", `km` in agent local path was used.
				ci.Value = proto.String(strings.TrimPrefix(s.kmPath, "file://"))
			} else {
				// URI could point directly to executable, e.g. hdfs:///km
				// or else indirectly, e.g. http://acmestorage/tarball.tgz
				// so we assume that for this case the command will always "km"
				ci.Uris = append(ci.Uris, &mesos.CommandInfo_URI{Value: proto.String(s.kmPath), Executable: proto.Bool(true)})
				ci.Value = proto.String("./km") // TODO(jdef) extract constant
			}
		} else if s.kmPath != "" {
			uri, kmCmd := s.serveFrameworkArtifact(s.kmPath)
			ci.Uris = append(ci.Uris, &mesos.CommandInfo_URI{Value: proto.String(uri), Executable: proto.Bool(true)})
			ci.Value = proto.String(fmt.Sprintf("./%s", kmCmd))
		} else {
			uri, kmCmd := s.serveFrameworkArtifact(s.executable)
			ci.Uris = append(ci.Uris, &mesos.CommandInfo_URI{Value: proto.String(uri), Executable: proto.Bool(true)})
			ci.Value = proto.String(fmt.Sprintf("./%s", kmCmd))
		}
		ci.Arguments = append(ci.Arguments, ci.GetValue(), hyperkube.CommandMinion)

		ci.Arguments = append(ci.Arguments, fmt.Sprintf("--run-proxy=%v", s.runProxy))
		ci.Arguments = append(ci.Arguments, fmt.Sprintf("--proxy-bindall=%v", s.proxyBindall))
		if s.proxyKubeconfig != "" {
			//TODO(jdef) should probably support non-local files, e.g. hdfs:///some/config/file
			uri, basename := s.serveFrameworkArtifact(s.proxyKubeconfig)
			ci.Uris = append(ci.Uris, &mesos.CommandInfo_URI{Value: proto.String(uri)})
			ci.Arguments = append(ci.Arguments, fmt.Sprintf("--proxy-kubeconfig=%v", basename))
		}
		ci.Arguments = append(ci.Arguments, fmt.Sprintf("--proxy-logv=%d", s.proxyLogV))
		ci.Arguments = append(ci.Arguments, fmt.Sprintf("--proxy-mode=%v", s.proxyMode))

		ci.Arguments = append(ci.Arguments, fmt.Sprintf("--path-override=%s", s.minionPathOverride))
		ci.Arguments = append(ci.Arguments, fmt.Sprintf("--max-log-size=%v", s.minionLogMaxSize.String()))
		ci.Arguments = append(ci.Arguments, fmt.Sprintf("--max-log-backups=%d", s.minionLogMaxBackups))
		ci.Arguments = append(ci.Arguments, fmt.Sprintf("--max-log-age=%d", s.minionLogMaxAgeInDays))
		ci.Arguments = append(ci.Arguments, fmt.Sprintf("--conntrack-max=%d", s.conntrackMax))
		ci.Arguments = append(ci.Arguments, fmt.Sprintf("--conntrack-max-per-core=%d", s.conntrackMaxPerCore))
		ci.Arguments = append(ci.Arguments, fmt.Sprintf("--conntrack-tcp-timeout-established=%d", s.conntrackTCPTimeoutEstablished))
	}

	if s.sandboxOverlay != "" {
		if _, err := os.Stat(s.sandboxOverlay); os.IsNotExist(err) {
			return nil, fmt.Errorf("Sandbox overlay archive not found: %s", s.sandboxOverlay)
		}
		uri, _ := s.serveFrameworkArtifact(s.sandboxOverlay)
		ci.Uris = append(ci.Uris, &mesos.CommandInfo_URI{Value: proto.String(uri), Executable: proto.Bool(false), Extract: proto.Bool(true)})
	}

	if s.dockerCfgPath != "" {
		uri := s.serveFrameworkArtifactWithFilename(s.dockerCfgPath, ".dockercfg")
		ci.Uris = append(ci.Uris, &mesos.CommandInfo_URI{Value: proto.String(uri), Executable: proto.Bool(false), Extract: proto.Bool(false)})
	}

	//TODO(jdef): provide some way (env var?) for users to customize executor config
	//TODO(jdef): set -address to 127.0.0.1 if `address` is 127.0.0.1

	var apiServerArgs string
	if len(s.kubeletApiServerList) > 0 {
		apiServerArgs = strings.Join(s.kubeletApiServerList, ",")
	} else {
		apiServerArgs = strings.Join(s.apiServerList, ",")
	}
	ci.Arguments = append(ci.Arguments, fmt.Sprintf("--api-servers=%s", apiServerArgs))
	ci.Arguments = append(ci.Arguments, fmt.Sprintf("--v=%d", s.executorLogV)) // this also applies to the minion
	ci.Arguments = append(ci.Arguments, fmt.Sprintf("--allow-privileged=%t", s.allowPrivileged))
	ci.Arguments = append(ci.Arguments, fmt.Sprintf("--suicide-timeout=%v", s.executorSuicideTimeout))
	ci.Arguments = append(ci.Arguments, fmt.Sprintf("--mesos-launch-grace-period=%v", s.launchGracePeriod))

	if s.executorBindall {
		//TODO(jdef) determine whether hostname-override is really needed for bindall because
		//it conflicts with kubelet node status checks/updates
		//ci.Arguments = append(ci.Arguments, "--hostname-override=0.0.0.0")
		ci.Arguments = append(ci.Arguments, "--address=0.0.0.0")
	}

	ci.Arguments = append(ci.Arguments, fmt.Sprintf("--mesos-cgroup-prefix=%v", s.mesosCgroupPrefix))
	ci.Arguments = append(ci.Arguments, fmt.Sprintf("--cadvisor-port=%v", s.kubeletCadvisorPort))
	ci.Arguments = append(ci.Arguments, fmt.Sprintf("--sync-frequency=%v", s.kubeletSyncFrequency))
	ci.Arguments = append(ci.Arguments, fmt.Sprintf("--contain-pod-resources=%t", s.containPodResources))
	ci.Arguments = append(ci.Arguments, fmt.Sprintf("--enable-debugging-handlers=%t", s.kubeletEnableDebuggingHandlers))

	if s.kubeletKubeconfig != "" {
		//TODO(jdef) should probably support non-local files, e.g. hdfs:///some/config/file
		if s.kubeletKubeconfig != s.proxyKubeconfig {
			if filepath.Base(s.kubeletKubeconfig) == filepath.Base(s.proxyKubeconfig) {
				// scheduler serves kubelet-kubeconfig and proxy-kubeconfig by their basename
				// we currently don't support the case where the 2 kubeconfig files have the same
				// basename but different absolute name, e.g., /kubelet/kubeconfig and /proxy/kubeconfig
				return nil, fmt.Errorf("if kubelet-kubeconfig and proxy-kubeconfig are different, they must have different basenames")
			}
			// allows kubelet-kubeconfig and proxy-kubeconfig to point to the same file
			uri, _ := s.serveFrameworkArtifact(s.kubeletKubeconfig)
			ci.Uris = append(ci.Uris, &mesos.CommandInfo_URI{Value: proto.String(uri)})
		}
		ci.Arguments = append(ci.Arguments, fmt.Sprintf("--kubeconfig=%s", filepath.Base(s.kubeletKubeconfig)))
	}
	appendOptional := func(name string, value string) {
		if value != "" {
			ci.Arguments = append(ci.Arguments, fmt.Sprintf("--%s=%s", name, value))
		}
	}
	if s.clusterDNS != nil {
		appendOptional("cluster-dns", s.clusterDNS.String())
	}
	appendOptional("cluster-domain", s.clusterDomain)
	appendOptional("root-dir", s.kubeletRootDirectory)
	appendOptional("docker-endpoint", s.kubeletDockerEndpoint)
	appendOptional("pod-infra-container-image", s.kubeletPodInfraContainerImage)
	appendOptional("host-network-sources", s.kubeletHostNetworkSources)
	appendOptional("network-plugin", s.kubeletNetworkPluginName)

	// TODO(jdef) this code depends on poorly scoped cadvisor flags, will need refactoring soon
	appendOptional(flagutil.Cadvisor.HousekeepingInterval.NameValue())
	appendOptional(flagutil.Cadvisor.GlobalHousekeepingInterval.NameValue())

	log.V(1).Infof("prepared executor command %q with args '%+v'", ci.GetValue(), ci.Arguments)

	// Create mesos scheduler driver.
	execInfo := &mesos.ExecutorInfo{
		Command: ci,
		Name:    proto.String(cloud.KubernetesExecutorName),
		Source:  proto.String(execcfg.DefaultInfoSource),
	}

	// Check for staticPods
	data, staticPodCPUs, staticPodMem := s.prepareStaticPods()

	// set prototype resource. During procument these act as the blue print only.
	// In a final ExecutorInfo they might differ due to different procured
	// resource roles.
	execInfo.Resources = []*mesos.Resource{
		mutil.NewScalarResource("cpus", float64(s.mesosExecutorCPUs)+staticPodCPUs),
		mutil.NewScalarResource("mem", float64(s.mesosExecutorMem)+staticPodMem),
	}

	// calculate the ExecutorInfo hash to be used for validating compatibility.
	// It is used to determine whether a running executor is compatible with the
	// current scheduler configuration. If it is not, offers for those nodes
	// are declined by our framework and the operator has to phase out those
	// running executors in a cluster.
	execInfo.ExecutorId = executorinfo.NewID(execInfo)
	execInfo.Data = data
	log.V(1).Infof("started with executor id %v", execInfo.ExecutorId.GetValue())

	return execInfo, nil
}

func (s *SchedulerServer) prepareStaticPods() (data []byte, staticPodCPUs, staticPodMem float64) {
	// TODO(sttts): add a directory watch and tell running executors about updates
	if s.staticPodsConfigPath == "" {
		return
	}

	entries, errCh := podutil.ReadFromDir(s.staticPodsConfigPath)
	go func() {
		// we just skip file system errors for now, do our best to gather
		// as many static pod specs as we can.
		for err := range errCh {
			log.Errorln(err.Error())
		}
	}()

	// validate cpu and memory limits, tracking the running totals in staticPod{CPUs,Mem}
	validateResourceLimits := StaticPodValidator(
		s.defaultContainerCPULimit,
		s.defaultContainerMemLimit,
		&staticPodCPUs,
		&staticPodMem)

	zipped, err := podutil.Gzip(validateResourceLimits.Do(entries))
	if err != nil {
		log.Errorf("failed to generate static pod data: %v", err)
		staticPodCPUs, staticPodMem = 0, 0
	} else {
		data = zipped
	}
	return
}

// TODO(jdef): hacked from plugin/cmd/kube-scheduler/app/server.go
func (s *SchedulerServer) createAPIServerClientConfig() (*restclient.Config, error) {
	kubeconfig, err := clientcmd.BuildConfigFromFlags(s.apiServerList[0], s.kubeconfig)
	if err != nil {
		return nil, err
	}

	// Override kubeconfig qps/burst settings from flags
	kubeconfig.QPS = s.kubeAPIQPS
	kubeconfig.Burst = s.kubeAPIBurst
	return kubeconfig, nil
}

func (s *SchedulerServer) setDriver(driver bindings.SchedulerDriver) {
	s.driverMutex.Lock()
	defer s.driverMutex.Unlock()
	s.driver = driver
}

func (s *SchedulerServer) getDriver() (driver bindings.SchedulerDriver) {
	s.driverMutex.RLock()
	defer s.driverMutex.RUnlock()
	return s.driver
}

func (s *SchedulerServer) Run(hks hyperkube.Interface, _ []string) error {
	if n := len(s.frameworkRoles); n == 0 || n > 2 || (n == 2 && s.frameworkRoles[0] != "*" && s.frameworkRoles[1] != "*") {
		log.Fatalf(`only one custom role allowed in addition to "*"`)
	}

	fwSet := sets.NewString(s.frameworkRoles...)
	podSet := sets.NewString(s.defaultPodRoles...)
	if !fwSet.IsSuperset(podSet) {
		log.Fatalf("all default pod roles %q must be included in framework roles %q", s.defaultPodRoles, s.frameworkRoles)
	}

	// get scheduler low-level config
	sc := schedcfg.CreateDefaultConfig()
	if s.schedulerConfigFileName != "" {
		f, err := os.Open(s.schedulerConfigFileName)
		if err != nil {
			log.Fatalf("Cannot open scheduler config file: %v", err)
		}
		defer f.Close()

		err = sc.Read(bufio.NewReader(f))
		if err != nil {
			log.Fatalf("Invalid scheduler config file: %v", err)
		}
	}

	schedulerProcess, driverFactory, etcdClient, eid := s.bootstrap(hks, sc)

	if s.enableProfiling {
		profile.InstallHandler(s.mux)
	}
	go runtime.Until(func() {
		log.V(1).Info("Starting HTTP interface")
		log.Error(http.ListenAndServe(net.JoinHostPort(s.address.String(), strconv.Itoa(s.port)), s.mux))
	}, sc.HttpBindInterval.Duration, schedulerProcess.Terminal())

	if s.ha {
		validation := ha.ValidationFunc(validateLeadershipTransition)
		srv := ha.NewCandidate(schedulerProcess, driverFactory, validation)
		path := meta.ElectionPath(s.frameworkName)
		uuid := eid.GetValue() + ":" + uuid.New() // unique for each scheduler instance
		log.Infof("registering for election at %v with id %v", path, uuid)
		go election.Notify(
			election.NewEtcdMasterElector(etcdClient),
			path,
			uuid,
			srv,
			nil)
	} else {
		log.Infoln("self-electing in non-HA mode")
		schedulerProcess.Elect(driverFactory)
	}
	return s.awaitFailover(schedulerProcess, func() error { return s.failover(s.getDriver(), hks) })
}

// watch the scheduler process for failover signals and properly handle such. may never return.
func (s *SchedulerServer) awaitFailover(schedulerProcess schedulerProcessInterface, handler func() error) error {

	// we only want to return the first error (if any), everyone else can block forever
	errCh := make(chan error, 1)
	doFailover := func() error {
		// we really don't expect handler to return, if it does something went seriously wrong
		err := handler()
		if err != nil {
			defer schedulerProcess.End()
			err = fmt.Errorf("failover failed, scheduler will terminate: %v", err)
		}
		return err
	}

	// guard for failover signal processing, first signal processor wins
	failoverLatch := &runtime.Latch{}
	runtime.On(schedulerProcess.Terminal(), func() {
		if !failoverLatch.Acquire() {
			log.V(1).Infof("scheduler process ending, already failing over")
			select {}
		}
		var err error
		defer func() { errCh <- err }()
		select {
		case <-schedulerProcess.Failover():
			err = doFailover()
		default:
			if s.ha {
				err = fmt.Errorf("ha scheduler exiting instead of failing over")
			} else {
				log.Infof("exiting scheduler")
			}
		}
	})
	runtime.OnOSSignal(makeFailoverSigChan(), func(_ os.Signal) {
		if !failoverLatch.Acquire() {
			log.V(1).Infof("scheduler process signalled, already failing over")
			select {}
		}
		errCh <- doFailover()
	})
	return <-errCh
}

func validateLeadershipTransition(desired, current string) {
	log.Infof("validating leadership transition")
	// desired, current are of the format <executor-id>:<scheduler-uuid> (see Run()).
	// parse them and ensure that executor ID's match, otherwise the cluster can get into
	// a bad state after scheduler failover: executor ID is a config hash that must remain
	// consistent across failover events.
	var (
		i = strings.LastIndex(desired, ":")
		j = strings.LastIndex(current, ":")
	)

	if i > -1 {
		desired = desired[0:i]
	} else {
		log.Fatalf("desired id %q is invalid", desired)
	}
	if j > -1 {
		current = current[0:j]
	} else if current != "" {
		log.Fatalf("current id %q is invalid", current)
	}

	if desired != current && current != "" {
		log.Fatalf("desired executor id %q != current executor id %q", desired, current)
	}
}

// hacked from https://github.com/kubernetes/kubernetes/blob/release-0.14/cmd/kube-apiserver/app/server.go
func newEtcd(etcdServerList []string) (etcd.Client, error) {
	cfg := etcd.Config{
		Endpoints: etcdServerList,
	}
	return etcd.New(cfg)
}

func (s *SchedulerServer) bootstrap(hks hyperkube.Interface, sc *schedcfg.Config) (*ha.SchedulerProcess, ha.DriverFactory, etcd.Client, *mesos.ExecutorID) {
	s.frameworkName = strings.TrimSpace(s.frameworkName)
	if s.frameworkName == "" {
		log.Fatalf("framework-name must be a non-empty string")
	}
	s.frameworkWebURI = strings.TrimSpace(s.frameworkWebURI)

	metrics.Register()
	runtime.Register()
	s.mux.Handle("/metrics", prometheus.Handler())
	healthz.InstallHandler(s.mux)

	if len(s.etcdServerList) == 0 {
		log.Fatalf("specify --etcd-servers must be specified")
	}

	if len(s.apiServerList) < 1 {
		log.Fatal("No api servers specified.")
	}

	clientConfig, err := s.createAPIServerClientConfig()
	if err != nil {
		log.Fatalf("Unable to make apiserver client config: %v", err)
	}
	s.client, err = clientset.NewForConfig(clientConfig)
	if err != nil {
		log.Fatalf("Unable to make apiserver clientset: %v", err)
	}

	if s.reconcileCooldown < defaultReconcileCooldown {
		s.reconcileCooldown = defaultReconcileCooldown
		log.Warningf("user-specified reconcile cooldown too small, defaulting to %v", s.reconcileCooldown)
	}

	eiPrototype, err := s.prepareExecutorInfo(hks)
	if err != nil {
		log.Fatalf("misconfigured executor: %v", err)
	}

	// TODO(jdef): remove the dependency on etcd as soon as
	// (1) the generic config store is available for the FrameworkId storage
	// (2) the generic master election is provided by the apiserver
	// Compare docs/proposals/high-availability.md
	etcdClient, err := newEtcd(s.etcdServerList)
	if err != nil {
		log.Fatalf("misconfigured etcd: %v", err)
	}
	keysAPI := etcd.NewKeysAPI(etcdClient)

	// mirror all nodes into the nodeStore
	var eiRegistry executorinfo.Registry
	nodesClientConfig := *clientConfig
	nodesClient, err := clientset.NewForConfig(&nodesClientConfig)
	if err != nil {
		log.Fatalf("Cannot create client to watch nodes: %v", err)
	}
	nodeLW := cache.NewListWatchFromClient(nodesClient.CoreClient, "nodes", api.NamespaceAll, fields.Everything())
	nodeStore, nodeCtl := cache.NewInformer(nodeLW, &api.Node{}, s.nodeRelistPeriod, &cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			if eiRegistry != nil {
				// TODO(jdef) use cache.DeletionHandlingMetaNamespaceKeyFunc at some point?
				nodeName := ""
				if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
					nodeName = tombstone.Key
				} else if node, ok := obj.(*api.Node); ok {
					nodeName = node.Name
				}
				if nodeName != "" {
					log.V(2).Infof("deleting node %q from registry", nodeName)
					eiRegistry.Invalidate(nodeName)
				}
			}
		},
	})

	lookupNode := func(hostName string) *api.Node {
		n, _, _ := nodeStore.GetByKey(hostName) // ignore error and return nil then
		if n == nil {
			return nil
		}
		return n.(*api.Node)
	}

	execInfoCache, err := executorinfo.NewCache(defaultExecutorInfoCacheSize)
	if err != nil {
		log.Fatalf("cannot create executorinfo cache: %v", err)
	}

	eiRegistry, err = executorinfo.NewRegistry(lookupNode, eiPrototype, execInfoCache)
	if err != nil {
		log.Fatalf("cannot create executorinfo registry: %v", err)
	}

	pr := podtask.NewDefaultProcurement(eiPrototype, eiRegistry)
	fcfs := podschedulers.NewFCFSPodScheduler(pr, lookupNode)
	frameworkIDStorage, err := s.frameworkIDStorage(keysAPI)
	if err != nil {
		log.Fatalf("cannot init framework ID storage: %v", err)
	}
	framework := framework.New(framework.Config{
		SchedulerConfig:   *sc,
		Client:            s.client,
		FailoverTimeout:   s.failoverTimeout,
		ReconcileInterval: s.reconcileInterval,
		ReconcileCooldown: s.reconcileCooldown,
		LookupNode:        lookupNode,
		StoreFrameworkId:  frameworkIDStorage.Set,
		ExecutorId:        eiPrototype.GetExecutorId(),
	})
	masterUri := s.mesosMaster
	info, cred, err := s.buildFrameworkInfo()
	if err != nil {
		log.Fatalf("Misconfigured mesos framework: %v", err)
	}

	schedulerProcess := ha.New(framework)

	// try publishing on the same IP as the slave
	var publishedAddress net.IP
	if libprocessIP := os.Getenv("LIBPROCESS_IP"); libprocessIP != "" {
		publishedAddress = net.ParseIP(libprocessIP)
	}
	if publishedAddress != nil {
		log.V(1).Infof("driver will publish address %v", publishedAddress)
	}

	dconfig := &bindings.DriverConfig{
		Scheduler:        schedulerProcess,
		Framework:        info,
		Master:           masterUri,
		Credential:       cred,
		BindingAddress:   s.address,
		BindingPort:      uint16(s.driverPort),
		PublishedAddress: publishedAddress,
		HostnameOverride: s.hostnameOverride,
		WithAuthContext: func(ctx context.Context) context.Context {
			ctx = auth.WithLoginProvider(ctx, s.mesosAuthProvider)
			ctx = sasl.WithBindingAddress(ctx, s.address)
			return ctx
		},
	}

	// create event recorder sending events to the "" namespace of the apiserver
	eventsClientConfig := *clientConfig
	eventsClient, err := clientset.NewForConfig(&eventsClientConfig)
	if err != nil {
		log.Fatalf("Invalid API configuration: %v", err)
	}
	broadcaster := record.NewBroadcaster()
	recorder := broadcaster.NewRecorder(api.EventSource{Component: api.DefaultSchedulerName})
	broadcaster.StartLogging(log.Infof)
	broadcaster.StartRecordingToSink(&unversionedcore.EventSinkImpl{Interface: eventsClient.Events("")})

	lw := cache.NewListWatchFromClient(s.client.CoreClient, "pods", api.NamespaceAll, fields.Everything())

	hostPortStrategy := hostport.StrategyFixed
	if s.useHostPortEndpoints {
		hostPortStrategy = hostport.StrategyWildcard
	}

	// create scheduler core with all components arranged around it
	sched := components.New(
		sc,
		framework,
		fcfs,
		s.client,
		recorder,
		schedulerProcess.Terminal(),
		s.mux,
		lw,
		podtask.Config{
			DefaultPodRoles:              s.defaultPodRoles,
			FrameworkRoles:               s.frameworkRoles,
			GenerateTaskDiscoveryEnabled: s.generateTaskDiscovery,
			HostPortStrategy:             hostPortStrategy,
			Prototype:                    eiPrototype,
		},
		s.defaultContainerCPULimit,
		s.defaultContainerMemLimit,
	)

	runtime.On(framework.Registration(), func() { sched.Run(schedulerProcess.Terminal()) })
	runtime.On(framework.Registration(), s.newServiceWriter(publishedAddress, schedulerProcess.Terminal()))
	runtime.On(framework.Registration(), func() { nodeCtl.Run(schedulerProcess.Terminal()) })

	driverFactory := ha.DriverFactory(func() (drv bindings.SchedulerDriver, err error) {
		log.V(1).Infoln("performing deferred initialization")
		if err = framework.Init(sched, schedulerProcess.Master(), s.mux); err != nil {
			return nil, fmt.Errorf("failed to initialize pod scheduler: %v", err)
		}

		log.V(1).Infoln("deferred init complete")
		if s.failoverTimeout > 0 {
			// defer obtaining framework ID to prevent multiple schedulers
			// from overwriting each other's framework IDs
			var frameworkID string
			frameworkID, err = frameworkIDStorage.Get(context.TODO())
			if err != nil {
				return nil, fmt.Errorf("failed to fetch framework ID from storage: %v", err)
			}
			if frameworkID != "" {
				log.Infof("configuring FrameworkInfo with ID found in storage: %q", frameworkID)
				dconfig.Framework.Id = &mesos.FrameworkID{Value: &frameworkID}
			} else {
				log.V(1).Infof("did not find framework ID in storage")
			}
		} else {
			// TODO(jdef) this is a hack, really for development, to simplify clean up of old framework IDs
			frameworkIDStorage.Remove(context.TODO())
		}

		log.V(1).Infoln("constructing mesos scheduler driver")
		drv, err = bindings.NewMesosSchedulerDriver(*dconfig)
		if err != nil {
			return nil, fmt.Errorf("failed to construct scheduler driver: %v", err)
		}

		log.V(1).Infoln("constructed mesos scheduler driver:", drv)
		s.setDriver(drv)
		return drv, nil
	})

	return schedulerProcess, driverFactory, etcdClient, eiPrototype.GetExecutorId()
}

func (s *SchedulerServer) failover(driver bindings.SchedulerDriver, hks hyperkube.Interface) error {
	if driver != nil {
		stat, err := driver.Stop(true)
		if stat != mesos.Status_DRIVER_STOPPED {
			return fmt.Errorf("failed to stop driver for failover, received unexpected status code: %v", stat)
		} else if err != nil {
			return err
		}
	}

	// there's no guarantee that all goroutines are actually programmed intelligently with 'done'
	// signals, so we'll need to restart if we want to really stop everything

	// run the same command that we were launched with
	//TODO(jdef) assumption here is that the scheduler is the only service running in this process, we should probably validate that somehow
	args := []string{}
	flags := pflag.CommandLine
	if hks != nil {
		args = append(args, hks.Name())
		flags = hks.Flags()
	}
	flags.Visit(func(flag *pflag.Flag) {
		if flag.Name != "api-servers" && flag.Name != "etcd-servers" && flag.Name != "kubelet-api-servers" {
			args = append(args, fmt.Sprintf("--%s=%s", flag.Name, flag.Value.String()))
		}
	})
	if !s.graceful {
		args = append(args, "--graceful")
	}
	if len(s.apiServerList) > 0 {
		args = append(args, "--api-servers="+strings.Join(s.apiServerList, ","))
	}
	if len(s.etcdServerList) > 0 {
		args = append(args, "--etcd-servers="+strings.Join(s.etcdServerList, ","))
	}
	if len(s.kubeletApiServerList) > 0 {
		args = append(args, "--kubelet-api-servers="+strings.Join(s.kubeletApiServerList, ","))
	}
	args = append(args, flags.Args()...)

	log.V(1).Infof("spawning scheduler for graceful failover: %s %+v", s.executable, args)

	cmd := exec.Command(s.executable, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = makeDisownedProcAttr()

	// TODO(jdef) pass in a pipe FD so that we can block, waiting for the child proc to be ready
	//cmd.ExtraFiles = []*os.File{}

	exitcode := 0
	log.Flush() // TODO(jdef) it would be really nice to ensure that no one else in our process was still logging
	if err := cmd.Start(); err != nil {
		//log to stdtout here to avoid conflicts with normal stderr logging
		fmt.Fprintf(os.Stdout, "failed to spawn failover process: %v\n", err)
		os.Exit(1)
	}
	os.Exit(exitcode)
	select {} // will never reach here
}

func (s *SchedulerServer) buildFrameworkInfo() (info *mesos.FrameworkInfo, cred *mesos.Credential, err error) {
	username, err := s.getUsername()
	if err != nil {
		return nil, nil, err
	}
	log.V(2).Infof("Framework configured with mesos user %v", username)
	info = &mesos.FrameworkInfo{
		Name:       proto.String(s.frameworkName),
		User:       proto.String(username),
		Checkpoint: proto.Bool(s.checkpoint),
	}
	if s.frameworkWebURI != "" {
		info.WebuiUrl = proto.String(s.frameworkWebURI)
	}
	if s.failoverTimeout > 0 {
		info.FailoverTimeout = proto.Float64(s.failoverTimeout)
	}

	// set the framework's role to the first configured non-star role.
	// once Mesos supports multiple roles simply set the configured mesos roles slice.
	for _, role := range s.frameworkRoles {
		if role != "*" {
			// mesos currently supports only one role per framework info
			// The framework will be offered role's resources as well as * resources
			info.Role = proto.String(role)
			break
		}
	}

	if s.mesosAuthPrincipal != "" {
		info.Principal = proto.String(s.mesosAuthPrincipal)
		cred = &mesos.Credential{
			Principal: proto.String(s.mesosAuthPrincipal),
		}
		if s.mesosAuthSecretFile != "" {
			secret, err := ioutil.ReadFile(s.mesosAuthSecretFile)
			if err != nil {
				return nil, nil, err
			}
			cred.Secret = proto.String(string(secret))
		}
	}
	return
}

func (s *SchedulerServer) getUsername() (username string, err error) {
	username = s.mesosUser
	if username == "" {
		if u, err := user.Current(); err == nil {
			username = u.Username
			if username == "" {
				username = defaultMesosUser
			}
		}
	}
	return
}

func (s *SchedulerServer) frameworkIDStorage(keysAPI etcd.KeysAPI) (frameworkid.Storage, error) {
	u, err := url.Parse(s.frameworkStoreURI)
	if err != nil {
		return nil, fmt.Errorf("cannot parse framework store URI: %v", err)
	}

	switch u.Scheme {
	case "etcd":
		idpath := meta.StoreChroot
		if u.Path != "" {
			idpath = path.Join("/", u.Path)
		}
		idpath = path.Join(idpath, s.frameworkName, "frameworkid")
		return frameworkidEtcd.Store(keysAPI, idpath, time.Duration(s.failoverTimeout)*time.Second), nil
	case "zk":
		return frameworkidZk.Store(s.frameworkStoreURI, s.frameworkName), nil
	default:
		return nil, fmt.Errorf("unsupported framework storage scheme: %q", u.Scheme)
	}
}
