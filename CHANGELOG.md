<!-- BEGIN MUNGE: GENERATED_TOC -->

<!-- END MUNGE: GENERATED_TOC -->

<!-- NEW RELEASE NOTES ENTRY -->

# 0.8.0

1. Move StaticPod from master to slave: km used to read conf and pass to kubelet; but in 0.8, the kubelet'll read it from local hosts
1. All kubelet parameters will read from `--kubelet-conf-file`, kube-mesos-framework did not know it by default
1. The executor's directory is defined as follow, the filesystem should be support by Mesos fetcher
```text
executor.tar.gz
  - k8sm-executor
  - kube-proxy
  - kubelet
```
2. Use k8s ThirdPartyResource for HA, instead of etcd.

[![Analytics](https://kubernetes-site.appspot.com/UA-36037335-10/GitHub/CHANGELOG.md?pixel)]()
