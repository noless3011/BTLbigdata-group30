# Issue

# Set up Minikube

## Problem:
```
    ❌  Exiting due to K8S_APISERVER_MISSING: wait 6m0s for node: wait for apiserver proc: apiserver process never appeared
```
## Solution:
```bash
minikube start --extra-config=apiserver.authorization-mode=RBAC
```

## Problem:
```bash
kubectl get node minikube
```

```
NAME       STATUS     ROLES           AGE    VERSION
minikube   NotReady   control-plane   368d   v1.31.0
```


```bash
kubectl describe node minikube
```

```
Conditions:
  Type             Status    LastHeartbeatTime                 LastTransitionTime                Reason              Message
  ----             ------    -----------------                 ------------------                ------              -------
  MemoryPressure   Unknown   Fri, 08 Nov 2024 15:35:32 +0700   Sun, 26 Oct 2025 10:10:26 +0700   NodeStatusUnknown   Kubelet stopped posting node status.
  DiskPressure     Unknown   Fri, 08 Nov 2024 15:35:32 +0700   Sun, 26 Oct 2025 10:10:26 +0700   NodeStatusUnknown   Kubelet stopped posting node status.
  PIDPressure      Unknown   Fri, 08 Nov 2024 15:35:32 +0700   Sun, 26 Oct 2025 10:10:26 +0700   NodeStatusUnknown   Kubelet stopped posting node status.
  Ready            Unknown   Fri, 08 Nov 2024 15:35:32 +0700   Sun, 26 Oct 2025 10:10:26 +0700   NodeStatusUnknown   Kubelet stopped posting node status.
```

## Solution
Reset 
```bash
minikube delete
minikube start
```


## Problem:
KafkaConnect pod keep toggling
## Solution:
Check the IP for the image registry in the KafkaConnect YAML file as it may fail to locate and push the image to wrong registry
