# DEV Instructions

## Use Kubernetes Dashboard Service Outside Cluster

Enter on machine that you want to use the dashboard and run this command

```bash
kubectl proxy --address='0.0.0.0' --accept-hosts='^*$'
```

Change this

```python
config.load_incluster_config()
v1 = client.CoreV1Api()
```

To this one

```python
from kubernetes.client import Configuration, ApiClient
myconfig=Configuration()
myconfig.host = "http://<machine_ip>:8001"
myapiclient = ApiClient(myconfig)
v1 = client.CoreV1Api(api_client=myapiclient)
```
