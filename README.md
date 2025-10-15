## QUICK START

1. Build docker image
`docker build -t kafka-listener:latest .`

Make sure the image have been built
`docker image ls`

You should find the image 
`kafka-listener`

2. Create kube namespace
`kubectl create namespace data-pipeline`

3. Create configmap
`kubectl create configmap kafka-listener-env --from-env-file=.env.dev -n data-pipeline`

4. Installing kafka listener using helm, I provide the pattern of kafka topics, 1 pod should be consume the topic that have pattern that you defined.
```
bridge: helm install listener-bridge ./k8s -n data-pipeline --create-namespace --set topicPattern="bridge.*"
loyalty: helm install listener-loyalty ./k8s -n data-pipeline --create-namespace --set topicPattern="loyalty.*"
etc
```

NB: If you want to delete pod
```helm uninstall listener-{topicpattern} -n data-pipeline

ex:
helm uninstall listener-bridge -n data-pipeline
helm uninstall listener-loyalty -n data-pipeline
```