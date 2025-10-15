## A Kafka Listener for process message and put into Clickhouse.

### QUICK START
```
Build docker image
$ docker build -t kafka-listener:latest .

Make sure the image have been built
$ docker image ls

You should find the image 
$ kafka-listener
```


#### Create kube namespace
`kubectl create namespace data-pipeline`

#### Create configmap
`kubectl create configmap kafka-listener-env --from-env-file=.env.dev -n data-pipeline`

#### Installing kafka listener using helm, I provide the pattern of kafka topics, 1 pod should be consume the topic that have pattern that you defined.
`helm install listener-covid ./k8s -n data-pipeline --create-namespace --set topicPattern="covid.*"`

#### If you want to delete pod
`helm uninstall listener-{topicpattern} -n data-pipeline`