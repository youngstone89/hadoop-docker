

```
$ git clone https://github.com/Marcel-Jan/docker-hadoop-spark
$ cd docker-hadoop-spark
$ docker-compose up
```


## Hadoop

### NameNode
>  http://<dockerhadoop_IP_address>:9870/dfshealth.html#tab-overview

### History Serve
 http://<dockerhadoop_IP_address>:8188/applicationhistory
### DataNode: 
> http://<dockerhadoop_IP_address>:9864/
### NodeManager
>  http://<dockerhadoop_IP_address>:8042/node
### ResourceManager
> http://<dockerhadoop_IP_address>:8088/


## Spark
### Master
>  http://<dockerhadoop_IP_address>:8080/
### Worker
> http://<dockerhadoop_IP_address>:8081/

### Hive URI
> http://<dockerhadoop_IP_address>:10000