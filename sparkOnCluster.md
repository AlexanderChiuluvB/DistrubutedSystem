### 集群模式下的Spark



![1563334276973](/home/alex/.config/Typora/typora-user-images/1563334276973.png)

1.SparkContext 维护一个DAG，可以链接多个Cluster Manager(eg:YARN),后者负责在应用程序之间调度资源。

2.每个Worker Node有一个Executor，是负责执行Task任务的进程。每一个人无都有一个executor processes

3.如果Spark Context要向远程集群发送请求，应该要open an RPC to the driver and have it submit operations from nearby



### Running on YARN

Ensure that HADOOP_CONF_DIR or YARN_CONF_DIR points to the client side configuration files for the Hadoop cluster.

#### cluster mode

Spark driver runs inside an application master process which is managed by YARN on the cluster

#### client mode

driver runs in the client process , application master process only used for requesting resources from YARN

















