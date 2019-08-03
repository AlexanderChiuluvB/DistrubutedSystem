https://www.jianshu.com/p/f852b3b9e398

kafka：使用教程

https://www.jianshu.com/p/4afba0ffb88a



kafka: 概念解析

https://blog.csdn.net/u010020099/article/details/82290403



kafka: 监控

https://www.jianshu.com/p/05a125e29723

1.为了可靠并且高效地处理大规模的视频流数据，要一个可扩展，能容错，松耦合的分布式系统。



2.

基本架构：视频流收集器，流数据缓冲，视频流处理器。

具体思想：1.视频流收集器负责把视频帧序列化为流数据缓冲

​				   2.流数据缓冲是一个用于视频流数据的可容错消息队列

​				   3.视频流处理器消费缓冲中的流数据，然后做相应的算法处理

最后存储的数据存储到HDFS

3.框架：OpenCV,Kafka,Spark

#### Kafka

一个分布式的流平台，提供一个发布和订阅流记录的系统，记录按照可容错的方式进行存储

#### Spark

主要用图像处理的GraphX以及Spark Streaming



![1563243407525](/home/alex/.config/Typora/typora-user-images/1563243407525.png)



src code

https://github.com/baghelamit/video-stream-analytics



#### 细节实现

1.视频流收集

使用OPENCV库把视频流转换为帧，每帧调整为所需的分辨率，OPENCV把每帧存储为Mat对象，Mat需要转换为可连续的字节数组形式

考虑用JSON结构存储如时间戳，帧的rows,cols(分辨率信息)，data 即代表帧的字节数组

视频流收集器使用Gson库将数据转换为JSON消息，消息会被发送到video-stream-event-topic上。

使用**KafkaProducer**客户端将JSON消息发送到KafkaBroker。KafkaProducer会把每个Key发送到相同分区并保证消息的顺序。

Kafka处理大小比较大的数据的时候，要先调整

```
batch.size
max.request.size
compression.type
```



2.缓冲消息队列(Kafka)

为了无丢失处理大量视频流数据，需要把视频保存到容错能力强的临时存储当中。

因为处理数据的顺序比较重要，所以Kafka需要保证单个分区内给定topic的消息顺序

```
message.max.bytes
replica.fetch.max.bytes
```



3视频流处理器(Spark)

从Kafka broker中读取json，根据json创建一个mat对象处理视频流数据

需要使用的是Spark Streaming API 流计算的相关API

Structured Streaming 提供了快速，可扩展，容错，端到端且保证仅执行一次的流处理功能



http://www.doc88.com/p-3415046852000.html

参考文献14 为小型项目和小公司提供可行的低成本处理海量视频数据流的方案

### spark streaming computation tutorial

https://juejin.im/post/5afbdbeaf265da0b886d98e8



#### paper reading

http://www.c-s-a.org.cn/csa/ch/reader/create_pdf.aspx?file_no=6112&flag=1&year_id=12&quarter_id=

对于前者, 可以将各帧图像分布至不同节点上并行处理; 对于后者, 由于帧序列之间相关联,所以需要进行单节点处理.



![1563349966599](/home/alex/.config/Typora/typora-user-images/1563349966599.png)

streamID 视频流ID

frameSID 帧序列编号

data 视频帧数据

解码后的视频数据是由图像帧序列组成. 现有的视频分析算法根据每帧图像之间的关系可分为帧间无关与帧间相关. 对于前者, 可以将各帧图像分布至不同节点上并行处理; 对于后者, 由于帧序列之间相关联,所以需要进行单节点处理. 视频帧处理过程包括图片预处理、特征提取、目标检测与识别等. 在图像处理过程中, 需要调用opencv库或者其他图像处理库. 在Spark中, 可以通过利用RDD上的pipe算子, 能够以管道形式调用外部程序对RDD中的数据进行处理。



#### Structed Streaming doc

https://jaceklaskowski.gitbooks.io/spark-structured-streaming/spark-structured-streaming.html



docker一个镜像里面跑不同的容器

分别有kafka容器，master和slave容器







