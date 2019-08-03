### Spark Structured Streaming tutorial

https://jaceklaskowski.gitbooks.io/spark-structured-streaming/spark-structured-streaming.html

#### Structured Streaming 编程模型

受到Google Dataflow 的批流统一思想，SS把流式数据看做一个不断增长的table，然后使用和批处理同一套API，都是基于DataSet/DataFrame的。如下图：通过把流式数据理解成一张不断增长的表，可以像操作batch静态数据一样处理流数据



![1563414819620](/home/alex/.config/Typora/typora-user-images/1563414819620.png)

#### 4 components

##### Input Unbounded Table 流式数据的抽象表示

##### Query 对input table的增量式查询

##### Result Table

##### Output



代码执行步骤：

流式数据当做一个不断增长的table，每秒trigger一次，在trigger的时候把query应用到input table新增加的数据上，有时候需要和之前的静态数据一起组合成结果。query产生的结果成为Result Table，我们可以选择把Result Table输出到外部存储。



三种输出模式

“complete”  全量输出

"update" 更新的row都会被输出

```
 StreamingQuery query = processedDataset.writeStream()
		      .outputMode("update")
		      .format("console")
		      .start();
```



#### Continuous Processing Mode

一有数据可用就会立刻进行处理。

比micro-batch而言延迟更低

![1563415934227](/home/alex/.config/Typora/typora-user-images/1563415934227.png)



在处理过程中，epoch的offset会被记录到log中 



![1563415862461](/home/alex/.config/Typora/typora-user-images/1563415862461.png)









