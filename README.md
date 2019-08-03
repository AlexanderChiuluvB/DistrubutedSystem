# SparkTutorial
Spark Tutorial Spark JAVA API的一些用法

### Map相关算子的用法

map主要是替换，由一个旧的RDD生成一个新的RDD

### Reduce相关算子的用法

reduce主要是计算

### combineByKey 

ref:https://blog.csdn.net/tianyaleixiaowu/article/details/79989697

1.createCombiner[V, C] 将当前的值V作为参数，然后对其进行一些操作或者类型转换等，相当于进行一次map操作，并返回map后的结果C。
2.mergeValue[C, V, C] 将createCombiner函数返回的结果C，再组合最初的PariRDD的V，将C和V作为输入参数，进行一些操作，并返回结果，类型也为C。
3.mergeCombiners[C, C] 将mergeValue产生的结果C，进行组合。这里主要是针对不同的分区，各自分区执行完上面两步后得到的C进行组合，最终得到结果。如果只有一个分区，那这个函数执行的结果，其实就是第二步的结果。

看例子，假如有多个学生，每个学生有多门功课的成绩，我们要计算每个学生的成绩平均分。


["zhangsan":10，"zhangsan":15]

注意，Key我们不用管，全程都用不到Key。我们需要做的就是对value的一系列转换。

通过第一步createCombiner将V转为C，做法是将10转为Tuple2，即第一次碰到zhangsan这个key时，变成{zhangsan:(10, 1)}，C就是Tuple2类型，目的是记录zhangsan这个key共出现了几次。

第二步mergeValue，输入是Tuple2和value，我们的做法就是将Tuple2的第一个参数加上value，然后将Tuple2的第二个参数加一。也就是又碰到zhangsan了，就用10+15，得到结果是{zhangsan:(25, 2)}.

第三步就是对第二步的结果进行合并，假设有另一个分区里，也有zhangsan的结果为{zhangsan:(30, 3)}.那么第三步就是将两个Tuple2分别相加。返回结果{zhangsan:(55, 5)}.

三步做完就可以collect了。

