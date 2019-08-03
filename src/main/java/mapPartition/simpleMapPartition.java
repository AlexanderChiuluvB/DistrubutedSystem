package mapPartition;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


/**
 *
 * mapPartition　是对rdd中每个分区的迭代器进行操作
 *
 *　如果map过程中需要频繁创建额外的对象，map需要为每一个元素创建一个链接，
 * 而mapPartition要为每个分区创建链接，效率会高很多
 *
 */

public class simpleMapPartition {


    public static void main(String[] args){

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("JavaWordCount")
                .master("local")
                .getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        List<Integer> list = Arrays.asList(1,2,3,4,5);

        //三个分区
        JavaRDD<Integer> stringRDD = javaSparkContext.parallelize(list,3);

        //把这里将1，2，3，4，5分为2个区，然后对每个分区进行累加。
        //
        //结果是1+2，3+4+5.如果是分3个区，则是1，2+3，4+5.
        JavaRDD rdd = stringRDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {

            @Override
            public Iterator<Integer>call(Iterator<Integer>integerIterator)throws Exception{

                int sum = 0;
                while(integerIterator.hasNext()){
                    sum+=integerIterator.next();
                }
                List<Integer>list1 = new LinkedList<>();
                list1.add(sum);
                return list1.iterator();
            }
        });

        List list1 = rdd.collect();
        System.out.println(list1);
    }

}
