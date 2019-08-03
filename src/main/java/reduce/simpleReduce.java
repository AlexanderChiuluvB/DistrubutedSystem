package reduce;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class simpleReduce {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();
        //spark对普通List的reduce操作
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> originRDD = javaSparkContext.parallelize(data);

        Integer sum = originRDD.reduce((a,b)->a+b);
        System.out.println(sum);

        //reduce by key
        //就是将key相同的键值对，按照Function进行计算。代码中就是将key相同的各value进行累加。
        // 结果就是[(key2,2), (key3,1), (key1,2)]
        List<String> list = Arrays.asList("key1","key2","key3","key1","key2");
        JavaRDD<String> keyRDD = javaSparkContext.parallelize(list);
        JavaPairRDD<String,Integer> pairRDD = keyRDD.mapToPair(s->new Tuple2<>(s,1));
        List list1 = pairRDD.reduceByKey((a,b)-> a+b).collect();
        System.out.println(list1);

        //basic operation
        // 去重
        List<Integer> repeatData = Arrays.asList(1,2,3,4,3,5,2,2);
        JavaRDD<Integer> repeatDataRDD = javaSparkContext.parallelize(repeatData);
        List<Integer> result = repeatDataRDD.distinct().collect();
        System.out.println(result);


        //union合并不去重

        List<Integer> mergeResult = repeatDataRDD.union(originRDD).collect();
        System.out.println(mergeResult);

        //交集
        List<Integer> interResult = repeatDataRDD.intersection(originRDD).collect();
        System.out.println(interResult);

        //笛卡尔积
        List<Tuple2<Integer,Integer>> results = originRDD.cartesian(repeatDataRDD).collect();

        System.out.println(results);

    }
}
