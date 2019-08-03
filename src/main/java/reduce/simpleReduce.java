package reduce;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
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

    }
}
