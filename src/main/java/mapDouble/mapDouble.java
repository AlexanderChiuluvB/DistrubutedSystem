package mapDouble;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * 这个则是将各元素计算平方，并转为double，最终打印结果的中位数、最大值、最小值、平均值等
 */
public class mapDouble {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();
        //spark对普通List的reduce操作
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        List<Integer> random = Collections.unmodifiableList(
                new Random()
                        .ints(-100, 101).limit(100000000)
                        .boxed()
                        .collect(Collectors.toList())
        );

        JavaRDD<Integer> rdd = javaSparkContext.parallelize(random);

        JavaDoubleRDD result = rdd.mapToDouble(x->(double)x*x);

        System.out.println(result.stats().toString());

    }
}
