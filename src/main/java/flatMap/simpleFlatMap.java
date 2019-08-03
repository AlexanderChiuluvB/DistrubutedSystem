package flatMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * flatMap 是把函数应用于ＲＤＤ每个元素，然后把返回的迭代器所有内容构成新的ＲＤＤ
 * LAMBDA　函数一定要返回一个迭代器
 */

public class simpleFlatMap {

    public static void main(String []args){

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("JavaWordCount")
                .master("local")
                .getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        List<String> data= Arrays.asList("hello world","I love darcy","hello spark");

        JavaRDD<String> originRDD = javaSparkContext.parallelize(data);

        JavaRDD<String> flatMap = originRDD.flatMap(s->Arrays.asList(s.split(" ")).iterator());

        //[hello, world, I, love, darcy, hello, spark]
        System.out.println(flatMap.collect());

    }
}
