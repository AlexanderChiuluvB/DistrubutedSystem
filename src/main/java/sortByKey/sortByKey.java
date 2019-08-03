package sortByKey;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class sortByKey {


    public static void main(String [] args){


        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();
        //spark对普通List的reduce操作
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        List<Tuple2<String, Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>("A", 10));
        data.add(new Tuple2<>("A", 20));
        data.add(new Tuple2<>("B", 2));
        data.add(new Tuple2<>("B", 3));
        data.add(new Tuple2<>("C", 5));

        JavaPairRDD<String,Integer> pairRDD = javaSparkContext.parallelizePairs(data);

        //increasing
        System.out.println(pairRDD.sortByKey(true).collect());

        //decreasing
        System.out.println(pairRDD.sortByKey(false).collect());

        //group by key each key 's value become an iterator
        //group by key作用对象一定是pairRDD
        System.out.println(pairRDD.groupByKey().collect());

        //group by　自己制定分组规则
        List<Integer> data2 = new ArrayList<>();
        data2.add(10);
        data2.add(1);
        data2.add(6);
        data2.add(5);
        data2.add(3);

        JavaRDD<Integer>originRDD = javaSparkContext.parallelize(data2);
        Map map = originRDD.groupBy(x->{
            //这里return的是key
            if(x%2==0){
                return "even";
            }else{
                return "odd";
            }
        }).collectAsMap();
        System.out.println(map);


        //cogroup
        //cogroup则是对多个RDD里key相同的，合并成集合的集合
        JavaRDD<Tuple2<String, Integer>> rdd1 = javaSparkContext.parallelize(Arrays.asList(
                new Tuple2<>("A", 10),
                new Tuple2<>("B", 20),
                new Tuple2<>("A", 30),
                new Tuple2<>("B", 40)));
        JavaRDD<Tuple2<String, Integer>> rdd2 = javaSparkContext.parallelize(Arrays.asList(
                new Tuple2<>("A", 100),
                new Tuple2<>("B", 200),
                new Tuple2<>("A", 300),
                new Tuple2<>("B", 400)));
        JavaRDD<Tuple2<String, Integer>> rdd3 = javaSparkContext.parallelize(Arrays.asList(
                new Tuple2<>("A", 1000),
                new Tuple2<>("B", 2000),
                new Tuple2<>("A", 3000),
                new Tuple2<>("B", 4000)));

        JavaPairRDD<String, Integer> pairRDD1 = JavaPairRDD.fromJavaRDD(rdd1);
        JavaPairRDD<String, Integer> pairRDD2 = JavaPairRDD.fromJavaRDD(rdd2);
        JavaPairRDD<String, Integer> pairRDD3 = JavaPairRDD.fromJavaRDD(rdd3);
        JavaPairRDD<String, Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>> pairRDD4 = pairRDD1.cogroup(pairRDD2, pairRDD3);
        System.out.println(pairRDD4.collect());


    }
}
