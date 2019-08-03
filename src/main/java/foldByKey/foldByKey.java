package foldByKey;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PairRDD<K,V>对不同的Ｋ分别对Ｖ做合并处理</>
 */
public class foldByKey {

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

        //zero value先与每一个key的第一个value进行计算,然后与同一个key的所有元素再作用
        Map map = pairRDD.foldByKey(2, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1*v2;
            }
        }).collectAsMap();
        //{A=400, C=10, B=12}
        System.out.println(map);


        Map map2 = pairRDD.foldByKey(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }).collectAsMap();
        //{A=30, C=5, B=5}
        System.out.println(map2);

    }

}
