package combineByKey;

import org.apache.derby.jdbc.EmbeddedXADataSource;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Test {

    /**
     * CombinebyKey要override三个函数
     *　　　　给定PairRDD<K,V>
     *     1.createCombiner[V, C] 将当前的值V作为参数，然后对其进行一些操作或者类型转换等，相当于进行一次map操作，并返回map后的结果C。
     *     2.mergeValue[C, V, C] 将createCombiner函数返回的结果C，再组合最初的PariRDD的V，将C和V作为输入参数，进行一些操作，并返回结果，类型也为C。
     *     3.mergeCombiners[C, C] 将mergeValue产生的结果C，进行组合。这里主要是针对不同的分区，各自分区执行完上面两步后得到的C进行组合，最终得到结果。如果只有一个分区，那这个函数执行的结果，其实就是第二步的结果。
     * @param args
     */


    public static void main(String[]args){

        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();
        //spark对普通List的reduce操作
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        List<ScoreDetail> data = new ArrayList<>();
        data.add(new ScoreDetail("xiaoming", "Math", 98));
        data.add(new ScoreDetail("xiaoming", "English", 88));
        data.add(new ScoreDetail("wangwu", "Math", 75));
        data.add(new ScoreDetail("wangwu", "English", 78));
        data.add(new ScoreDetail("lihua", "Math", 90));
        data.add(new ScoreDetail("lihua", "English", 80));
        data.add(new ScoreDetail("zhangsan", "Math", 91));

        //转换为name->score key-value对
        JavaRDD<ScoreDetail> origin = javaSparkContext.parallelize(data);
        JavaPairRDD<String,Integer> pairRdd = origin.mapToPair(ScoreDetail->
                new Tuple2<>(ScoreDetail.getStudentName(),ScoreDetail.getScore()));

        Map<String,Tuple2> map = pairRdd.combineByKey(

                //createCombiner[V, C]
                new Function<Integer, Tuple2>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Integer score) throws Exception {
                        return new Tuple2<>(score, 1);
                    }
                },

                //这3个参数第一个是上一个function的返回值，第二个是最早的pairRDD的value，第三个是该函数的返回值类型
                new Function2<Tuple2, Integer, Tuple2>() {
                    @Override
                    public Tuple2 call(Tuple2 v1, Integer score) throws Exception {
                        Tuple2<Integer,Integer>tuple2 = new Tuple2<>((int)v1._1+score,(int)v1._2+1);
                        System.out.println(tuple2);
                        return tuple2;
                    }
                },

                new Function2<Tuple2, Tuple2, Tuple2>() {
                    @Override
                    public Tuple2 call(Tuple2 v1, Tuple2 v2) throws Exception {

                        return new Tuple2((int)v1._1+(int)v2._1,(int)v1._2+(int)v2._2);
                    }
                }).collectAsMap();

        System.out.println(map);
    }

}
