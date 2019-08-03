package map;


import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * map是对RDD每个元素都执行一个指定的函数来产生new RDD
 *
 * map is one to one
 */
public class SimpleMap {

    public static void main(String [] args){

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("JavaWordCount")
                .master("local")
                .getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        List<Integer> data = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> originRDD = javaSparkContext.parallelize(data);

        //计算所有元素的和
        System.out.println(originRDD.reduce((a,b)->a+b));

        //map的使用
        JavaRDD<Integer> doubleRDD = originRDD.map(a->a*2);
        //collect RDD to become list
        List<Integer> doubleData = doubleRDD.collect();
        System.out.println(doubleData);
        int sumOfDouble = doubleRDD.reduce((a,b)->a+b);
        System.out.println(sumOfDouble);

        //transform key to key-value , the add value
        List<String> stringList = Arrays.asList("a","bb","ccc","dddd");
        JavaRDD<String> stringRdd = javaSparkContext.parallelize(stringList);
        JavaPairRDD pairRDD = stringRdd.mapToPair(k-> new Tuple2<>(k,1));
        List list1 = pairRDD.collect();
        //[(a,1), (bb,1), (ccc,1), (dddd,1)]
        System.out.println(list1);

        //modify key
        JavaPairRDD valueRdd = stringRdd.mapToPair(k->new Tuple2<>(k.length(),1));
        List list2 = valueRdd.collect();
        //[(1,1), (2,1), (3,1), (4,1)]
        System.out.println(list2);


        //[(1,1_tails), (2,1_tails), (3,1_tails), (4,1_tails)]
        List list3 = valueRdd.mapValues(k->k+"_tails").collect();
        System.out.println(list3);
    }

}
