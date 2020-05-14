
// import static spark.Spark.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Iterables;

import io.netty.util.internal.SystemPropertyUtil;
import scala.Tuple2;
// import spark.Spark.SparkConf;
public class FlatMap {
	public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().set("spark.driver.host", "127.0.0.1");
        JavaSparkContext sc = new JavaSparkContext("local[4]", "startingSpark", conf);

    //     sc.parallelize(inputData)
    //         .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
    //         .reduceByKey((value1, value2) -> value1 + value2)
    //         .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));;


    // //  groupByKey
    // sc.parallelize(inputData)
    //     .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
    //     .groupByKey()
    //     .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));;
    //     sc.close();
    JavaRDD<String> sentences = sc.parallelize(inputData);
    JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
    words.collect().forEach(System.out::println);
}
}
