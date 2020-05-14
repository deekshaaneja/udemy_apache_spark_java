// import static spark.Spark.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import io.netty.util.internal.SystemPropertyUtil;
// import spark.Spark.SparkConf;
public class class_1 {
	public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().set("spark.driver.host", "127.0.0.1");
        JavaSparkContext sc = new JavaSparkContext("local[4]", "startingSpark", conf);

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);
        
        Integer result = myRdd.reduce((value1, value2) -> value1 + value2);

        JavaRDD<Double> sqrtRdd = myRdd.map( value -> Math.sqrt(value));

        // sqrtRdd.foreach(value -> System.out.println(value));
        sqrtRdd.collect().forEach(System.out::println);
        System.out.println(result);

        // how many elements in sqrtRdd
        System.out.println(sqrtRdd.count());

        // using just map and reduce
        JavaRDD<Long> singleIntegerRdd = sqrtRdd.map( value -> 1L);
        Long count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
        System.out.println("count is " + count);

        JavaRDD<Integer> originalIntegers = sc.parallelize(inputData); 
        JavaRDD<IntegerWithSquareRoot> sqrtRdd1 = originalIntegers.map(value ->  new IntegerWithSquareRoot(value));          

        sc.close();
    }
}
