
// import static spark.Spark.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import io.netty.util.internal.SystemPropertyUtil;
import scala.Tuple2;
// import spark.Spark.SparkConf;
public class Tuple {
	public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().set("spark.driver.host", "127.0.0.1");
        JavaSparkContext sc = new JavaSparkContext("local[4]", "startingSpark", conf);

        JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
        
        JavaRDD<Tuple2<Integer, Double>> sqrtRdd1 = originalIntegers.map(value ->  new Tuple2<>(value, Math.sqrt(value)));          

        sc.close();
    }
}
