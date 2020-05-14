
// import static spark.Spark.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.print.event.PrintJobListener;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.broadcast.Broadcast;
import org.spark_project.guava.collect.Iterables;

import io.netty.util.internal.SystemPropertyUtil;
import scala.Tuple2;

// import spark.Spark.SparkConf;
public class TestingJoins {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().set("spark.driver.host", "127.0.0.1");
        JavaSparkContext sc = new JavaSparkContext("local[4]", "startingSpark", conf);

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Merybelle"));
        usersRaw.add(new Tuple2<>(6, "Raquel"));

        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);
        // InnerJoin
        // JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);
        // joinedRdd.collect().forEach(System.out::println);
        // LeftOuterJoin
        // JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRdd = visits.leftOuterJoin(users);
        // joinedRdd.foreach(it -> System.out.println(it._2._2.orElse("blank").toUpperCase()));
        
        // Right Outer Join
        // JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRdd = visits.rightOuterJoin(users);
        // joinedRdd.foreach(it -> System.out.println("user " + it._2._2 + " had " + it._2._1.orElse(0)));
        
        // Full Outer Join
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> joinedRdd = visits.cartesian(users);
        joinedRdd.foreach(it -> System.out.println(it));

        sc.close();

}
}
