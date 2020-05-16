
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
public class Main {
        public static Integer getScore(Double percentage) {
                if (percentage >= .9) {
                        return 10;
                } else if (percentage >= .5) {
                        return 4;
                } else if (percentage >= .25) {
                        return 2;
                } else {
                        return 0;
                }
        }

        public static void main(String[] args) {

                Logger.getLogger("org.apache").setLevel(Level.WARN);
                SparkConf conf = new SparkConf().set("spark.driver.host", "127.0.0.1");
                JavaSparkContext sc = new JavaSparkContext("local[4]", "startingSpark", conf);

                ArrayList<Tuple2<Integer, Integer>> views = new ArrayList<>();
                views.add(new Tuple2<>(14, 96));
                views.add(new Tuple2<>(14, 97));
                views.add(new Tuple2<>(13, 96));
                views.add(new Tuple2<>(13, 96));
                views.add(new Tuple2<>(13, 96));
                views.add(new Tuple2<>(14, 99));
                views.add(new Tuple2<>(13, 100));

                ArrayList<Tuple2<Integer, Integer>> chapterIds = new ArrayList<>();
                chapterIds.add(new Tuple2<>(96, 1));
                chapterIds.add(new Tuple2<>(97, 1));
                chapterIds.add(new Tuple2<>(98, 1));
                chapterIds.add(new Tuple2<>(99, 2));
                chapterIds.add(new Tuple2<>(100, 3));
                chapterIds.add(new Tuple2<>(101, 3));
                chapterIds.add(new Tuple2<>(102, 3));
                chapterIds.add(new Tuple2<>(103, 3));
                chapterIds.add(new Tuple2<>(104, 3));
                chapterIds.add(new Tuple2<>(105, 3));
                chapterIds.add(new Tuple2<>(106, 3));
                chapterIds.add(new Tuple2<>(107, 3));
                chapterIds.add(new Tuple2<>(108, 3));
                chapterIds.add(new Tuple2<>(109, 3));

                // Exercise 1
                JavaPairRDD<Integer, Integer> chapterIdRdd = sc.parallelizePairs(chapterIds);
                JavaPairRDD<Integer, Integer> courseIdRdd = chapterIdRdd
                                .mapToPair(value -> new Tuple2<Integer, Integer>(value._2, 1));
                JavaPairRDD<Integer, Integer> countCourse = courseIdRdd
                                .reduceByKey((value1, value2) -> value1 + value2);
                // countCourse.collect().forEach(System.out::println);

                JavaPairRDD<Integer, Integer> viewsRdd = sc.parallelizePairs(views);
                JavaPairRDD<Integer, Integer> distinctViewRdd = viewsRdd.distinct();
                JavaPairRDD<Integer, Integer> distinctCourseRdd = distinctViewRdd
                                .mapToPair(tuple -> new Tuple2(tuple._2, tuple._1));

                JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRdd = distinctCourseRdd.join(chapterIdRdd);

                JavaPairRDD<Tuple2<Integer, Integer>, Long> dropcourseIdRdd = joinedRdd.mapToPair(row -> {
                        Integer userId = row._2._1;
                        Integer courseId = row._2._2;
                        return new Tuple2<Tuple2<Integer, Integer>, Long>(
                                        new Tuple2<Integer, Integer>(userId, courseId), 1L);

                });

                JavaPairRDD<Tuple2<Integer, Integer>, Long> courseIdReduceRdd = dropcourseIdRdd
                                .reduceByKey((value1, value2) -> value1 + value2);

                JavaPairRDD<Integer, Long> courseIdViewRdd = courseIdReduceRdd.mapToPair(tuple -> {
                        Integer courseId = tuple._1._2;
                        Long view_count = tuple._2;
                        return new Tuple2<>(courseId, view_count);
                });
                JavaPairRDD<Integer, Tuple2<Long, Integer>> step6 = courseIdViewRdd.join(countCourse);

                JavaPairRDD<Integer, Double> step7 = step6.mapToPair(tuple -> {
                        Integer x = tuple._1;
                        Double y = (double) (tuple._2._1 / (double) tuple._2._2);
                        return new Tuple2<>(x, y);
                });

                JavaPairRDD<Integer, Integer> step8 = step7.mapToPair(tuple -> {
                        Integer course = tuple._1;
                        Integer score = getScore(tuple._2);
                return new Tuple2<>(course, score);
        });

        JavaPairRDD<Integer, Integer> step9 = step8.reduceByKey((value1, value2) -> value1 + value2);

        step9.collect().forEach(System.out::println);


        sc.close();

}
}
