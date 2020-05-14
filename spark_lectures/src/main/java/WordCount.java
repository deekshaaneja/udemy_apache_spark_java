
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
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

// import spark.Spark.SparkConf;
public class WordCount {
        public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().set("spark.driver.host", "127.0.0.1");
        JavaSparkContext sc = new JavaSparkContext("local[4]", "startingSpark", conf);
        
        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        JavaRDD<String> lettersOnlyRdd = initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

        JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(sentence -> sentence.trim().length() > 0);

        JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

        List<String> stopWords = sc.textFile("src/main/resources/subtitles/stopwords.txt").collect();
        // stopWords.collect().forEach(System.out::println);

        Broadcast<List<String>> stopWordsBroadcast = sc.broadcast(stopWords);

        JavaRDD<String> interestingWords = justWords.filter(token -> !stopWordsBroadcast.value().contains(token));

        JavaPairRDD<String, Long> pairRdd = interestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));
        
        JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);

        JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1));

        JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

        System.out.println("numPartitions:" + sorted.getNumPartitions());

        List<Tuple2<Long, String>> results = sorted.take(10);
        results.forEach(System.out::println);
        sc.close();

}
}
