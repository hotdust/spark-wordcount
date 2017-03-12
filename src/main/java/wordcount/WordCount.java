package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;

/**
 *执行方法：
 * 1，把jar文件上传到服务器上
 * 2，把输入文件上传到hadoop上
 * 3，执行下面的命令
 *    在Driver机器输出Log时
 *     ./spark-submit --class wordcount.WordCount ~/spark/wordcount/spark_word_count.jar hdfs://10.211.55.5:9000/test/spark/wordcount/input/wordcount.txt
 *    把输出保存到Hadoop时
 *     ./spark-submit --class wordcount.WordCount ~/spark_word_count.jar hdfs://10.211.55.5:9000/test/spark/wordcount/input/wordcount.txt hdfs://10.211.55.5:9000/test/spark/wordcount/output/
 *
 * Created by shijiapeng on 17/3/6.
 */
public class WordCount {

    public static void main(String[] args) throws IOException {
        String inputFile = args[0];
        String outputFile = args[1];
        // 使用工程中的资源方式
//        String inputFile = Thread.currentThread().getClass().getResource("/") + "chapter1.TXT";

        SparkConf conf = new SparkConf().setAppName("wordCount_111").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(inputFile);

//        JavaPairRDD<String, Integer> result = input
//                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
//                .mapToPair(word -> new Tuple2<>(word, 1))
//                .reduceByKey((a, b) -> a + b);

        JavaRDD<String> words = input.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> counts = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> result = counts.reduceByKey((x, y) -> x + y);

        // 如果文件存夹存在，删除
        deleteFolder(outputFile);
        // 保存结果
        result.saveAsTextFile(outputFile);
        // 如果结果集不大，可以用下面这种方式进行显示
//        result.collect().forEach(tu -> System.out.println(tu._1() + ":" + tu._2()));
    }

    public static void deleteFolder(String folderPath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(folderPath), conf);
        Path path = new Path(folderPath);
        if (fs.exists(path)) fs.delete(path, true);
    }
}
