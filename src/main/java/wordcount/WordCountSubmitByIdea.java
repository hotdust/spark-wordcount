package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

/**
 * 此类可以直接使用Idea进行提交，提交之前请确认下面的设置是否正确
 * 1，输入文件inputFile的地址（可以设置通过参数取得）
 * 2，输出文件outputFile的地址（可以设置通过参数取得）
 * 3，设置的Master是否正确（setMaster）
 * 4，通过setJars设置的Jar文件和路径是否正确（Jar文件就是通过本类生成的Jar）
 *
 *
 * Created by shijiapeng on 17/3/6.
 */
public class WordCountSubmitByIdea {

    // lambda表达式可以提交成功
    public static void main(String[] args) throws IOException {

        String inputFile = "hdfs://10.211.55.5:9000/test/spark/wordcount/input/wordcount.txt";
        String outputFile = "hdfs://10.211.55.5:9000/test/spark/wordcount/output/";
//        String inputFile = args[0];
//        String outputFile = args[1];

        SparkConf conf = new SparkConf().setAppName("wordCount_111")
                .setMaster("spark://10.211.55.5:7077")
                .setJars(new String[]{"/Users/shijiapeng/IdeaProjects/spark/spark-wordcount/out/artifacts/spark_word_count/spark_word_count.jar"});
//        SparkConf conf = new SparkConf().setAppName("wordCount_111").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(inputFile);


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
