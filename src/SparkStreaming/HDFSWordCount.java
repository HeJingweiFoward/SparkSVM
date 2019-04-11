package SparkStreaming;
 
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
 
import scala.Tuple2;
 
public class HDFSWordCount {
 
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setAppName("SparkStreamingWordCount").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//设置日志运行级别
		sc.setLogLevel("WARN");
		JavaStreamingContext jssc = new JavaStreamingContext(sc,Durations.seconds(5));

		JavaDStream<String> lines = jssc.textFileStream("hdfs://192.168.2.151:9000/test/hjw/SparkStreaming");
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>(){
 
			private static final long serialVersionUID = 1L;
 
			@Override
			public Iterator<String> call(String line) throws Exception {
			 	return Arrays.asList(line.split(" ")).iterator();
			}
			
		});
		
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>(){
 
			private static final long serialVersionUID = 1L;
 
			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
			
		});
		
		JavaPairDStream<String, Integer> wordcounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){
 
			private static final long serialVersionUID = 1L;
 
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
			
		});
		
		wordcounts.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.stop();
		jssc.close();
	}
}
