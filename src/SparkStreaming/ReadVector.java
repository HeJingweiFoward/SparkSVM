package SparkStreaming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Vector;

import javax.validation.constraints.Null;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.base.Function;

public class ReadVector {
	// 从HDFS读取支持向量样本
	public static Vector<String> ReadTrainFromHDFS(FileSystem fs, Path pt) throws IOException {
		Vector<String> svRecords = new Vector<String>();

		if (fs != null) {
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

			try {
				String line;
				while ((line = br.readLine()) != null && line.length() > 1) {
					svRecords.addElement(line);
				}
			} finally {
				// you should close out the BufferedReader
				br.close();
			}
		}

		return svRecords;

	}
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("SparkStreamingReadVector").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//设置日志运行级别
		sc.setLogLevel("WARN");
		JavaStreamingContext jssc = new JavaStreamingContext(sc,Durations.seconds(5));

		JavaDStream<String> lines = jssc.textFileStream("hdfs://192.168.2.151:9000/test/hjw/SparkStreaming");
		
		Vector<String> svRecords = new Vector<String>();
	JavaDStream<Long> size=	lines.flatMap(new FlatMapFunction<String, String>() {
	/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String arg0) throws Exception {
				
				svRecords.addElement(arg0);
				//System.out.println("添加");
				return Arrays.asList(arg0.split(" ")).iterator();
			}
		}).count();
	
	
	System.out.println("svRecords.size()"+svRecords.size());
	size.print();


		
		jssc.start();
		jssc.awaitTermination();
		jssc.stop();
		jssc.close();
	}

}
