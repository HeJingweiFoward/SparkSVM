package common;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class DataSample {
	public static void main(String[] args) throws IOException, URISyntaxException {
		long startTime=System.currentTimeMillis();
		SparkConf sparkConf = new SparkConf().setJars(new String[] { "E:\\论文实验\\mjsl1.jar" })
	/*			.set("spark.num.executors", "4")
				.set("spark.executor.cores", "3")
				.set("spark.default.parallelism", "12")
				.set("spark.executor.memory", "2048m")
				.set("spark.network.timeout", "300")
				.setMaster("spark://192.168.2.151:7077");*/
				.setMaster("local[4]");
		SparkSession spark = SparkSession.builder().appName("DataSample").config(sparkConf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		System.setProperty("HADOOP_USER_NAME", "hadoop");
	



		JavaRDD<String> lines = jsc.textFile("hdfs://192.168.2.151:9000/test/hjw/sample/skin_nonskin",1);
		JavaRDD<String>trainSet= lines.sample(false, 0.4);
/*		JavaRDD<String>subtracttrainSet=lines.subtract(trainSet,1);
		JavaRDD<String>testSet=subtracttrainSet.sample(false, 0.5);*/
		JavaRDD<String>testSet=lines.subtract(trainSet,1);
		
		System.out.println("训练集数据条数"+trainSet.count()+"；测试集数据条数"+testSet.count()/*+",subtracttrainSet:"+subtracttrainSet.count()*/);
		trainSet.saveAsTextFile("hdfs://192.168.2.151:9000/test/hjw/sample/skin_nonskin10WTrain");
		testSet.saveAsTextFile("hdfs://192.168.2.151:9000/test/hjw/sample/skin_nonskin10WTest");
		
		
		
	
		


	}
}
