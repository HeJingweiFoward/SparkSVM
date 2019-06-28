package common;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

public class DataSplit {
	public static void main(String[] args) throws IOException, URISyntaxException {
		long startTime=System.currentTimeMillis();
		SparkConf sparkConf = new SparkConf().setJars(new String[] { "E:\\论文实验\\mjsl1.jar" })

				.setMaster("local[4]");
		SparkSession spark = SparkSession.builder().appName("DataSplit").config(sparkConf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		System.setProperty("HADOOP_USER_NAME", "hadoop");
	



		//JavaRDD<String> lines = jsc.textFile("hdfs://192.168.2.151:9000/SVM/DataSet/skin_nonskin",2);
		JavaRDD<String> lines = jsc.textFile("hdfs://192.168.2.151:9000/SVM/DataSet/real-sim").repartition(7);
		lines.saveAsTextFile("hdfs://192.168.2.151:9000/SVM/DataSet/real-simTrainAndTestRepartition");

			

	
		
		
	
		jsc.stop();
		jsc.close();


	}
}
