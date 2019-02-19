package spark.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import parallelSVM.MSSvmTrainer;
import scala.Tuple2;

public class MyJavaSparkLearning1 {

	public static Vector<String> ReadTrain(String path) throws IOException {
		Vector<String> svRecords = new Vector<String>();
		File file = new File(path);

		BufferedReader br = new BufferedReader(new FileReader(file));

		try {
			String line;
			while ((line = br.readLine()) != null && line.length() > 1) {
				//System.out.println(line);
				svRecords.addElement(line);
			}
		} finally {

			br.close();
		}

		return svRecords;

	}

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("MyJavaSpark").master("local[4]").getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaRDD<String> lines = jsc.textFile("E:\\论文实验\\ModelSelectIn");
		System.out.println("开始");

		
		JavaPairRDD<String, String> mapToPairLines = lines.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) throws Exception {

				Vector<String> svRecords = new Vector<String>();
				//svRecords = ReadTrain("E:\\论文实验\\SVM\\a8t");
				svRecords = ReadTrain("E:\\论文实验\\a8trained");
				String[] ssvRecords = svRecords.toArray(new String[svRecords.size()]);
				MSSvmTrainer svmTrainer = new MSSvmTrainer(ssvRecords, Double.parseDouble(s.split("-")[0]),
						Double.parseDouble(s.split("-")[1]));
				String acc = svmTrainer.do_cross_validation();

				return new Tuple2(s, acc);
			}
		});

		System.out.println("mapToPairLines"+mapToPairLines.collect());
		System.out.println("结束");
/*		 JavaPairRDD<String, String> mapTest= lines.mapToPair(row->{ return
		 new Tuple2(""+TaskContext.getPartitionId(), row); });
		  
		  JavaPairRDD<String, String>reduceTest=mapTest.reduceByKey((x,y)->{
		  return x+y+"测试Reduce"; //return x+"测试Reduce"; 
		  });
		  
		 System.out.println("map"+mapTest.collect());
		  System.out.println("reduce"+reduceTest.collect());*/
	}
}
