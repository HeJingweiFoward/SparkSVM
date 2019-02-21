package spark.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.resources.CreateParentParam;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import parallelSVM.MSSvmTrainer;
import scala.Tuple2;

public class MyJavaSparkLearning1 {

	// 从本地读取支持向量样本
	public static Vector<String> ReadTrain(String path) throws IOException {
		Vector<String> svRecords = new Vector<String>();
		File file = new File(path);

		BufferedReader br = new BufferedReader(new FileReader(file));

		try {
			String line;
			while ((line = br.readLine()) != null && line.length() > 1) {
				// System.out.println(line);
				svRecords.addElement(line);
			}
		} finally {

			br.close();
		}

		return svRecords;

	}

	// 从HDFS读取支持向量样本
	public static Vector<String> ReadTrainFromHDFS(FileSystem fs, Path pt) throws IOException {
		Vector<String> svRecords = new Vector<String>();

		if (fs != null) {
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

			try {
				String line;
				// line=br.readLine();
				while ((line = br.readLine()) != null && line.length() > 1) {
					// System.out.println(line);
					svRecords.addElement(line);
				}
			} finally {
				// you should close out the BufferedReader
				br.close();
			}
		}

		return svRecords;

	}

	public static void main(String[] args) throws IOException, URISyntaxException {
		long startTime=System.currentTimeMillis();
		SparkConf sparkConf = new SparkConf().setJars(new String[] { "E:\\论文实验\\mjsl1.jar" }).set("spark.executor.memory", "1000m").setMaster("spark://192.168.2.151:7077");
		SparkSession spark = SparkSession.builder().appName("MyJavaSpark").config(sparkConf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		System.setProperty("HADOOP_USER_NAME", "hadoop");
		// c,g参数的存储路径
		String cgPath = "hdfs://192.168.2.151:9000/test/hjw/SvmIn";
		// c,g参数的数量
		int cnum = 6;// c
		int gnum = 6;// g
		// 一次允许运行的Reduce最大数量
		int maxReduceTask = 16;
		// 第几次实验
		int experimentNum = 0;

		// 生成c,g，向HDFS中写入c,g
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.2.151:9000"), new Configuration());

		int count = 0;
		for (int i = 0; i < cnum; i++) {
			for (int j = gnum; j > 0; j--) {
				String sparam = String.valueOf(0.5 + i * 0.25) + "-" + String.valueOf(0.05 + j * 0.0125);
				count++;
				Path path = new Path(cgPath + "/" + String.valueOf(count));
				FSDataOutputStream out = fs.create(path, true);
				out.writeBytes(sparam);
				out.flush();
				out.close();
			}
		}
		// 读取c,g
		// JavaRDD<String> lines = jsc.textFile("E:\\论文实验\\ModelSelectIn");
		// 从HDFS读取c,g
		JavaRDD<String> lines = jsc.textFile(cgPath);
		System.out.println(lines.collect());
		
		System.out.println("开始读取支持向量");
		Vector<String> svRecords = new Vector<String>();
		Path pt = new Path(new URI("hdfs://datanode1:9000/SVM/DataSet/a8a"));
		// 从HDFS读取训练样本
		svRecords = ReadTrainFromHDFS(fs, pt);
		String[] ssvRecords = svRecords.toArray(new String[svRecords.size()]);
		System.out.println("开始网格搜索");
		// 开始并行网格搜索，交叉验证
		JavaPairRDD<String, String> mapToPairLines = lines.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) throws Exception {

				// svRecords = ReadTrain("E:\\论文实验\\SVM\\a8t");
				// svRecords = ReadTrain("E:\\论文实验\\a8trained");
				System.out.println("开始交叉验证");
				MSSvmTrainer svmTrainer = new MSSvmTrainer(ssvRecords, Double.parseDouble(s.split("-")[0]),
						Double.parseDouble(s.split("-")[1]));
				String acc = svmTrainer.do_cross_validation();
				System.out.println("交叉验证结束");
				return new Tuple2(s, acc);
			}
		});

		// System.out.println("mapToPairLines" + mapToPairLines.collect());

		List<Tuple2<String, String>> list = mapToPairLines.collect();
		long endTime=System.currentTimeMillis();		
		System.out.println("网格搜索结束，用时："+(endTime-startTime));
		
		System.out.println("mapToPairLines" + list);
		double max = Double.MIN_VALUE;
		for (Tuple2<String, String> tuple2 : list) {

			if (Double.parseDouble(tuple2._2().replace("%", "")) > max) {
				max = Double.parseDouble(tuple2._2().replace("%", ""));
			}
		}
		System.out.println("最高准确率："+max+"%");

		/*
		 * JavaPairRDD<String, String> mapTest= lines.mapToPair(row->{ return
		 * new Tuple2(""+TaskContext.getPartitionId(), row); });
		 * 
		 * JavaPairRDD<String, String>reduceTest=mapTest.reduceByKey((x,y)->{
		 * return x+y+"测试Reduce"; //return x+"测试Reduce"; });
		 * 
		 * System.out.println("map"+mapTest.collect());
		 * System.out.println("reduce"+reduceTest.collect());
		 */
	}
}
