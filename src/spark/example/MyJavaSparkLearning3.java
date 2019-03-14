package spark.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import parallelSVM.MSSvmTrainer;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.generic.BitOperations.Int;


public class MyJavaSparkLearning3 {


	
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
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		// TODO Auto-generated method stub
		long startTime = System.currentTimeMillis();
		SparkConf sparkConf = new SparkConf()
				.setJars(new String[] { "E:\\论文实验\\mjsl1.jar" })
				.set("spark.num.executors", "4")
				.set("spark.executor.cores", "3")
				.set("spark.default.parallelism", "12")
				.set("spark.executor.memory", "2048m")
				.set("spark.network.timeout", "300")
				//.set("spark.storage.memoryFraction", "0.5")//管理堆内内存
				//.set("spark.memory.storageFraction", "0.5")//管理堆外内存
			/*	.setMaster("local[4]");*/
				.setMaster("spark://192.168.2.151:7077");
		SparkSession spark = SparkSession.builder().appName("MyJavaSpark3").config(sparkConf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		System.setProperty("HADOOP_USER_NAME", "hadoop");
		// c,g参数的数量
		int cnum =4;// c
		int gnum = 4;// g

	    /**
	     * 计数器！
	     */
	
		  org.apache.spark.util.LongAccumulator accumulator = jsc.sc().longAccumulator();
	    
		// 生成c,g，向HDFS中写入c,g
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.2.151:9000"), new Configuration());

		// 测试从一个文件读取参数 或者直接将参数写入RDD、DataFrame
		List<CG> cgList = new ArrayList<CG>();
		for (int i = 0; i < cnum; i++) {
			for (int j = gnum; j > 0; j--) {
				/*String sparam = String.valueOf(0.5 + i * 0.25) + "-" + String.valueOf(0.05 + j * 0.0125);*/
			/*	String sparam = String.valueOf(i) + "-" +  String.valueOf(Math.pow(10,-j));*/						
				/*cgList.add(new CG(0.5 + i * 0.25,0.05 + j * 0.0125));*/
				CG cg=new CG();
				cg.setC(0.5 + i * 0.25);
				cg.setG(0.05 + j * 0.0125);
				cgList.add(cg);
			}
		}

		// 直接从List读取c,g
		//JavaRDD<String> lines = jsc.parallelize(cgList);
		Encoder<CG> cgEncoder = Encoders.bean(CG.class);
		Dataset<CG> cgDF = spark.createDataset(cgList,cgEncoder);



		System.out.println("开始读取支持向量");
		Vector<String> svRecords = new Vector<String>();
		/*Path pt = new Path(new URI("hdfs://datanode1:9000/SVM/DataSet/covtypebinaryscale"));*/
		Path pt = new Path(new URI("hdfs://datanode1:9000/SVM/DataSet/a8a"));
		// 从HDFS读取训练样本
		svRecords = ReadTrainFromHDFS(fs, pt);
		// 使用广播变量
		Broadcast<List<String>> broadcastssvRecords = jsc
				.broadcast(Arrays.asList(svRecords.toArray(new String[svRecords.size()])));

		System.out.println("开始网格搜索");
		// 开始并行网格搜索，交叉验证
	/*	JavaPairRDD<String, String> mapToPairLines = lines.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) throws Exception {

				System.out.println("开始交叉验证");
				// 使用广播变量中的支持向量
				String[] svr = (String[]) broadcastssvRecords.value().toArray();
				Double c=Double.valueOf(s.split("-")[0]);
				Double g=Double.valueOf(s.split("-")[1]);
				
				MSSvmTrainer svmTrainer = new MSSvmTrainer(svr, c,g);

				//计数
				accumulator.add(1);
				String acc = svmTrainer.do_cross_validation();
				System.out.println("交叉验证结束");
				return new Tuple2(s, acc);
			}
		});*/

		// 使用reduce排序
/*		Tuple2<String, String> maxACC = mapToPairLines.reduce((x, y) -> {
			if (Double.parseDouble(x._2().replace("%", "")) > Double.parseDouble(y._2().replace("%", ""))) {
				return x;
			} else {
				return y;
			}
		});*/
		Encoder<Double> doubleEncoder = Encoders.DOUBLE();
		Dataset<Double> cgMap=cgDF.map(new MapFunction<CG,Double>() {
			@Override
			public Double call(CG cg) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("开始交叉验证");
				// 使用广播变量中的支持向量
				String[] svr = (String[]) broadcastssvRecords.value().toArray();		
				MSSvmTrainer svmTrainer = new MSSvmTrainer(svr, cg.getC(),cg.getG());
				//计数
				accumulator.add(1);
				double acc =Double.parseDouble(svmTrainer.do_cross_validation().replace("%", "")) ;
				System.out.println("交叉验证结束");
				return acc;
			}
		}, doubleEncoder);
		//在这里使用show()导致分多次提交job
		cgMap.collect();
		
		long endTime = System.currentTimeMillis();
		System.out.println("网格搜索结束，用时：" + (endTime - startTime));
	/*	System.out.println("最高准确率：" + maxACC);*/
		System.out.println("共"+accumulator.value()+"组参数");
	
	}

}
