package common;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import SparkStreaming.MSSvmTrainer;
import libsvm.svm_model;

public class DataSampleForTwoCatagroy {
	public static void main(String[] args) throws IOException, URISyntaxException {
		long startTime=System.currentTimeMillis();
		SparkConf sparkConf = new SparkConf().setJars(new String[] { "E:\\论文实验\\mjsl1.jar" })
				.setMaster("local[4]");
		SparkSession spark = SparkSession.builder().appName("DataSample").config(sparkConf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		System.setProperty("HADOOP_USER_NAME", "hadoop");
	

/*		JavaRDD<String> lines = jsc.textFile("hdfs://192.168.2.151:9000/SVM/SVs/cod-rnaTwoLevelSvs.txt",1);
		
		List<String> PositiveRecords = new ArrayList<String>();
		List<String> NegativeRecords = new ArrayList<String>();
		
		lines.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
	
			private static final long serialVersionUID = 453291289524473319L;

			@Override
			public Iterator<String> call(Iterator<String> arg0) throws Exception {
				// TODO Auto-generated method stub
				for (Iterator<String> it=arg0;it.hasNext();) {
					if (it.next().split(" ")[0].equals("+1")) {
						PositiveRecords.add(it.next());
					}
					else {
						NegativeRecords.add(it.next());
					}				
				} 		
				return null;
			}
		}).collect();*/
		
		
		
		JavaRDD<String>PositiveTrainSet= jsc.textFile("file:///D:\\Users\\workspace\\LibSVMLearning\\DataSet\\PNSvs\\cod-rnaTwoLevelPSvs.txt",1).sample(false, 0.8);
		JavaRDD<String>NegativeTrainSet= jsc.textFile("file:///D:\\Users\\workspace\\LibSVMLearning\\DataSet\\PNSvs\\cod-rnaTwoLevelNSvs.txt",1).sample(false, 0.8);

		JavaRDD<String>trainSet=PositiveTrainSet.union(NegativeTrainSet).repartition(1);
		//对支持向量进行随机抽样
		System.out.println("训练集数据条数"+trainSet.count());
		trainSet.saveAsTextFile("hdfs://192.168.2.151:9000/SVM/SVs/cod-rnaTwoLevelSvs80%.txt");
		
		
	
		jsc.stop();
		jsc.close();


	}
}
