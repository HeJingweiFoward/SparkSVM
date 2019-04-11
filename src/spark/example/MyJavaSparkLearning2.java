package spark.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.LongAccumulator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import parallelSVM.MSSvmTrainer;
import scala.Enumeration.Val;
import scala.Tuple2;

public class MyJavaSparkLearning2 {

	// ��HDFS��ȡ֧����������
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
				.setJars(new String[] { "E:\\����ʵ��\\mjsl1.jar" })
				.set("spark.num.executors", "4")
				.set("spark.executor.cores", "3")
				//.set("spark.default.parallelism", "12")
				.set("spark.executor.memory", "2048m")
				.set("spark.network.timeout", "300")
				//.set("spark.storage.memoryFraction", "0.5")//��������ڴ�
				//.set("spark.memory.storageFraction", "0.5")//��������ڴ�
			/*	.setMaster("local[4]");*/
				.setMaster("spark://192.168.2.151:7077");
		SparkSession spark = SparkSession.builder().appName("MyJavaSpark2").config(sparkConf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		System.setProperty("HADOOP_USER_NAME", "hadoop");
		// c,g�����Ĵ洢·��
/*		String cgPath = "hdfs://192.168.2.151:9000/test/hjw/SvmIn";*/
		// c,g����������
		int cnum = 8;// c
		int gnum = 8;// g


	    /**
	     * ��������
	     */
	
		  org.apache.spark.util.LongAccumulator accumulator = jsc.sc().longAccumulator();
	    
		// ����c,g����HDFS��д��c,g
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.2.151:9000"), new Configuration());
		/*
		 * int count = 0; for (int i = 0; i < cnum; i++) { for (int j = gnum; j
		 * > 0; j--) { String sparam = String.valueOf(0.5 + i * 0.25) + "-" +
		 * String.valueOf(0.05 + j * 0.0125); count++; Path path = new
		 * Path(cgPath + "/" + String.valueOf(count)); FSDataOutputStream out =
		 * fs.create(path, true); out.writeBytes(sparam); out.flush();
		 * out.close(); } }
		 */
		// ���Դ�һ���ļ���ȡ���� ����ֱ�ӽ�����д��RDD��DataFrame
		List<String> cgList = new ArrayList<String>();
		for (int i = 0; i < cnum; i++) {
			for (int j = gnum; j > 0; j--) {
				String sparam = String.valueOf(0.5 + i * 0.25) + "-" + String.valueOf(0.05 + j * 0.0125);
			/*	String sparam = String.valueOf(i) + "-" +  String.valueOf(Math.pow(10,-j));*/
				cgList.add(sparam);
			}
		}

		// ��HDFS��ȡc,g
		// JavaRDD<String> lines = jsc.textFile(cgPath);
		// ֱ�Ӵ�List��ȡc,g
		JavaRDD<String> lines = jsc.parallelize(cgList,64);
/*		System.out.println(lines.collect());*/

		System.out.println("��ʼ��ȡ֧������");
		Vector<String> svRecords = new Vector<String>();
		/*Path pt = new Path(new URI("hdfs://datanode1:9000/SVM/DataSet/covtypebinaryscale"));*/
		Path pt = new Path(new URI("hdfs://datanode1:9000/SVM/DataSet/a8a"));
		// ��HDFS��ȡѵ������
		svRecords = ReadTrainFromHDFS(fs, pt);
		// String[] ssvRecords = svRecords.toArray(new
		// String[svRecords.size()]);
		// ʹ�ù㲥����
		Broadcast<List<String>> broadcastssvRecords = jsc
				.broadcast(Arrays.asList(svRecords.toArray(new String[svRecords.size()])));

		System.out.println("��ʼ��������");
		// ��ʼ��������������������֤
		JavaPairRDD<String, String> mapToPairLines = lines.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) throws Exception {

				System.out.println("��ʼ������֤");
				// ʹ�ù㲥�����е�֧������
				String[] svr = (String[]) broadcastssvRecords.value().toArray();
				Double c=Double.valueOf(s.split("-")[0]);
				Double g=Double.valueOf(s.split("-")[1]);
				
				MSSvmTrainer svmTrainer = new MSSvmTrainer(svr, c,g);
				/*
				 * MSSvmTrainer svmTrainer = new MSSvmTrainer(ssvRecords,
				 * Double.parseDouble(s.split("-")[0]),
				 * Double.parseDouble(s.split("-")[1]));
				 */
				//����
				accumulator.add(1);
				String acc = svmTrainer.do_cross_validation();
				System.out.println("������֤����");
				return new Tuple2(s, acc);
			}
		});

		/*
		 * List<Tuple2<String, String>> list = mapToPairLines.collect(); long
		 * endTime=System.currentTimeMillis();
		 * System.out.println("����������������ʱ��"+(endTime-startTime));
		 * 
		 * System.out.println("mapToPairLines" + list); double max =
		 * Double.MIN_VALUE; for (Tuple2<String, String> tuple2 : list) {
		 * 
		 * if (Double.parseDouble(tuple2._2().replace("%", "")) > max) { max =
		 * Double.parseDouble(tuple2._2().replace("%", "")); } }
		 * System.out.println("���׼ȷ�ʣ�"+max+"%");
		 */

		// ʹ��sort()����û�ã�ֻ�ܰ���key����
		// mapToPairLines.sortByKey();
		// ʹ��reduce����
		Tuple2<String, String> maxACC = mapToPairLines.reduce((x, y) -> {
			if (Double.parseDouble(x._2().replace("%", "")) > Double.parseDouble(y._2().replace("%", ""))) {
				return x;
			} else {
				return y;
			}
		});
		long endTime = System.currentTimeMillis();
		System.out.println("����������������ʱ��" + (endTime - startTime));
		System.out.println("���׼ȷ�ʣ�" + maxACC);
		System.out.println("��"+accumulator.value()+"�����");
		// ɾ�������ļ�
		/*
		 * for(int k = 1;k<=count;k++) fs.delete(new Path(cgPath +"/" +
		 * String.valueOf(k)),true);
		 */
	}
}
