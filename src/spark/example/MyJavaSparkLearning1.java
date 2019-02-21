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

	// �ӱ��ض�ȡ֧����������
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

	// ��HDFS��ȡ֧����������
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
		SparkConf sparkConf = new SparkConf().setJars(new String[] { "E:\\����ʵ��\\mjsl1.jar" }).set("spark.executor.memory", "1000m").setMaster("spark://192.168.2.151:7077");
		SparkSession spark = SparkSession.builder().appName("MyJavaSpark").config(sparkConf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		System.setProperty("HADOOP_USER_NAME", "hadoop");
		// c,g�����Ĵ洢·��
		String cgPath = "hdfs://192.168.2.151:9000/test/hjw/SvmIn";
		// c,g����������
		int cnum = 6;// c
		int gnum = 6;// g
		// һ���������е�Reduce�������
		int maxReduceTask = 16;
		// �ڼ���ʵ��
		int experimentNum = 0;

		// ����c,g����HDFS��д��c,g
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
		// ��ȡc,g
		// JavaRDD<String> lines = jsc.textFile("E:\\����ʵ��\\ModelSelectIn");
		// ��HDFS��ȡc,g
		JavaRDD<String> lines = jsc.textFile(cgPath);
		System.out.println(lines.collect());
		
		System.out.println("��ʼ��ȡ֧������");
		Vector<String> svRecords = new Vector<String>();
		Path pt = new Path(new URI("hdfs://datanode1:9000/SVM/DataSet/a8a"));
		// ��HDFS��ȡѵ������
		svRecords = ReadTrainFromHDFS(fs, pt);
		String[] ssvRecords = svRecords.toArray(new String[svRecords.size()]);
		System.out.println("��ʼ��������");
		// ��ʼ��������������������֤
		JavaPairRDD<String, String> mapToPairLines = lines.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) throws Exception {

				// svRecords = ReadTrain("E:\\����ʵ��\\SVM\\a8t");
				// svRecords = ReadTrain("E:\\����ʵ��\\a8trained");
				System.out.println("��ʼ������֤");
				MSSvmTrainer svmTrainer = new MSSvmTrainer(ssvRecords, Double.parseDouble(s.split("-")[0]),
						Double.parseDouble(s.split("-")[1]));
				String acc = svmTrainer.do_cross_validation();
				System.out.println("������֤����");
				return new Tuple2(s, acc);
			}
		});

		// System.out.println("mapToPairLines" + mapToPairLines.collect());

		List<Tuple2<String, String>> list = mapToPairLines.collect();
		long endTime=System.currentTimeMillis();		
		System.out.println("����������������ʱ��"+(endTime-startTime));
		
		System.out.println("mapToPairLines" + list);
		double max = Double.MIN_VALUE;
		for (Tuple2<String, String> tuple2 : list) {

			if (Double.parseDouble(tuple2._2().replace("%", "")) > max) {
				max = Double.parseDouble(tuple2._2().replace("%", ""));
			}
		}
		System.out.println("���׼ȷ�ʣ�"+max+"%");

		/*
		 * JavaPairRDD<String, String> mapTest= lines.mapToPair(row->{ return
		 * new Tuple2(""+TaskContext.getPartitionId(), row); });
		 * 
		 * JavaPairRDD<String, String>reduceTest=mapTest.reduceByKey((x,y)->{
		 * return x+y+"����Reduce"; //return x+"����Reduce"; });
		 * 
		 * System.out.println("map"+mapTest.collect());
		 * System.out.println("reduce"+reduceTest.collect());
		 */
	}
}
