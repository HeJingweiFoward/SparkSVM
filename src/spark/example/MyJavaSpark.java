package spark.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import parallelSVM.MSSvmTrainer;
import scala.Tuple2;

public class MyJavaSpark {
	
public static	Vector<String> ReadTrain(String path) throws IOException {
	    Vector<String> svRecords = new Vector<String>();
	    File file = new File(path);

	    //判断文件是否存在
	        InputStreamReader read = new InputStreamReader(
	                new FileInputStream(file));//考虑到编码格式
	    
		        BufferedReader br=new BufferedReader(read);
		        try {
		            String line;
		            //line=br.readLine();
		            while ((line =br.readLine())!=null&&line.length()>1){
		                 //System.out.println(line);
		                 svRecords.addElement(line);
		              }
		            } finally {
		                // you should close out the BufferedReader
		                br.close();
		            } 
	      
		        System.out.println("--------------------------------------------------");
	     return svRecords;
	 
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

/*		SparkConf sparkConf = new SparkConf().setJars(new String[] { "F:\\研二\\科研\\MyJavaSpark.jar" })
				.set("spark.executor.memory", "1000m");

		SparkSession spark = SparkSession.builder().appName("MyJavaSpark").config(sparkConf)
				.master("spark://192.168.2.151:7077").getOrCreate();*/

		SparkSession spark = SparkSession.builder().appName("MyJavaSpark")
				.master("local[4]").getOrCreate();
		
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaRDD<String> lines = jsc.textFile("E:\\论文实验\\ModelSelectIn");
		
/*		JavaRDD<String>flatMapLines=lines.flatMap(
				new FlatMapFunction<String,String>() {
					public Iterator<String> call(String s) throws Exception {
						// TODO Auto-generated method stub
					      return  Arrays.asList(s.split("-")).iterator();
					}
		}
			 );
System.out.println(flatMapLines.collect());*/
//在这里训练模型



JavaPairRDD<String,String> mapToPairLines=lines.mapToPair(
        new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) throws Exception {
            	
         	   Vector<String> svRecords = new Vector<String>();            	
	      		 svRecords = ReadTrain("E:\\论文实验\\SVM\\a8t");
	      		  String[] ssvRecords = svRecords.toArray(new String[svRecords.size()]);   
			     MSSvmTrainer svmTrainer = new MSSvmTrainer(ssvRecords,Double.parseDouble(s.split("-")[0]),Double.parseDouble(s.split("-")[1]));
			      String acc = svmTrainer.do_cross_validation();
	      		 
                return new Tuple2(s, acc);
            }
        }
);
System.out.println(mapToPairLines.collect());

/*		JavaPairRDD<String, String> ones = lines.mapToPair(s -> new Tuple2<>(System.currentTimeMillis() + "", s));
		List<Tuple2<String, String>> output = ones.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}*/
		
/*
	List counts = ones.reduceByKey(new Function2<String, String, String>() {
		      public String call(String i1, String i2) {		 
		    		System.out.println(i1+i2);
		        return "";
		      
		      }
		    }).collect();*/
/*		List<Tuple2<String, String>> counts = ones.reduceByKey((x,y)->"").collect();
		for (Tuple2<?, ?> tuple : counts) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}*/
	// System.out.println(counts);
		jsc.stop();
		jsc.close();
		spark.stop();
		spark.close();
	}

}
