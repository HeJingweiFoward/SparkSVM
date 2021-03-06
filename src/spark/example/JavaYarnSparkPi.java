package spark.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class JavaYarnSparkPi {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		 System.setProperty("HADOOP_USER_NAME", "hadoop");
	    	 SparkConf sparkConf = new SparkConf().setAppName("JavaYarnSparkPi")
	    			 .setMaster("yarn")
	    			 .set("spark.yarn.dist.files", "E:\\论文实验\\JavaSpark\\src\\yarn-site.xml")
	    			 .set("spark.yarn.jars", "hdfs://192.168.2.151:9000/test/hjw/jars/*");
	        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
	        jsc.addJar("E:\\论文实验\\mjsl1.jar");
	        long start = System.currentTimeMillis();
	        int slices = 10;
	        int n = 100000 * slices;
	        List<Integer> l = new ArrayList<Integer>(n);
	        for (int i = 0; i < n; i++) {
	            l.add(i);
	        }
	        /*
	          JavaSparkContext的parallelize:将一个集合变成一个RDD
	          - 第一个参数一是一个 Seq集合 
	          - 第二个参数是分区数 
	          - 返回的是RDD[T]
	         */
	        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);
	        int count = dataSet.map(new Function<Integer, Integer>() {
	            private static final long serialVersionUID = 1L;
	            public Integer call(Integer integer) {
	                double x = Math.random() * 2 - 1;
	                double y = Math.random() * 2 - 1;
	                return (x * x + y * y < 1) ? 1 : 0;
	            }
	        }).reduce(new Function2<Integer, Integer, Integer>() {
	            private static final long serialVersionUID = 1L;

	            public Integer call(Integer integer, Integer integer2) {
	                return integer + integer2;
	            }
	        });
	        long end = System.currentTimeMillis();
	        System.out.println("Pi is roughly " + 4.0 * count / n+",use : "+(end-start)+"ms");
	        jsc.stop();
	        jsc.close();
	}

}
