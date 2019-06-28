package SparkStreaming;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import SparkStreamingCopyFileDirToHDFS.SaveSvsToHDFS;
import libsvm.svm_model;

public class TwoLevelNoCacheCascadeSVM {
	public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
		
		 
		String sampleHDFSPath=args[0];
		String modelHDFSPath=args[1];
		String testHDFSPath=args[2];
		String predictHDFSPath=args[3];
		String svscgHDFSPath=args[4];
		
		SparkConf conf = new SparkConf().setAppName("TwoLevelNOCacheCascadeSVM").setJars(new String[] { "E:\\论文实验\\mjsl1.jar" })
				.set("spark.network.timeout", "300")
				.set("spark.num.executors", "4")
				.set("spark.executor.cores", "3")
				.set("spark.executor.memory", "2048m")
				.set("spark.default.parallelism", "12")
				.set("spark.executor.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
				.setMaster("spark://192.168.2.151:7077");
		JavaSparkContext sc = new JavaSparkContext(conf);
	
		//设置日志运行级别
		sc.setLogLevel("WARN");
		
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.2.151:9000"),new Configuration());    
		JavaRDD<String> lines = sc.textFile(sampleHDFSPath).repartition(12);
		double startTime=System.currentTimeMillis();
		
		JavaRDD<String> svs1 = lines.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
			private static final long serialVersionUID = 6225255971065755198L;
			Vector<String> svRecords = new Vector<String>();
			List<String>Svs=new ArrayList<String>();
			@Override
			public Iterator<String> call(Iterator<String> arg0) throws Exception {
				// TODO Auto-generated method stub
				for (Iterator<String> it=arg0;it.hasNext();) {
					svRecords.addElement(it.next());
				} 
				if (svRecords.size()>0) {
					String[] svRecordArr=svRecords.toArray(new String[svRecords.size()]);
					MSSvmTrainer mSvmTrainer=new MSSvmTrainer(svRecordArr);
					svm_model model= mSvmTrainer.train();
					int[] svIndices = model.sv_indices;
					for(int i=0; i<svIndices.length; i++) {
						Svs.add(svRecordArr[svIndices[i]-1]);
            		}
					System.out.println("第一层每个分区支持向量数量Svs.size()："+Svs.size());
				}
				return Svs.iterator();
			}
		});
		
		//6    ---   4
		JavaRDD<String>svs2=svs1.repartition(6).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 453291289524473319L;
			Vector<String> svRecords = new Vector<String>();
			List<String>Svs=new ArrayList<String>();
			@Override
			public Iterator<String> call(Iterator<String> arg0) throws Exception {
				// TODO Auto-generated method stub
				for (Iterator<String> it=arg0;it.hasNext();) {
					svRecords.addElement(it.next());
				} 
				if (svRecords.size()>0) {
					String[] svRecordArr=svRecords.toArray(new String[svRecords.size()]);
					MSSvmTrainer mSvmTrainer=new MSSvmTrainer(svRecordArr);
					svm_model model= mSvmTrainer.train();
					/*repartitionNum.add(1);*/
					int[] svIndices = model.sv_indices;
					for(int i=0; i<svIndices.length; i++) {
						Svs.add(svRecordArr[svIndices[i]-1]);
            		}				
					System.out.println("第二层每个分区支持向量数量Svs.size()："+Svs.size());
				}
				return Svs.iterator();
			}
		});

		System.out.println("svs2.count():"+svs2.count());
		
		//svs2.cache().repartition(1).saveAsTextFile(svscgHDFSPath);
		
		//List和广播变量在map内应该是只读，不能对其进行add操作
	
		JavaRDD<String>twoLevelSvs= svs2.repartition(1);
		// lable:1
		JavaRDD<String>PositiveSvs= twoLevelSvs.mapPartitions(new  FlatMapFunction<Iterator<String>, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 2300783931514320728L;
			List<String> PositiveRecords = new ArrayList<String>();
			@Override
			public Iterator<String> call(Iterator<String> arg0) throws Exception {
				// TODO Auto-generated method stub
				List<String>svs= IteratorUtils.toList(arg0);
		/*		for (Iterator<String> it=arg0;it.hasNext();) {
					if (it.next().split(" ")[0].equals("1")) {
						PositiveRecords.add(it.next());
					}			
				} 	*/	
				for (String string : svs) {
					if (!string.split(" ")[0].equals("-1")) {
						PositiveRecords.add(string);
					}	
				}
				return PositiveRecords.iterator();
			}
		});
		
		//lable: -1
		JavaRDD<String>NegativeSvs=twoLevelSvs.mapPartitions(new  FlatMapFunction<Iterator<String>, String>() {			
			/**
			 * 
			 */
			private static final long serialVersionUID = -8441737558968376929L;
			List<String> NegativeRecords = new ArrayList<String>();
			@Override
			public Iterator<String> call(Iterator<String> arg0) throws Exception {
				// TODO Auto-generated method stub
				
				List<String>svs= IteratorUtils.toList(arg0);
				
		/*		for (Iterator<String> it=arg0;it.hasNext();) {
					if (it.next().split(" ")[0].equals("-1")) {
						NegativeRecords.add(it.next());
					}			
				} 	*/	
				for (String string : svs) {
					if (string.split(" ")[0].equals("-1")) {
						NegativeRecords.add(string);
					}	
				}
				
				return NegativeRecords.iterator();
			}
		});
				
		System.out.println("PositiveSvs Size:"+PositiveSvs.count()+";NegativeSvs Size:"+NegativeSvs.count());
		
		
		JavaRDD<String>Positive80TrainSet= PositiveSvs.sample(false, 0.8);
		JavaRDD<String>Negative80TrainSet= NegativeSvs.sample(false, 0.8);
		
		JavaRDD<String>svs3=Positive80TrainSet.union(Negative80TrainSet).repartition(1).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = -6664536116678651770L;
			Vector<String> svRecords = new Vector<String>();
			List<String>Svs=new ArrayList<String>();
			@Override
			public Iterator<String> call(Iterator<String> arg0) throws Exception {
				// TODO Auto-generated method stub
				for (Iterator<String> it=arg0;it.hasNext();) {
					svRecords.addElement(it.next());
				} 
				if (svRecords.size()>0) {
					String[] svRecordArr=svRecords.toArray(new String[svRecords.size()]);
					MSSvmTrainer mSvmTrainer=new MSSvmTrainer(svRecordArr);
					svm_model model= mSvmTrainer.train();
					SaveSvsToHDFS saveSvsToHDFS=new SaveSvsToHDFS();
					Configuration configuration=new Configuration();
					saveSvsToHDFS.SvmSaveModelToHDFS(model, configuration,modelHDFSPath);
					//预测
				/*	ModifiedPredict.CascadePredict(configuration, model, testHDFSPath, predictHDFSPath);*/
					//支持向量，c和g
					int[] svIndices = model.sv_indices;
					for(int i=0; i<svIndices.length; i++) {
						Svs.add(svRecordArr[svIndices[i]-1]);
            		}	
					saveSvsToHDFS.SaveSvsCGToHDFS(model, configuration, Svs, svscgHDFSPath);
					System.out.println("第三层每个分区支持向量数量Svs.size()："+Svs.size());
				}
				return Svs.iterator();
			}
		});
		
		System.out.println("svs3 count:"+svs3.count());
		double endTime=System.currentTimeMillis();	
		System.out.println("总用时:"+(endTime-startTime)/1000);
		
		sc.stop();
		sc.close();
	}
}
