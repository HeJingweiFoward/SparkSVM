/**
 * Created by pateu14 (patel udita)on 4/10/2016.
 */
package parallelSVM.cascade_svm;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class Driver  {
    private static final Logger LOG = Logger.getLogger(Driver.class);

    public static void main(String[] args) throws Exception {

        Configuration firstConf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(firstConf, args).getRemainingArgs();
        // otherArgs[0]: input path otherArgs[1]: output path otherArgs[2]: num of subset power of 2
        if (otherArgs.length < 2) {
            System.err.println("Usage: cascade-svm <in> <out> <subsets>");
            System.exit(2);
        }

        final double subsets = Double.valueOf(otherArgs[2]);

        if (subsets % 2 != 0){
            System.err.println("The number of subsets for a binary cascade svm should be a power of 2.");
            System.exit(2);
        }
        
      //删除在hdfs上已存在的目录   
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://datanode1:9000"),new Configuration());          
        Path path1 = new Path(args[1] + "/tmp");  
        Path path2 =  new Path(args[1] + "/layer-input-subsets");
        
        if(fileSystem.exists(path1)){  
            fileSystem.delete(path1,true);  
        }  
        
        if(fileSystem.exists(path2)){  
            fileSystem.delete(path2,true);  
        }          
       
        
        firstConf.setInt("SUBSET_COUNT",(int)subsets);	// set a Int global value
        // pre-partition job count is two for two map-reds that partitions the data
        final int prepartitionJobCount = 2;
        final int cascadeJobCount = (int)(Math.log(subsets)/Math.log(2));

        Configuration[] prepartitionConfs = new Configuration[prepartitionJobCount];
        prepartitionConfs[0] = firstConf;        
        Precascade1 pre1=new Precascade1();
        int res1 = ToolRunner.run(prepartitionConfs[0],pre1, otherArgs);
        System.out.println("The mapper exited with : "+res1);
        
        prepartitionConfs[1] = new Configuration();
        prepartitionConfs[1].setInt("SUBSET_COUNT",(int)subsets);
        prepartitionConfs[1].setInt("TOTAL_RECORD_COUNT",
                (int)pre1.getJob().getCounters().findCounter("trainingDataStats","TOTAL_RECORD_COUNT").getValue());
        
        prepartitionConfs[1].setInt("CLASS_1_COUNT",
				(int)pre1.getJob().getCounters().findCounter("trainingDataStats","CLASS_1_COUNT").getValue());
		prepartitionConfs[1].setInt("CLASS_2_COUNT",
				(int)pre1.getJob().getCounters().findCounter("trainingDataStats","CLASS_2_COUNT").getValue());
		
        Precascade2 pre2=new Precascade2();
        int res2= ToolRunner.run(prepartitionConfs[1],pre2,otherArgs);

        /*** cascade job starts ***/

        Configuration[] cascadeConfs = new Configuration[cascadeJobCount];

	      for(int i=0;i<cascadeJobCount;i++){
            cascadeConfs[i]=new Configuration();
        }

        cascadeConfs[cascadeJobCount-1].set("USER_OUTPUT_PATH", otherArgs[1]);
        for(int remainingConfs = 0; remainingConfs < cascadeJobCount; remainingConfs++) {
            cascadeConfs[remainingConfs] = new Configuration();
            cascadeConfs[remainingConfs].setInt("SUBSET_COUNT",(int)subsets);	        
        }
        
        String[] cargs=new String[otherArgs.length];
        for(int i=0;i<otherArgs.length-1;i++){
            cargs[i]=otherArgs[i];
        }
        for(int jobItr = 0; jobItr < cascadeJobCount; jobItr++){
            System.out.println("===== Beginning job for layer " + (jobItr+1) + " =====");
            cargs[2]=String.valueOf((jobItr+1));
	          System.out.println("===== Beginning job for layer " + cargs[2] + " =====");
            if(jobItr != cascadeJobCount-1){
                ToolRunner.run(cascadeConfs[jobItr],new Midcascade(),cargs);
            } else {
                ToolRunner.run(cascadeConfs[jobItr],new Lastcascade(),cargs);
            }
        }
        
        Predict.main(new String[] {"/SVM/breastCancerTest","/SVM/BCmodel","/SVM/predict"});
        
        System.exit(res2);
    }
}
