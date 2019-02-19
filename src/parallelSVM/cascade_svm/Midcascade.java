package parallelSVM.cascade_svm;

import java.io.IOException;
import java.net.URI;
import libsvm.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import parallelSVM.io.*;
import parallelSVM.SvmTrainer;

class Midcascade extends Configured implements Tool{
    private Job job;
    
    public int run(String[] args) throws Exception {
        job= Job.getInstance(this.getConf(), "Cascade SVM: Layer " + args[2]);
        job.addFileToClassPath(new Path("hdfs://datanode1:9000/SVM/libsvm.jar"));
        job.setJarByClass(this.getClass());
        job.setNumReduceTasks((int)(getConf().getInt("SUBSET_COUNT",2)/Math.pow(2,Integer.parseInt(args[2]))));
        job.setMapperClass(SubSvmMapper.class);
        job.setReducerClass(SubsetDataOutputReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TrainingSubsetInputFormat.class);
        int x=Integer.parseInt(args[2]);
        x++; 
        FileInputFormat.addInputPath(job, new Path(args[1]+"/layer-input-subsets/layer-"+Integer.parseInt(args[2])));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/layer-input-subsets/layer-"+x));
        return job.waitForCompletion(true) ? 0 : 1;
    }

   
    public static class SubSvmMapper extends Mapper<Object, Text, IntWritable, Text>{

      private Text supportVector = new Text();
      private IntWritable partitionIndex = new IntWritable();
      
      public void map(Object offset, Text wholeSubset,Context context) throws IOException, InterruptedException {
        String[] subsetRecords = wholeSubset.toString().split("\n");
        SvmTrainer svmTrainer = new SvmTrainer(subsetRecords);
		svm_model model = svmTrainer.train();        
        int[] svIndices = model.sv_indices;        
       
        for(int i=0; i<svIndices.length; i++) {
          supportVector.set(subsetRecords[svIndices[i]-1]);
          int taskId = context.getTaskAttemptID().getTaskID().getId();
          partitionIndex.set((int)Math.floor(taskId/2));
          context.write(partitionIndex, supportVector);
        } 
      }
    }

    public static class SubsetDataOutputReducer extends Reducer<IntWritable, Text, NullWritable, Text>{
      public void reduce(IntWritable subsetId, Iterable<Text> v_subsetTrainingDataset,Context context) throws IOException, InterruptedException {
          for(Text trainingData : v_subsetTrainingDataset)
              context.write(NullWritable.get(), trainingData);
      }
    }
  }

