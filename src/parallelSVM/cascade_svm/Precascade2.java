package parallelSVM.cascade_svm;
import java.io.IOException;
import java.util.Random;
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

class Precascade2 extends Configured implements Tool {
    private Job job;

    public int run(String[] args) throws Exception {
        job = Job.getInstance(this.getConf(), "Cascade SVM: Partitioning training data, Phase 2");
        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(this.getConf().getInt("SUBSET_COUNT",4));
        job.setMapperClass(PrePartitionerMapper.class);
        job.setReducerClass(SubsetDataOutputReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]+"/tmp"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/layer-input-subsets/layer-1"));
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static class PrePartitionerMapper extends Mapper<Object, Text, IntWritable, Text> {
      Random r = new Random();
      public void map(Object offset, Text v_trainingData,Context context) throws IOException, InterruptedException {
//        final double subsetCount = context.getConfiguration().getInt("SUBSET_COUNT", 2);
//        // get counts of all classes here
//        //final double classCount_1 = context.getConfiguration().getFloat("CLASS_1_COUNT", 2);
//        final double totalRecordCount = context.getConfiguration().getInt("TOTAL_RECORD_COUNT", 2);
//        // final double classRatio = classCount_1/totalRecordCount;
//        final double subsetMaxRecordCount = Math.ceil(totalRecordCount / subsetCount);
//        // final double subsetMaxClassCount_1 = Math.ceil(subsetMaxRecordCount*classRatio);
//        //final double subsetMaxClassCount_2 = subsetMaxRecordCount - subsetMaxClassCount_1 + 1;
//        String dataStr = v_trainingData.toString();
//        String label = dataStr.substring(0, dataStr.indexOf(","));
//        int subsetId = r.nextInt((int) subsetCount);
//        while (context.getCounter("subsetDataStats", "SUBSET_" + subsetId).getValue() >= subsetMaxRecordCount) {
//          subsetId = r.nextInt((int) subsetCount);
//        }
//        context.getCounter("subsetDataStats", "SUBSET_" + subsetId).increment(1);
//        context.write(new IntWritable(subsetId), v_trainingData);
        final double subsetCount = context.getConfiguration().getInt("SUBSET_COUNT", 2);
      	final double classCount_1 = context.getConfiguration().getDouble("CLASS_1_COUNT", 2);
		final double totalRecordCount = context.getConfiguration().getDouble("TOTAL_RECORD_COUNT", 2);

		final double classRatio = classCount_1/totalRecordCount;

		final double subsetMaxRecordCount = Math.ceil(totalRecordCount/subsetCount);
		final double subsetMaxClassCount_1 = Math.ceil(subsetMaxRecordCount*classRatio);
		final double subsetMaxClassCount_2 = subsetMaxRecordCount - subsetMaxClassCount_1 + 1;
		
		String dataStr = v_trainingData.toString();
		String label = dataStr.substring(0, dataStr.indexOf(" "));
		int subsetId = r.nextInt((int)subsetCount);
		
		if(label.equals("+1")) {
			while(context.getCounter("subsetDataStats", "SUBSET_" + subsetId + "_CLASS_1").getValue()
					>= subsetMaxClassCount_1)
				subsetId = r.nextInt((int)subsetCount);
			context.getCounter("subsetDataStats", "SUBSET_" + subsetId + "_CLASS_1").increment(1);
			context.write(new IntWritable(subsetId), v_trainingData);
		} else if(label.equals("-1")) {
			while(context.getCounter("subsetDataStats", "SUBSET_" + subsetId + "_CLASS_2").getValue()
					>= subsetMaxClassCount_2)
				subsetId = r.nextInt((int)subsetCount);
			context.getCounter("subsetDataStats", "SUBSET_" + subsetId + "_CLASS_2").increment(1);
			context.write(new IntWritable(subsetId), v_trainingData);
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
