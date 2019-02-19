package parallelSVM.io;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class TrainingSubsetInputFormat extends FileInputFormat<NullWritable, Text> {
	
	@Override
	public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) 
		throws IOException, InterruptedException {
		TrainingSubsetRecordReader reader = new TrainingSubsetRecordReader();
		reader.initialize(split, context);
		return reader;
	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
}
