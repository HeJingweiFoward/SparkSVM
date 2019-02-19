package parallelSVM.cascade_svm;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Vector;
import libsvm.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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

class Lastcascade extends Configured implements Tool{
    private Job job;
    public int run(String[] args) throws Exception {
      job= Job.getInstance(this.getConf(), "Cascade SVM: Layer last  " + args[2]);
      job.addFileToClassPath(new Path("hdfs://datanode1:9000/SVM/libsvm.jar"));
      job.setJarByClass(this.getClass());
      job.setNumReduceTasks((int)(getConf().getInt("SUBSET_COUNT",2)/Math.pow(2,Integer.parseInt(args[2]))));
      job.setMapperClass(SubSvmMapper.class);
      job.setReducerClass(LastLayerSvmModelOutputReducer.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(Text.class);
      job.setInputFormatClass(TrainingSubsetInputFormat.class);
    	int x=Integer.parseInt(args[2]);
	    x++;
      FileInputFormat.addInputPath(job, new Path(args[1]+"/layer-input-subsets/layer-"+args[2]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]+"/layer-input-subsets/layer-"+x));
      return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class LastLayerSvmModelOutputReducer extends Reducer<IntWritable, Text, NullWritable, Text>{

      private static final String svm_type_table[] = {
        "c_svc","nu_svc","one_class","epsilon_svr","nu_svr",
      };

      static final String kernel_type_table[]= {
        "linear","polynomial","rbf","sigmoid","precomputed"
      };
      private Text supportVector = new Text();   
      // An identical implementation of svm.svm_save_model in LIBSVM,
      // different in that the file is saved to HDFS instead of a local path.
      private void saveModelToHdfs(svm_model model, String pathStr, Context context) throws URISyntaxException{
        try {
          FileSystem fs = FileSystem.get(new URI("hdfs://datanode1:9000"),context.getConfiguration());
		  String pathStrModel  = "/SVM/BCmodel";				
		  Path file = new Path(pathStrModel);
		  FSDataOutputStream fos = fs.create(file,true);
          svm_parameter param = model.param;
          fos.writeBytes("svm_type "+svm_type_table[param.svm_type]+"\n");
          fos.writeBytes("kernel_type "+kernel_type_table[param.kernel_type]+"\n");
          if(param.kernel_type == svm_parameter.POLY)
            fos.writeBytes("degree "+param.degree+"\n");
          if(param.kernel_type == svm_parameter.POLY ||
                  param.kernel_type == svm_parameter.RBF ||
                  param.kernel_type == svm_parameter.SIGMOID)
            fos.writeBytes("gamma "+param.gamma+"\n");
          if(param.kernel_type == svm_parameter.POLY ||
                  param.kernel_type == svm_parameter.SIGMOID)
            fos.writeBytes("coef0 "+param.coef0+"\n");
          int nr_class = model.nr_class;
          int l = model.l;
          fos.writeBytes("nr_class "+nr_class+"\n");
          fos.writeBytes("total_sv "+l+"\n");
          fos.writeBytes("rho");
          for(int i=0;i<nr_class*(nr_class-1)/2;i++)
              fos.writeBytes(" "+model.rho[i]);
          fos.writeBytes("\n");
          if(model.label != null) {
            fos.writeBytes("label");
            for(int i=0;i<nr_class;i++)
                fos.writeBytes(" "+model.label[i]);
            fos.writeBytes("\n");
          }
          if(model.probA != null) { // regression has probA only
            fos.writeBytes("probA");
            for(int i=0;i<nr_class*(nr_class-1)/2;i++)
              fos.writeBytes(" "+model.probA[i]);
            fos.writeBytes("\n");
          }
          if(model.probB != null) {
            fos.writeBytes("probB");
            for(int i=0;i<nr_class*(nr_class-1)/2;i++)
              fos.writeBytes(" "+model.probB[i]);
            fos.writeBytes("\n");
          }
          if(model.nSV != null) {
            fos.writeBytes("nr_sv");
            for(int i=0;i<nr_class;i++)
              fos.writeBytes(" "+model.nSV[i]);
            fos.writeBytes("\n");
          }

          fos.writeBytes("SV\n");
          double[][] sv_coef = model.sv_coef;
          svm_node[][] SV = model.SV;

          for(int i=0;i<l;i++) {
            for(int j=0;j<nr_class-1;j++)
              fos.writeBytes(sv_coef[j][i]+" ");
              svm_node[] p = SV[i];
              if(param.kernel_type == svm_parameter.PRECOMPUTED)
                fos.writeBytes("0:"+(int)(p[0].value));
              else
                for(int j=0;j<p.length;j++)
                  fos.writeBytes(p[j].index+":"+p[j].value+" ");
                fos.writeBytes("\n");
          }

          fos.close();

        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
     

      public void reduce(IntWritable subsetId, Iterable<Text> extractedSvs,Context context) throws IOException, InterruptedException {
        Vector<String> svRecordsAsVector = new Vector<String>();
        for(Text sv : extractedSvs)
          svRecordsAsVector.addElement(sv.toString());
        String[] svRecords = svRecordsAsVector.toArray(new String[svRecordsAsVector.size()]);

        SvmTrainer svmTrainer = new SvmTrainer(svRecords);
        svm_model model = svmTrainer.train();

        String userOutputPathStr = context.getConfiguration().get("USER_OUTPUT_PATH");
        try {
			saveModelToHdfs(model,userOutputPathStr,context);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        int[] svIndices = model.sv_indices;

        for(int i=0; i<svIndices.length; i++) {
            supportVector.set(svRecords[svIndices[i]-1]);
            context.write(NullWritable.get(), supportVector);
        } 
      }
    }

    public static class SubSvmMapper extends Mapper<Object, Text, IntWritable, Text>{
      private Text supportVector = new Text();
      private IntWritable partitionIndex = new IntWritable();

      public void map(Object offset, Text wholeSubset,Context context) throws IOException, InterruptedException {
        String[] subsetRecords = wholeSubset.toString().split("\n");
        SvmTrainer svmTrainer = new  SvmTrainer(subsetRecords);
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
  }


