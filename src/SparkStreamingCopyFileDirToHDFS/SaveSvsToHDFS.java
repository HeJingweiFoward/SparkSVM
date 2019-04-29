package SparkStreamingCopyFileDirToHDFS;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;

public class SaveSvsToHDFS implements Serializable{
	
	public  SaveSvsToHDFS(/*FileSystem fileSystem,String hdfsPath, svm_model model*/) throws IOException, URISyntaxException {
		//SvmSaveSvsToHDFS(fileSystem, hdfsPath, model);
		
	}
	
	private static final String svm_type_table[] = {
        	"c_svc","nu_svc","one_class","epsilon_svr","nu_svr",
    	};

	static final String kernel_type_table[]= {
                "linear","polynomial","rbf","sigmoid","precomputed"
    	};
	
	public  void SvmSaveModelToHDFS(svm_model model,Configuration configuration,String modelHDFSPath) throws IOException, URISyntaxException
	{
		try {
			
		    FileSystem fs = FileSystem.get(new URI("hdfs://datanode1:9000"),configuration);
			//String pathStrModel  = "/test/hjw/model/model.txt";	
		    
		    SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		    String pathStrModel  =modelHDFSPath+df.format(new Date())+".txt";
		    
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

	
	
	public List<String> SaveSvsToList(svm_model model)
	{
		svm_parameter param = model.param;

		double[][] sv_coef = model.sv_coef;
		svm_node[][] SV = model.SV;

		int nr_class = model.nr_class;
		int l = model.l;
		
		List<String> Svs=new ArrayList<String>();
		
		for(int i=0;i<l;i++)
		{
			String sv="";
			for(int j=0;j<nr_class-1;j++)
			{
	
				//sv=sv+sv_coef[j][i]+" ";
				if (sv_coef[j][i]==1.0) {
					sv=sv+"+1"+" ";
				}
				else {
					sv=sv+"-1"+" ";
				}
				
				//System.out.println("sv_coef[j][i]:"+sv_coef[j][i]);
			}
			svm_node[] p = SV[i];
			/*if(param.kernel_type == svm_parameter.PRECOMPUTED)
			{
				sv=sv+"0:"+(int)(p[0].value);
			}
			else
			{*/
				for(int j=0;j<p.length;j++)
				{
	
					sv=sv+p[j].index+":"+p[j].value+" ";
				}
		/*	}*/
			Svs.add(sv);
		}
		return Svs;
	}
	
	public List<String> SaveSvsBysv_indices(svm_model model)
	{
		List<String> Svs=new ArrayList<String>();
		
		return Svs;
	}
	
	
	public void SaveSvsCGToHDFS(svm_model model,Configuration configuration,	List<String>Svs,String svscgHDFSPath) throws IOException, URISyntaxException {
	    FileSystem fs = FileSystem.get(new URI("hdfs://datanode1:9000"),configuration);
	    SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
	    String path  =svscgHDFSPath+df.format(new Date())+".txt";
		Path file = new Path(path);
		FSDataOutputStream fos = fs.create(file,true);
		fos.writeBytes("c:"+model.param.C+"       g:"+model.param.gamma+"\n");
		for (String sv : Svs) {
			fos.writeBytes(sv+"\n");
		}
		
		fos.close();
	}
	
}
