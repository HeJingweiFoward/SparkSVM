package SparkStreamingCopyFileDirToHDFS;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

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
	
	public  void SvmSaveSvsToHDFS(FileSystem fileSystem,String hdfsPath, svm_model model) throws IOException, URISyntaxException
	{
		FSDataOutputStream fsDataOutputStream=null;
		 Path path = new Path(new URI( hdfsPath));
		 fsDataOutputStream=fileSystem.create(path, true);
		 
		svm_parameter param = model.param;

		double[][] sv_coef = model.sv_coef;
		svm_node[][] SV = model.SV;

		int nr_class = model.nr_class;
		int l = model.l;
		
		for(int i=0;i<l;i++)
		{
			for(int j=0;j<nr_class-1;j++)
				fsDataOutputStream.writeBytes(sv_coef[j][i]+" ");

			svm_node[] p = SV[i];
			if(param.kernel_type == svm_parameter.PRECOMPUTED)
				fsDataOutputStream.writeBytes("0:"+(int)(p[0].value));
			else
				for(int j=0;j<p.length;j++)
					fsDataOutputStream.writeBytes(p[j].index+":"+p[j].value+" ");
			fsDataOutputStream.writeBytes("\n");
		}
		fsDataOutputStream.flush();
		fsDataOutputStream.close();
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
}
