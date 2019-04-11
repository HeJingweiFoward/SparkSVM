package SparkStreamingCopyFileDirToHDFS;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import libsvm.svm;
import libsvm.svm_print_interface;
public class Copy {

    String localDir = "";
    String hdfsDir = "";
	Configuration conf = new Configuration();
	public Copy(String localPath,String hdfsPath)
	{
		localDir=localPath;
		hdfsDir=hdfsPath;
		CopyFileToHDFS();
	}
	
    public void CopyFileToHDFS() {
        try{
            Path localPath = new Path(localDir);
            Path hdfsPath = new Path(hdfsDir);
            FileSystem hdfs = FileSystem.get(conf);
            if(!hdfs.exists(hdfsPath)){
                 hdfs.mkdirs(hdfsPath);
             }
             hdfs.copyFromLocalFile(localPath, hdfsPath);
         }catch(Exception e){
         e.printStackTrace();
         }
	}

	
}
