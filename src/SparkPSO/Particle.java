package SparkPSO;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Particle implements Serializable{
    //维数
    public  int dimension = 2;
    
    //粒子的位置
    public double[] X = new double[dimension];
    
    //局部最好位置
    public double[] pbest = new double[dimension];
    
    //粒子的速度
    public double[] V = new double[dimension];
    
    //最大速度
    public double Vmax = 1;
    
    public double[] getX() {
		return X;
	}


	public void setX(double[] x) {
		X = x;
	}


	public double[] getPbest() {
		return pbest;
	}


	public void setPbest(double[] pbest) {
		this.pbest = pbest;
	}


	public double getFitness() {
		return fitness;
	}


	public void setFitness(double fitness) {
		this.fitness = fitness;
	}

	//适应值
    public double fitness;
    
    //2 -8次方到2 8次方     2 -2次方到2 2次方
	public  double Xmin = Math.pow(2, -2);
	public  double Xmax = Math.pow(2, 2);

    
    
    /**
     * 根据当前位置计算适应值
     * @return newFitness
     * @throws IOException 
     * @throws URISyntaxException 
     */
    public double calculateFitness() throws IOException, URISyntaxException {
    	Vector<String> svRecords = new Vector<String>();
    	FileSystem fs = FileSystem.get(new URI("hdfs://192.168.2.151:9000"), new Configuration());
    	Path pt = new Path(new URI("hdfs://datanode1:9000/SVM/DataSet/a8a"));
		svRecords = ReadTrainFromHDFS(fs, pt);
		String[] svr = svRecords.toArray(new String[svRecords.size()]);  
		MSSvmTrainer svmTrainer = new MSSvmTrainer(svr, X[0],X[1]);
		String acc = svmTrainer.do_cross_validation();
		double newFitness=100-Double.parseDouble(acc);
		System.out.println("                              1-交叉验证准确率："+newFitness);
  
        
        return newFitness;
    }
    
    
    /**
     * 初始化自己的位置和pbest
     */
    public void initialX() {
        for(int i=0;i<dimension;i++) {
            X[i] = Xmin + new Random().nextDouble() * (Xmax - Xmin);
            pbest[i] = X[i];
        }
    }
    
    /**
     * 初始化自己的速度
     */
    public void initialV() {
        for(int i=0;i<dimension;i++) {
      /*      double tmp = new Random().nextDouble();//随机产生一个0~1的随机小数
            V[i] = tmp*4+(-2);*/
        	V[i] =-Vmax + new Random().nextDouble() * (Vmax - (-Vmax));
        }
    }
    
	// 从HDFS读取支持向量样本
	public static Vector<String> ReadTrainFromHDFS(FileSystem fs, Path pt) throws IOException {
		Vector<String> svRecords = new Vector<String>();

		if (fs != null) {
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

			try {
				String line;
				while ((line = br.readLine()) != null && line.length() > 1) {
					svRecords.addElement(line);
				}
			} finally {
				// you should close out the BufferedReader
				br.close();
			}
		}

		return svRecords;

	}
}
