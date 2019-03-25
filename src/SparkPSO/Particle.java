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
    //ά��
    public  int dimension = 2;
    
    //���ӵ�λ��
    public double[] X = new double[dimension];
    
    //�ֲ����λ��
    public double[] pbest = new double[dimension];
    
    //���ӵ��ٶ�
    public double[] V = new double[dimension];
    
    //����ٶ�
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

	//��Ӧֵ
    public double fitness;
    
    //2 -8�η���2 8�η�     2 -2�η���2 2�η�
	public  double Xmin = Math.pow(2, -2);
	public  double Xmax = Math.pow(2, 2);

    
    
    /**
     * ���ݵ�ǰλ�ü�����Ӧֵ
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
		System.out.println("                              1-������֤׼ȷ�ʣ�"+newFitness);
  
        
        return newFitness;
    }
    
    
    /**
     * ��ʼ���Լ���λ�ú�pbest
     */
    public void initialX() {
        for(int i=0;i<dimension;i++) {
            X[i] = Xmin + new Random().nextDouble() * (Xmax - Xmin);
            pbest[i] = X[i];
        }
    }
    
    /**
     * ��ʼ���Լ����ٶ�
     */
    public void initialV() {
        for(int i=0;i<dimension;i++) {
      /*      double tmp = new Random().nextDouble();//�������һ��0~1�����С��
            V[i] = tmp*4+(-2);*/
        	V[i] =-Vmax + new Random().nextDouble() * (Vmax - (-Vmax));
        }
    }
    
	// ��HDFS��ȡ֧����������
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
