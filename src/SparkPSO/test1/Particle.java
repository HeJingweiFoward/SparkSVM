package SparkPSO.test1;

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

import org.apache.derby.vti.VTITemplate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Particle implements Serializable {
	// ά��
	public int dimension = 2;

	// ���ӵ�λ��
	public double[] X = new double[dimension];

	// �ֲ����λ��
	public double[] pbest = new double[dimension];

	// ���ӵ��ٶ�
	public double[] V = new double[dimension];

	// ����ٶ�
	public double Vmax = 2;
	//��С�ٶ�
	public double Vmin=0.2;

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

	// ��Ӧֵ
	public double fitness;

	// 2 -8�η���2 8�η� 2 -2�η���2 2�η� 2 -6�η���2 6�η� 2 -7�η���2 7�η�
	//public double Xmin = Math.pow(2, -8);
	//public double Xmax = Math.pow(2, 8);//ͨ����һ�ε���ȷ���ڶ��ε�������Χ

	public double Cmin=Math.pow(2, -5);
	public double Cmax=Math.pow(2, 14);
	
	public double Gmin=Math.pow(2, -15);
	public double Gmax=Math.pow(2, -4);
	
	/**
	 * ���ݵ�ǰλ�ü�����Ӧֵ
	 * 
	 * @return newFitness
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public double calculateFitness() throws IOException, URISyntaxException {
		Vector<String> svRecords = new Vector<String>();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.2.151:9000"), new Configuration());
		/*
		 * Path pt = new Path(new URI("hdfs://datanode1:9000/SVM/DataSet/a8a"));
		 */
		Path pt = new Path(new URI("hdfs://datanode1:9000/test/hjw/sample/a5a"));
		svRecords = ReadTrainFromHDFS(fs, pt);
		String[] svr = svRecords.toArray(new String[svRecords.size()]);
		MSSvmTrainer svmTrainer = new MSSvmTrainer(svr, X[0], X[1]);
		String acc = svmTrainer.do_cross_validation();
		double newFitness = 100 - Double.parseDouble(acc);
		System.out.println("                              1-������֤׼ȷ�ʣ�" + newFitness);

		return newFitness;
	}

	/**
	 * ��ʼ���Լ���λ�ú�pbest
	 */
	double c[] = new double[20];
	int cInit = -5;
	double g[] = new double[20];
	int gInit = -15;

	public void initialcgArr() {
		for (int i = 0; i < 20; i++) {
			c[i] = Math.pow(2, cInit);
			cInit++;
		}

		for (int i = 0; i < 20; i++) {
			g[i] = Math.pow(2, gInit);
			gInit++;
		}
	}

	public void initialX() {

/*		for (int i = 0; i < dimension; i++) {
			X[i] = Xmin + new Random().nextDouble() * (Xmax - Xmin);
			pbest[i] = X[i];
		}*/
		
		X[0] = Cmin + new Random().nextDouble() * (Cmax -Cmin);
		X[1] = Gmin + new Random().nextDouble() * (Gmax - Gmin);
		pbest[0] = X[0];
		pbest[1] = X[1];
		/*
		 * initialcgArr(); int cIndex=new Random().nextInt(20); int gIndex=new
		 * Random().nextInt(20); X[0] = c[cIndex]; X[1] = g[gIndex];
		 * 
		 * 
		 * pbest[0] = X[0]; pbest[1] = X[1];
		 */
	}

	/**
	 * ��ʼ���Լ����ٶ�
	 */
	public void initialV() {

		for (int i = 0; i < dimension; i++) {
			V[i] = Vmin + new Random().nextDouble() * (Vmax - Vmin);
		}

		/*
		 * V[0] = 2.0; V[1] = 0.02;
		 */
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
