package SparkPSO;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import parallelSVM.MSSvmTrainer;
import spark.example.CG;

public class PSO {

	private static double[] gbest;// ȫ������λ��

	private static double gbest_fitness = Double.MAX_VALUE;// ȫ������λ�ö�Ӧ��fitness
	
	private static double previous=Double.MAX_VALUE;

	private static int particle_num = 20;// ������

	private static int N = 10;// ��������

	private static int c1, c2 = 2;

	//0.9-0.1    1.2-0.1    Xmax/N-0.1
	private static double Wmax = 1.2;// ����Ȩ��   ����Ȩ�ز���LDW���Եݼ�����
	private static double Wmin=0.1;

	private static List<Particle> particles = new ArrayList<Particle>();// ����Ⱥ

	/**
	 * ��ʼ����������
	 * 
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	public static void initialParticles() throws IOException, URISyntaxException {
		for (int i = 0; i < particle_num; i++) {
			Particle particle = new Particle();
			particle.initialX();
			System.out.println("c:"+particle.X[0]+",g:"+particle.X[1]);
			particle.initialV();
			System.out.println("cv:"+particle.V[0]+",gv:"+particle.V[1]);
			particle.fitness = particle.calculateFitness();
			particles.add(particle);
		}
	}

	/**
	 * update gbest
	 */
	public static void updateGbest() {
		double fitness = Double.MAX_VALUE;
		int index = 0;
		for (int i = 0; i < particle_num; i++) {
			if (particles.get(i).fitness < fitness) {
				index = i;
				fitness = particles.get(i).fitness;
			}
		}
		if (fitness < gbest_fitness) {
			gbest = particles.get(index).pbest.clone();
			gbest_fitness = fitness;
		}
	}

	/**
	 * ����ÿ�����ӵ��ٶ�
	 */
	public static void updateV(int k) {
		//���Եݼ�����
		double w=Wmax-(Wmax-Wmin)*(k*1.0/N);
		//�����Եݼ�����
		//double w=Wmin+(Wmax-Wmin)*Math.exp(-20*Math.pow(k*1.0/N, 6));
		System.out.println("k:"+k+",k/N:"+(k*1.0/N)+",w:"+w);
		//System.out.println("k:"+k+",Math.exp(-20*Math.pow(k*1.0/N, 6)):"+Math.exp(-20*Math.pow(k*1.0/N, 6))+",w:"+w);
		for (Particle particle : particles) {
			for (int i = 0; i < particle.dimension; i++) {
			
				double v = w * particle.V[i] + c1 * rand() * (particle.pbest[i] - particle.X[i])
						+ c2 * rand() * (gbest[i] - particle.X[i]);
				if (v > particle.Vmax)
					v = particle.Vmax;
				else if (v < -particle.Vmax)
					v = -particle.Vmax;
				particle.V[i] = v;// ����Vi
			}
		}
	}

	/**
	 * ����ÿ�����ӵ�λ�ú�pbest �Ѹ�������λ�ú�pBest�ֿ�
	 * 
	 * @throws IOException
	 */
	/*
	 * public static void updateX() throws IOException { for(Particle
	 * particle:particles) { for(int i=0;i<particle.dimension;i++) {
	 * particle.X[i] = particle.X[i] + particle.V[i]; } double newFitness =
	 * particle.calculateFitness();//�µ���Ӧֵ //����µ���Ӧֵ��ԭ����С�����fitness��pbest
	 * ��Ӧ��ֵ��pbestͬ�� if(newFitness<particle.fitness) { particle.pbest =
	 * particle.X.clone(); particle.fitness = newFitness; } } }
	 */
	// ��������λ��
	public static void updateX() throws IOException {
		for (Particle particle : particles) {
			for (int i = 0; i < particle.dimension; i++) {
				particle.X[i] = particle.X[i] + particle.V[i];
			}
		}
	}

	// ��������pBest
	public static void updatepBest() throws IOException, URISyntaxException {
		for (Particle particle : particles) {
			double newFitness = particle.calculateFitness();// �µ���Ӧֵ
			// ����µ���Ӧֵ��ԭ����С�����fitness��pbest ��Ӧ��ֵ��pbestͬ��
			if (newFitness < particle.fitness) {
				particle.pbest = particle.X.clone();
				particle.fitness = newFitness;
			}
		}

	}
	
	//

	public static void updatepBestByMR() throws IOException {
		

	}
	
	/**
	 * �㷨��Ҫ����
	 * 
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	public static void process() throws IOException, URISyntaxException {
		
		SparkConf sparkConf = new SparkConf()
				.setJars(new String[] { "E:\\����ʵ��\\mjsl1.jar" })
				.set("spark.num.executors", "4")
				.set("spark.executor.cores", "3")
				.set("spark.default.parallelism", "12")
				.set("spark.executor.memory", "2048m")
				.set("spark.network.timeout", "300")
				.setMaster("spark://192.168.2.151:7077");
			/*	.setMaster("local[4]");*/
		SparkSession spark = SparkSession.builder().appName("MyJavaSparkPSO").config(sparkConf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		Encoder<Particle> particleEncoder = Encoders.bean(Particle.class);
	
		int n = 0;
		System.out.println("����Ⱥ��ʼ����");
		//initialParticles();
		for (int i = 0; i < particle_num; i++) {
			Particle particle = new Particle();
			particle.initialX();
			System.out.println("c:"+particle.X[0]+",g:"+particle.X[1]);
			particle.initialV();
			particles.add(particle);
		}
		Dataset<Particle> initialParticleDF = spark.createDataset(particles,particleEncoder);
		Dataset<Particle> initialParticleMap=initialParticleDF.map(new MapFunction<Particle,Particle>() {
			@Override
			public Particle call(Particle particle) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("��ʼ������֤");
				particle.fitness = particle.calculateFitness();
				return particle;
			}
		}, particleEncoder);
		particles= initialParticleMap.collectAsList();
				
		updateGbest();
		System.out.println("����Ⱥ��ʼ����ϣ���ʼ������");
		
		while (n++ < N) {
			updateV(n);
			updateX();
			//updatepBest();
			Dataset<Particle> updatepBestParticleDF = spark.createDataset(particles,particleEncoder);
			Dataset<Particle> updatepBestParticleMap=updatepBestParticleDF.map(new MapFunction<Particle,Particle>() {
				@Override
				public Particle call(Particle particle) throws Exception {
					// TODO Auto-generated method stub
					System.out.println("��ʼ������֤");
					double newFitness = particle.calculateFitness();// �µ���Ӧֵ
					// ����µ���Ӧֵ��ԭ����С�����fitness��pbest ��Ӧ��ֵ��pbestͬ��
					if (newFitness < particle.fitness) {
						particle.pbest = particle.X.clone();
						particle.fitness = newFitness;
					}
					System.out.println("������֤����");
					return particle;
				}
			}, particleEncoder);
			particles= updatepBestParticleMap.collectAsList();
			
			updateGbest();
			System.out.println(
					"**************��" + n + ".��ǰgbest:(" + gbest[0] + "," + gbest[1] + ")  fitness=" + gbest_fitness);
		}
	}

	/**
	 * ����һ��0~1�������
	 * 
	 * @return
	 */
	public static double rand() {
		return new Random().nextDouble();
	}

	public static void main(String[] args) throws IOException, URISyntaxException {
		process();
	}
}
