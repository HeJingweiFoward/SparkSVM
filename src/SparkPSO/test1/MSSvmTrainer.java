package SparkPSO.test1;

import java.util.StringTokenizer;
import java.util.Vector;

import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;
import libsvm.svm_problem;

public class MSSvmTrainer {
	    private svm_problem prob;
	    private svm_parameter param;
	    private int max_index = 0;
	    private String[] subsetRecords;
	    //private int nr_fold = 10;
	    //改成做4折交叉验证
	    private int nr_fold = 2;
	    
	    public MSSvmTrainer(String[] subsetRecords,double c,double g){
	        this.subsetRecords = subsetRecords;
	        formSvmProblem();
	        configureSvmParameters(c,g);
	    }
	    
	    private void formSvmProblem() {
	      Vector<Double> vy = new Vector<Double>();
	      Vector<svm_node[]> vx = new Vector<svm_node[]>();
	      for(int itr=0; itr<this.subsetRecords.length; itr++) {
	        StringTokenizer recordTokenItr = new StringTokenizer(this.subsetRecords[itr]," \\t\\n\\r\\f:");
	        vy.addElement(Double.valueOf(recordTokenItr.nextToken()).doubleValue());
	        int featureCount = recordTokenItr.countTokens()/2;
	        libsvm.svm_node[] features = new svm_node[featureCount];
	        // filling in the features of current record
	        for(int i=0; i<featureCount; i++) {
	            features[i] = new svm_node();
	            features[i].index = Integer.parseInt(recordTokenItr.nextToken());
	            features[i].value = Double.valueOf(recordTokenItr.nextToken()).doubleValue();
	        }
	        // compare the largest feature index with max_index
	        if(featureCount>0)
	            this.max_index = Math.max(this.max_index, features[featureCount-1].index);
	        vx.addElement(features);
	      }

	      this.prob = new svm_problem();
	      this.prob.l = vy.size();

	      this.prob.x = new svm_node[this.prob.l][];
	      this.prob.y = new double[this.prob.l];
	      for(int i=0; i<prob.l; i++) {
	          this.prob.x[i] = vx.elementAt(i);
	          this.prob.y[i] = vy.elementAt(i);
	      }
	    }

	    private void configureSvmParameters(double c ,double g) {
	        this.param = new svm_parameter();
	        // default values
	        this.param.svm_type = svm_parameter.C_SVC;
	        this.param.kernel_type = svm_parameter.RBF;
	        this.param.degree = 3;
	        this.param.gamma = g;        // 1/num_features
	        this.param.coef0 = 0;
	        this.param.nu = 0.5;
	        this.param.cache_size = 100;
	        this.param.C = c;
	        this.param.eps = 1e-3;
	        this.param.p = 0.1;
	        this.param.shrinking = 1;
	        this.param.probability = 0;
	        this.param.nr_weight = 0;
	        this.param.weight_label = new int[0];
	        this.param.weight = new double[0];
	        if(this.param.gamma == 0 && this.max_index > 0)
	          this.param.gamma = 1.0/this.max_index;
	        if(this.param.kernel_type == svm_parameter.PRECOMPUTED){
	          for(int i=0;i<this.prob.l;i++) {
	            if (this.prob.x[i][0].index != 0) {
	              System.err.print("Wrong kernel matrix: first column must be 0:sample_serial_number\n");
	              System.exit(1);
	            }
	            if ((int)this.prob.x[i][0].value <= 0 || (int)this.prob.x[i][0].value > this.max_index) {
	              System.err.print("Wrong input format: sample_serial_number out of range\n");
	              System.exit(1);
	            }
	          }
	        }
	      }
	   
	    public String do_cross_validation()
		{
			int i;
			int total_correct = 0;
			double total_error = 0;
			double sumv = 0, sumy = 0, sumvv = 0, sumyy = 0, sumvy = 0;
			double[] target = new double[prob.l];

			svm.svm_cross_validation(prob,param,nr_fold,target);
			if(param.svm_type == svm_parameter.EPSILON_SVR ||
			   param.svm_type == svm_parameter.NU_SVR)
			{
				for(i=0;i<prob.l;i++)
				{
					double y = prob.y[i];
					double v = target[i];
					total_error += (v-y)*(v-y);
					sumv += v;
					sumy += y;
					sumvv += v*v;
					sumyy += y*y;
					sumvy += v*y;
				}
				System.out.print("Cross Validation Mean squared error = "+total_error/prob.l+"\n");
				System.out.print("Cross Validation Squared correlation coefficient = "+
					((prob.l*sumvy-sumv*sumy)*(prob.l*sumvy-sumv*sumy))/
					((prob.l*sumvv-sumv*sumv)*(prob.l*sumyy-sumy*sumy))+"\n"
					);
			}
			else
			{
				for(i=0;i<prob.l;i++)
					if(target[i] == prob.y[i])
						++total_correct;
				System.out.print("Cross Validation Accuracy = "+100.0*total_correct/prob.l+"%\\n");
			}
			
			/*return 100.0*total_correct/prob.l+"%";*/
			return 100.0*total_correct/prob.l+"";
		}	
	    
	    public svm_model train(){
	    	svm_model model = svm.svm_train(this.prob, this.param);	 
	    	return model;
	    }	    
}
