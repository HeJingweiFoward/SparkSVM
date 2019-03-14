package spark.example;

import java.io.Serializable;

public class CG implements Serializable{
	public double C;
	public double G;
	
/*	public CG(double c, double g) {
		// TODO Auto-generated constructor stub
		this.C=c;
		this.G=g;
	}*/
	public double getC() {
		return C;
	}
	public void setC(double c) {
		C = c;
	}
	public double getG() {
		return G;
	}
	public void setG(double g) {
		G = g;
	}
	
}
