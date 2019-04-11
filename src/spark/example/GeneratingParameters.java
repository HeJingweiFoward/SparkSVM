package spark.example;

import java.util.ArrayList;
import java.util.List;

public class GeneratingParameters {

	
	public static List<CG> CG(double stepSize,double initialValue,double maxValue)
	{
		List<CG> cgList = new ArrayList<CG>();
	
	
		int i=0;
		while(initialValue+stepSize*i<=maxValue) 
		{
			int j=0;
			while(initialValue+stepSize*j<=maxValue)
			{
				CG cg=new CG();
				cg.setC(initialValue+stepSize*i);
				cg.setG(initialValue+stepSize*j);
				System.out.println(cg.C+","+cg.G);
				cgList.add(cg);
				j++;
			}
			i++;

	
		}
		return cgList;
	}
}
