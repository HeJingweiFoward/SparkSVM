package spark.example;

import java.util.ArrayList;
import java.util.List;

public class GeneratingParameters {
/**
 * 
 * @param cStepSize
 * @param gStepSize
 * @param cInitialValue
 * @param gInitialValue
 * @param cMaxValue
 * @param gMaxValue
 * @return
 */
	
	public static List<CG> CG(double cStepSize,double gStepSize,double cInitialValue,double gInitialValue,
			double cMaxValue,double gMaxValue)
	{
		List<CG> cgList = new ArrayList<CG>();
	
	
		int i=0;
		while(cInitialValue+cStepSize*i<=cMaxValue) 
		{
			int j=0;
			while(gInitialValue+gStepSize*j<=gMaxValue)
			{
				CG cg=new CG();
				cg.setC(cInitialValue+cStepSize*i);
				cg.setG(gInitialValue+gStepSize*j);
				System.out.println(cg.C+","+cg.G);
				cgList.add(cg);
				j++;
			}
			i++;

	
		}
		return cgList;
	}
}
