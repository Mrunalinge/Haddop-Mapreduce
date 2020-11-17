package assignment2;

public class DistanceCalc {
	
public double getEucliDistance(double [] feature1, double []feature2) {
	double dist =0;
	System.out.println("Length of f1 and f2 "+feature1.length+" "+feature2.length);
	for(int i=0; i< feature1.length;i++) {
		dist += Math.pow(feature1[i]-feature2[i], 2);
	}
	return Math.sqrt(dist);
}

}
