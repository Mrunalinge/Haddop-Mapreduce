package assignment2;

public class KnnPojo implements Comparable<Object> {
	
	double feature;
	String label;
	String testLbl;
	double distance;
	
	
	public KnnPojo() {
		
	}
	
	public KnnPojo(double distance, String label) {
		this.distance= distance;
		this.label =label;
	}
	public KnnPojo(double distance, String label,String testLbl) {
		this.distance= distance;
		this.label =label;
		this.testLbl = testLbl;
	}
	 

	public String getTestLbl() {
		return testLbl;
	}

	public void setTestLbl(String testLbl) {
		this.testLbl = testLbl;
	}

	public double getFeature() {
		return feature;
	}

	public void setFeature(double feature) {
		this.feature = feature;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}
	
	public String toString() {
		String str=  "distance: "+this.distance+" label: "+this.label;
		return str;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		
		double distance1 = this.distance;
		KnnPojo knp = (KnnPojo)o;
		double distance2 = knp.distance;
		if(distance1<distance2) {
			return 1;
		}
		else
		return -1;
	}

	

}
