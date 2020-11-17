package assignment2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class KNNDemo {
	
	public static List<double[]> trainData = new ArrayList<double[]>();
	public static List<String> trainLabel = new ArrayList<String>();
	static ClassLoader loader = KNNDemo.class.getClassLoader();
	static URL u = loader.getResource("Train.csv");
	static String file = u.getFile();
	
	//public static List<double[]> testData = new ArrayList<double[]>();
	//public static List<String> testLabel = new ArrayList<String>();

	static int knnValue= 5;
	int distMetric=0;
	int totalLables=0;
	public static int count=0;
	
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException, NumberFormatException {
			// TODO Auto-generated method stub
			 List<double[]> testData = new ArrayList<double[]>();
	         List<String> testLabel = new ArrayList<String>();
			String val= value.toString();
			loadTrainData();
			//loadTestData(val);

			if(val.matches(".*\\w.*")) {
			String parts []= val.split(",");
			double[] features = new double[parts.length];
			for(int i=0;i<parts.length;i++)
			{
			    System.out.println("Parts[i] and i "+i+" "+parts[i]);
				features[i]= Double.parseDouble(parts[i]);
				
				
				
			}
			testData.add(features);
			testLabel.add(Integer.toString(count++));
			ArrayList<KnnPojo> knp = eucliDistance(testData, testLabel);
			System.out.println("Size of final word arr "+knp.size());
			for(int i=0; i<knp.size();i++) {
				String testRow=  knp.get(i).getTestLbl();
				String label = knp.get(i).getLabel();
				context.write(new Text(testRow), new Text(label));
			}
		}
			else {
				System.out.println("Empty string!! ");
			}
			
			
			
			//super.map(key, value, context);
		}
		public static void loadTrainData() {
			BufferedReader br;
			try {
				String filename = System.getProperty("user.dir")+"/Train.csv";
				
				br = new BufferedReader(new FileReader(file));///home/cse587/Downloads/Train.csv
				String line= "";
				while((line = br.readLine())!=null) {
					String parts []= line.split(",");
					double[] features = new double[parts.length-1];
					
					for(int i=0;i<parts.length-2;i++)
					{
						features[i]= Double.parseDouble(parts[i]);
					}
					trainData.add(features);
					trainLabel.add(parts[features.length-1]);
				}
				br.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
		
		/*
		 * public static ArrayList<KnnPojo> loadTestData(String value) {
		 * 
		 * 
		 * String parts []= value.split(","); double[] features = new
		 * double[parts.length-1]; for(int i=0;i<parts.length;i++) { features[i]=
		 * Double.parseDouble(parts[i]);
		 * 
		 * 
		 * } testData.add(features); testLabel.add(Integer.toString(count++));
		 * ArrayList<KnnPojo> arr = new ArrayList<KnnPojo>(); arr = eucliDistance();
		 * return arr;
		 * 
		 * 
		 * 
		 * }
		 */
		
		public static ArrayList<KnnPojo> eucliDistance(List<double[]> testData,List<String> testLabel) {
			DistanceCalc distanceKnn = new DistanceCalc();
			Iterator <double[]> testIter = testData.iterator();
			ArrayList<KnnPojo> arr = new ArrayList<KnnPojo>();
			System.out.println("In finding distance");
			
			int num =0;
			int labl=0;
			while(testIter.hasNext()) {
				double[] testFeatures = testIter.next();
				Iterator<double[]> trainIter = trainData.iterator();
				while(trainIter.hasNext()) {
					double [] trainFeature = trainIter.next();
				double dist=0;
				dist = distanceKnn.getEucliDistance(trainFeature, testFeatures);
				System.out.println("Ecucli dist is: "+dist);
				String lbl = trainLabel.get(num);
				String tlb = testLabel.get(labl);
				System.out.println("Train and test labels are: "+lbl+" "+tlb);
				KnnPojo knnp = new KnnPojo(dist,lbl,tlb);
				arr.add(knnp);
				Collections.sort(arr);
				num++;
				}
				labl++;
				
			}
			ArrayList<KnnPojo> arr1 = new ArrayList<KnnPojo>();
			for(int i=0;i<knnValue;i++) {
				arr1.add(arr.get(i));
				
			}
			
			return arr1;
		}
		
		
		
		
	}
	
	
	public static class ReduceClass extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			
			HashMap<String , Integer> hashMap =new HashMap();
			ArrayList <Integer> lst = new ArrayList<Integer>();
			int cnt=0;
			for(Text val: values) {
				String word = val.toString();
				//check if the classifer is already present in the hasmap. If not add the filename to the hashmap and increment counter by 1
				if(hashMap!= null && hashMap.get(word)!=null) {
					cnt = (int)hashMap.get(word);
					hashMap.put(word, ++cnt);
				}
				else {
					//if the value is already present int he hashmap, just increment the count for that word by one
					hashMap.put(word, 1);
				}
			}
			
			
			for(Map.Entry e :hashMap.entrySet()) {
				lst.add((Integer) e.getValue());
				Collections.sort(lst);
				System.out.println("List k count ka lnght "+lst.size());
				
			}
			for(Map.Entry e :hashMap.entrySet()) {
			   if(e.getValue()== lst.get(lst.size()-1)) {
				   String finalLabel = e.getKey().toString();
				   System.out.println("Final Label: "+finalLabel);
				   String row = key.toString();
				   context.write(new Text(row), new Text(finalLabel)); 
				   break;
			   }
			}
			
		//	
			
			//super.reduce(arg0, arg1, arg2);
		}
		
		
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration confg = new Configuration();
		
		Job job = Job.getInstance(confg, "KNN");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(KNNDemo.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path path = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, path);
		
		path.getFileSystem(confg).delete(path, true);
		
		
		
		System.exit(job.waitForCompletion(true)? 0 :1);

	}

}
