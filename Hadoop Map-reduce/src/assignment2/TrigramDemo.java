package assignment2;

import java.awt.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import assignment2.WordCount.MapClass;
import assignment2.WordCount.Reduce;

public class TrigramDemo/* extends Configured implements Tool */ {

	public static final Pattern specialChars = Pattern.compile("[(){},.;!+\"?<>%'-_]");
	
	public static Map<Integer, Text> maps = new TreeMap<Integer, Text>(new TreeComp());

	
		public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {

		ArrayList<String> arr = new ArrayList<String>();
		ArrayList<String> finalArr = new ArrayList<String>();
		int s = 0;

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// here we split the lines
			ArrayList<String> list = new ArrayList();
			String line = "";
			String line1 = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line1);

			while (tokenizer.hasMoreTokens()) { // in this we assign 1 to each word String
				String noSpecial = specialChars.matcher(tokenizer.nextToken().toString().toLowerCase()).replaceAll("");
				line = line + noSpecial + " ";
				
			}

			String testToken[] = {};
			String token[];
			String keyword1 = "science";
			String keyword2 = "fire";
			String keyword3 = "sea";

			if (line.contains(keyword1)) {
				line = line.replaceAll(keyword1, "\\$");
			} else if (line.contains(keyword2)) {
				line = line.replaceAll(keyword2, "\\$");
			} else if (line.contains(keyword3)) {
				line = line.replaceAll(keyword3, "\\$");
			}

			String parts[] = line.split(" ");

			for (int i = 0; i < parts.length; i++) {
				list.add(parts[i]);
			}

			for (int j = 0; j < list.size(); j++) {
				int index = -1;
				ArrayList<String> list1 = new ArrayList();
				ArrayList<String> receiveArr = new ArrayList<String>();
				if (list.get(j).equals("$")) {
					index = j;
				}

				if (index < 0) {
					continue;
				}
				System.out.println("list ka size!!! "+list.size());
				try {
				if (index == 0) {
					list1.add(list.get(0));
					list1.add(list.get(1));
					list1.add(list.get(2));
				} else if (index == 1) {
					list1.add(list.get(0));
					list1.add(list.get(1));
					list1.add(list.get(2));
					list1.add(list.get(3));

				} else if (index == list.size() - 1) {
					list1.add(list.get(list.size() - 1));
					list1.add(list.get(list.size() - 2));
					list1.add(list.get(list.size() - 3));

				} else if (index == list.size() - 2) {
					list1.add(list.get(list.size() - 1));
					list1.add(list.get(list.size() - 2));
					list1.add(list.get(list.size() - 3));
					list1.add(list.get(list.size() - 3));

				} else {

					list1.add(list.get(index - 2));
					list1.add(list.get(index - 1));
					list1.add(list.get(index));
					list1.add(list.get(index + 1));
					list1.add(list.get(index + 2));
				}
				}catch(IndexOutOfBoundsException e) {
					System.out.println("Error: "+e.getMessage());
					continue;
					
				}

				list.add(index + 1, "#");
				receiveArr = (ArrayList<String>) fun(list1).clone();
				for (int i = 0; i < receiveArr.size(); i++) {
					finalArr.add(receiveArr.get(i));
					
				}
				for (int i = 0; i < finalArr.size(); i++) {
					value.set(finalArr.get(i));
					System.out.println("Final value: " + value.toString());
					context.write(value, new IntWritable(1));
				}
				finalArr.clear();

				// }
			}

		}

	}

	public static ArrayList<String> myFun(String arr[]) {

		ArrayList<String> str = new ArrayList<String>();
		
		/*
		 * for (int i = 0; i < arr.length; i++) { String temp = arr[0]; arr[0] = arr[1];
		 * arr[1] = arr[2]; arr[2] = temp;
		 */
			String value = arr[0] + "_" + arr[1] + "_" + arr[2];
		//	String value1 = arr[0] + "_" + arr[2] + "_" + arr[1];
			str.add(value);
			//str.add(value1);
			System.out.println("value: " + value);
			//System.out.println("value: " + value1);
			// return str;
		//}
		
		return str;
	}

	public static ArrayList<String> fun(ArrayList<String> list) {
		String arr[] = new String[3];
		ArrayList<String> str1 = new ArrayList<String>();

		for (int j = 0; j < list.size(); j++) {
			if (list.size() > 3) {

				for (int i = 0; i < 3; i++) {
					arr[i] = list.get(i);
				}
				if (!list.get(0).equals("$")) {
					list.remove(0);
				}
				str1 = myFun(arr);

			} else {

				for (int i = 0; i < list.size(); i++) {
					arr[i] = list.get(i);
				}
				str1 = myFun(arr);

			}

		}
		return str1;
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
				
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			
			int sumOfValues = 0;
			

			for (IntWritable s : value) {
				sumOfValues += s.get();
			}
			String str = key.toString();
			
			 //  maps.put(new Text(str),sumOfValues);
		       	maps.put(sumOfValues,new Text(str));
			  			   
			   
			   

			// super.reduce(arg0, arg1, arg2);
		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			
			int count = 0;
	
			for(Entry e: maps.entrySet())
			  { if(count == 10) { break; } 
			  System.out.println("Value of count: "+count); 
			  count++;
			  context.write(new Text(e.getValue().toString()), new IntWritable((int) e.getKey()));
			  
			  }
			 
			
	
		}
	
		
		
	}
	
	  public static class TreeComp implements Comparator<Integer>{
	  
	  @Override public int compare(Integer arg0, Integer arg1) { // TODOAuto-generated method stub
		  if(arg0>arg1) { return -1; }
		  else if(arg1>arg0) {
	  
	  return 1; }
		  return 1; }
	  
	  }


	public static void main(String[] args) { // TODO Auto-generated method stub

		try {
			Configuration confg = new Configuration();

			Job job = Job.getInstance(confg, "TrigramDemo");
			job.setJarByClass(TrigramDemo.class);
			job.setMapperClass(MapClass.class);
			job.setReducerClass(Reduce.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			Path path = new Path(args[1]);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, path);

			path.getFileSystem(confg).delete(path, true);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODOAuto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}



}

