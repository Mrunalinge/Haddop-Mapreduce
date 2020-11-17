package assignment2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import assignment2.WordCount.MapClass;
import assignment2.WordCount.Reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Pattern;


public class InvertedIndexDemo {
	
	public static final Pattern specialChars = Pattern.compile("[(){},.;!+\"?<>%'-_]");	
	public static class Map extends Mapper<LongWritable, Text,Text,Text>{

		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			//here we are getting name of the file
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			String line = value.toString();
			//line splitting code like simple mapreduce wordcount
            StringTokenizer tokenizer = new StringTokenizer(line);
			
			while(tokenizer.hasMoreTokens()) {
                //here we remove special characters from the word
				String noSpecial = specialChars.matcher(tokenizer.nextToken().toString()).replaceAll(""); 
				//here for each word, we write word as key and filename as value
				context.write(new Text(noSpecial), new Text(fileName));
			}
			
			//super.map(key, value, context);
		}
		
	}
	
	public static class Reduce extends Reducer<Text,Text,Text,Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			//created a hashmap to store filename as key to compute and store
			//number of times the filename occurs for a value
			HashMap hashMap = new HashMap();
			int count =0;
			
			for(Text val: values) {
				String word = val.toString();
				//check if the filename is already present in the hasmap. If not add the filename to the hashmap and increment counter by 1
				if(hashMap!= null && hashMap.get(word)!=null) {
					count = (int)hashMap.get(word);
					hashMap.put(word, ++count);
				}
				else {
					//if the value is already present int he hashmap, just increment the count for that word by one
					hashMap.put(word, 1);
				}
			}
			//finally write [file1 word1 count, file2 word1 count etc
			context.write(key, new Text(hashMap.toString()));
			
			//super.reduce(arg0, arg1, arg2);
		}
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			Configuration confg = new Configuration();
			
			Job job = Job.getInstance(confg, "InvertedIndexDemo");
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setJarByClass(InvertedIndexDemo.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			Path path = new Path(args[1]);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, path);
			
			path.getFileSystem(confg).delete(path, true);
			
			
			
			System.exit(job.waitForCompletion(true)? 0 :1);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}

}
