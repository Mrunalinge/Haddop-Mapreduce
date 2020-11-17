package assignment2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;


public class WordCount {
	
	public static final Pattern specialChars = Pattern.compile("[(){},.;!+\"?<>%'-_]");

	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//here we split the lines
			String line = value.toString();
		
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			while(tokenizer.hasMoreTokens()) {
				//in this we assign 1 to each word
				String noSpecial = specialChars.matcher(tokenizer.nextToken().toString().toLowerCase()).replaceAll(""); 
				value.set(noSpecial);
				context.write(value, new IntWritable(1));
			}
			
			//super.map(key, value, context);
		}
		
		
		
	}
	
	public static class Reduce extends Reducer<Text, IntWritable,Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			int sumOfValues = 0;
			
			for(IntWritable s: value) {
				sumOfValues+= s.get();
			}
			context.write(key, new IntWritable(sumOfValues));
			
			
			
			//super.reduce(arg0, arg1, arg2);
		}
		
		
		
	}
	public static void main(String[] args)  {
		// TODO Auto-generated method stub
		
		try {
		Configuration confg = new Configuration();
		
		Job job = Job.getInstance(confg, "WordCount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path path = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
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
