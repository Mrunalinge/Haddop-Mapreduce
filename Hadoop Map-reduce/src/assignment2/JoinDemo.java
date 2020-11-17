package assignment2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import assignment2.WordCount.MapClass;
import assignment2.WordCount.Reduce;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;


public class JoinDemo {
	
	//public static final Pattern specialChars = Pattern.compile("\"?<>%'-_]");
	
	public static class Join1Mapper extends Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String allData = value.toString();
			String [] cells = allData.split(",");
			context.write(new Text(cells[0]), new Text("join1 "+cells[1]));
			//super.map(key, value, context);
		}
		
	}
	
	public static class Join2Mapper extends Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String allData = value.toString();
			String [] cells = allData.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)",-1);
			String id = cells[0];
			String salary = cells[1];
			String country = cells[2];
			String passCode = cells[3];
			
			context.write(new Text(id), new Text("join2 "+salary+" "+country+" "+passCode));
			
			//super.map(key, value, context);
		}
		
	}
	
	public static class ReduceJoin extends Reducer <Text,Text,Text,Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String finalVal ="";
			String name="";
			String value="";
		    for(Text t: values) {
		    	String val[] = t.toString().split(" ");
		    		if(val[0].equals("join2")) {
		    			 value =val[1]+"  "+val[2]+"  "+val[3];
		    			
		    		}
		    		else {
		    			 name =val[1]+"  ";
		    	}
		    }
		    finalVal = name+value;
		    context.write(key, new Text(finalVal));
			//super.reduce(arg0, arg1, arg2);
		}
		
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
         Configuration confg = new Configuration();
		
		Job job = Job.getInstance(confg, "JoinDemo");
		job.setJarByClass(JoinDemo.class);
		//job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceJoin.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,Join1Mapper.class );
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,Join2Mapper.class );
		
		
		Path path = new Path(args[2]);
		
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,path);
		
		path.getFileSystem(confg).delete(path, true);
		
		
		
		
			System.exit(job.waitForCompletion(true)? 0 :1);

	}

}
