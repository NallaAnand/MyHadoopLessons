package hadoop.edureka.assignments.m3;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Nalla Anand AyeeGounder
 */
public class M3A1WordCountWithPartitioner {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		System.out.println("Entering M3A1WordCount.main method.....");
		
		if (args.length !=2){
			System.out.println("Invalid Input!! Expected 2 args <input Path> <output Path>");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job wordCountJob = Job.getInstance(conf, "WordCountWithPartitioner");
		wordCountJob.setJarByClass(M3A1WordCountWithPartitioner.class);
		
		wordCountJob.setMapperClass(WordCountMapperWithPartitioner.class);
		wordCountJob.setReducerClass(WordCountReducerWithPartitioner.class);
		wordCountJob.setCombinerClass(WordCountReducerWithPartitioner.class);
		wordCountJob.setPartitionerClass(WordCountPartitionerWithPartitioner.class);
		wordCountJob.setNumReduceTasks(3);
		
		FileInputFormat.addInputPaths(wordCountJob, args[0]);
		FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1] + "_" + new SimpleDateFormat("yyyy-MM-dd_hhmm").format(Calendar.getInstance().getTime()) ));
		
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(IntWritable.class);		

		boolean isJobSuccessful = wordCountJob.waitForCompletion(true);

		System.out.println("Exiting M3A1WordCount.main method.....\n" + (isJobSuccessful ? "Job is Successful" : "Job Failed"));
		
		System.exit(isJobSuccessful ? 0 : 1);
		
	}

}

class WordCountMapperWithPartitioner extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private String[] words;
	final IntWritable count = new IntWritable(1);
	
	@Override
	public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		words = value.toString().split("\\W+");
		for (String word : words)
			context.write(new Text(word), count);
	}
	
}

class WordCountReducerWithPartitioner extends Reducer<Text, IntWritable, Text, IntWritable>{
	private int wordCount;

	@Override
	public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		wordCount = 0;
		for (IntWritable value : values){
			wordCount += value.get();
		}		
		
		context.write(key, new IntWritable(wordCount));
	}
}

class WordCountPartitionerWithPartitioner extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		if (key.toString().matches("[A-Ma-m].*")) {
			//System.out.println("----: " + key + " :" + "Zero Value");
			return 0;
		}
		
		else if (key.toString().matches("[N-Zn-z].*"))  {
			//System.out.println("-------------: " + key + " :" + "One Value");
			return 1;
		}
		else 		
			return 2;
	}
	
}

