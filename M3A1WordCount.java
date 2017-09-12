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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Nalla Anand AyeeGounder
 */
public class M3A1WordCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		System.out.println("Entering M3A1WordCount.main method.....");
		
		if (args.length !=2){
			System.out.println("Invalid Input!! Expected 2 args <input Path> <output Path>");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job wordCountJob = Job.getInstance(conf, "WordCount");
		wordCountJob.setJarByClass(M3A1WordCount.class);
		
		wordCountJob.setMapperClass(WordCountMapper.class);
		wordCountJob.setReducerClass(WordCountReducer.class);
		wordCountJob.setCombinerClass(WordCountReducer.class);
		
		FileInputFormat.addInputPaths(wordCountJob, args[0]);
		FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1] + "_" + new SimpleDateFormat("yyyy-MM-dd_hhmm").format(Calendar.getInstance().getTime()) ));
		
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(IntWritable.class);		

		boolean isJobSuccessful = wordCountJob.waitForCompletion(true);

		System.out.println("Exiting M3A1WordCount.main method.....\n" + (isJobSuccessful ? "Job is Successful" : "Job Failed"));
		
		System.exit(isJobSuccessful ? 0 : 1);
		
	}

}

class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	WordCountMapper(){
		super();
		System.out.println("&&&&&&&&&&&&&& WordCountMapper constructor");
	}
	static {
		System.out.println("+++++++++++++ WordCountMapper");
	}
	
	private String[] words;
	final IntWritable count = new IntWritable(1);
	
	@Override
	public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		words = value.toString().split("\\W+");
		for (String word : words)
			context.write(new Text(word), count);
	}
	
}

class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
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

