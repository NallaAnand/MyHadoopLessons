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
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author Nalla Anand AyeeGounder
 */
public class M3A3SubPatentCount {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if (args.length != 2){
			System.out.println("Incorrect no of Aruguments !!! Program expects 2 argument <InputPath> <OutputPath>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"M3A3SubPatentCount");
		
		job.setJarByClass(SubPatentCountMapper.class);
		job.setMapperClass(SubPatentCountMapper.class);
		job.setReducerClass(subPatentCountReducer.class);
		
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "_" + new SimpleDateFormat("yyyy-MM-dd_hhss").format(Calendar.getInstance().getTime())));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.waitForCompletion(true);
				
		JobStatus jobStatus = job.getStatus();
		System.out.println("Job Status is " + jobStatus.toString());
		
		
	}

}

class SubPatentCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private Text patentNo;
	private final IntWritable count = new IntWritable(1);
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		patentNo = new Text(value.toString().split("\\W+")[0]);
		context.write(patentNo, count);
	}
}

class subPatentCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	private int count;
	@Override
	public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		count = 0;
		
		for (IntWritable value: values){
			count += value.get();			
		}
		
		context.write(key, new IntWritable(count));
		
	}
}