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

//Using Text Input Format

/**
 * @author Nalla Anand AyeeGounder
 */
public class M3A4MaxTempForAYear {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		if (args.length !=2) {
				System.out.println("Incorrect No of Arguments. Expect 2 arguments: <inputPath> <OutputPath>");
				System.exit(-1);
		}
		
		Configuration conf = new Configuration();		
		Job job = Job.getInstance(conf, "MaxTempForAYear");
		
		job.setJarByClass(M3A4MaxTempForAYear.class);
		
		job.setMapperClass(MaxTempForAYearMapper.class);
		job.setReducerClass(MaxTempForAYearReducer.class);
		job.setCombinerClass(MaxTempForAYearReducer.class);
		
	
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path (args[1]+ "_" + new SimpleDateFormat("yyyy-MM-dd_hhmm").format(Calendar.getInstance().getTime())));

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		System.exit(job.waitForCompletion(true)? 0: 1);
		
	}

}

class MaxTempForAYearMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
	
	private String[] words;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		words = value.toString().split(" ");
		context.write(new IntWritable(Integer.parseInt(words[0])), new IntWritable(Integer.parseInt(words[1])));		
	}
}


class MaxTempForAYearReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
	
	private int maxTemp;
	@Override
	public void reduce (IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		maxTemp = -100000;
		for (IntWritable value : values){
			if (maxTemp < value.get())
				maxTemp = value.get();
		}
		context.write (key, new IntWritable(maxTemp));
	}
}
