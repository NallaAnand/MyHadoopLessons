package hadoop.edureka.assignments.m3;

import hadoop.edureka.assignments.m3.mapred.HotAndColdDaysMapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Nalla Anand AyeeGounder
 */
public class M3A5HotAndColdDays {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.out.println("Incorrect No of Arguments. Expect 2 arguments: <inputPath> <OutputPath>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"HotAndColdDays");
		
		job.setJarByClass(HotAndColdDaysMapper.class);
		job.setMapperClass(HotAndColdDaysMapper.class);
		//No Reducer Logic is required for this as a single day has only one entry. 
		//If the input splits are more than 1 then we may need the default reducer to run which would just aggregate the results
		//job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "_" + new SimpleDateFormat("yyyy-MM-dd_hhmm").format(Calendar.getInstance().getTime())));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}

}
