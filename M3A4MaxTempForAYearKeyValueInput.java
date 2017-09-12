package hadoop.edureka.assignments.m3;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Nalla Anand AyeeGounder
 */
public class M3A4MaxTempForAYearKeyValueInput {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		if (args.length !=2) {
				System.out.println("Incorrect No of Arguments. Expect 2 arguments: <inputPath> <OutputPath>");
				System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
		
		
		Job job = Job.getInstance(conf, "MaxTempForAYearKeyValueInput");
		
		job.setJarByClass(M3A4MaxTempForAYearKeyValueInput.class);
		
		//job.setMapperClass(MaxTempForAYearKeyValueInputMapper.class);
		job.setReducerClass(MaxTempForAYearKeyValueInputReducer.class);
		job.setCombinerClass(MaxTempForAYearKeyValueInputReducer.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path (args[1]+ "_" + new SimpleDateFormat("yyyy-MM-dd_hhmm").format(Calendar.getInstance().getTime())));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		System.exit(job.waitForCompletion(true)? 0: 1);
		
	}

}

class MaxTempForAYearKeyValueInputMapper extends Mapper<Text, Text, Text, Text> {
	
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		context.write(key, value);		
	}
}

class MaxTempForAYearKeyValueInputReducer extends Reducer<Text, Text, Text, Text> {
	
	private int maxTemp;
	@Override
	public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		maxTemp = -100000;
		for (Text value : values){
			if (maxTemp < Integer.parseInt(value.toString()))
				maxTemp = Integer.parseInt(value.toString());
		}
		context.write (key, new Text(""+maxTemp));
	}
}


