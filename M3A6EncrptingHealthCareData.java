package hadoop.edureka.assignments.m3;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Nalla Anand AyeeGounder
 */
public class M3A6EncrptingHealthCareData {


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if (args.length !=2){
			System.out.println("Invalid Input!! Expected 2 args <input Path> <output Path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "EncrptingHealthCareData");
		
		job.setJarByClass(EncrptingHealthCareDataMapper.class);
		job.setMapperClass(EncrptingHealthCareDataMapper.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "_" + new SimpleDateFormat("yyyy-MM-dd_hhmm").format(Calendar.getInstance().getTime()) ));
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
	
	
		public static class EncrptingHealthCareDataMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
		ArrayList<String> words;

		final Integer[] colIndexToEncrypt = {1,3,4,5};
		final byte[] encryptKey = "NALLAANANDKEYS12".getBytes();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			words = new ArrayList<String>( Arrays.asList(value.toString().split(",")));			
			for (int colIndex : colIndexToEncrypt){
				words.set(colIndex, encrypt(words.get(colIndex), encryptKey));
			}
			
			StringBuilder encryptedRow = new StringBuilder();
			for (String word: words){
				encryptedRow = encryptedRow.append(word + ",");
			}
			
			
			context.write(NullWritable.get(), new Text(encryptedRow.deleteCharAt(encryptedRow.length()-1).toString()));
		}
		
		public String encrypt(String strToEncrypt, byte[] key)
		{
			try
			{
				Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
				SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
				cipher.init(Cipher.ENCRYPT_MODE, secretKey);
				//cipher.init(Cipher.DECRYPT_MODE, secretKey);
				String encryptedString = Base64.encodeBase64String(cipher.doFinal(strToEncrypt.getBytes()));
				//String decrypted = new String(cipher.doFinal(Base64.decodeBase64(strToEncrypt)));
			
				return encryptedString.trim();
				//return decrypted;
			}
			catch (Exception e)
			{
				System.out.println("Error while encrypting" + e);
			}
			return null;

		}
	
	}
	


}
