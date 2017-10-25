import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataDividerByUser {
	
	public static class DataDividerMapper
			extends Mapper<LongWritable, Text, IntWritable, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {

			String[] user_movie_rating = value.toString().trim().split(",");
			int userID = Integer.parseInt(user_movie_rating[0]);
			String movieID = user_movie_rating[1];
			String rating = user_movie_rating[2];

			context.write(new IntWritable(userID), new Text(movieID +":" + rating));
		}
	}
	/**
	 * Note:
	 * (1) mapper input: userID, movieID, rating
	 * (2) mapper expected output:
	 * 		<key, value> = <userID, <movieID:rating>>
	 */

	public static class DataDividerReducer
			extends Reducer<IntWritable, Text, IntWritable, Text> {
		
		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			while (values.iterator().hasNext()) {
				sb.append("," + values.iterator().next());
			}

			//sb = ,movie1:2.3,movie2:4.9....
			context.write(key, new Text(sb.toString().replaceFirst(",", "")));

		}
	}
	/**
	 *  Note:
	 * (1) reducer input:
	 * 		<key, value> = <userID, movieID:rating> 
	 * (2) reducer expected output:
	 * 		<key, value> = <userID, <movieID:rating, movieID:rating, ...>>
	 * (3) The reducer stores all <movie:rating> pairs of the same user
	 * 		in an "Iterable" format.
	 */
	

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		
		// tells Job mapper class and reducer class
		job.setMapperClass(DataDividerMapper.class);
		job.setReducerClass(DataDividerReducer.class);
		
		// tells Job where the main method locates
		job.setJarByClass(DataDividerByUser.class);

		// tells Job input and output formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// sets paths for input and output: command line
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		// this is just the 1st job
		// wait for its completion and then start the 2nd job
		job.waitForCompletion(true);
	}

}
