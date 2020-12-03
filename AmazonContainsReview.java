import java.io.IOException;
import java.util.regex.*;
import java.util.Set;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import com.google.gson.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * This Map-Reduce code will go through every Amazon review in rfox12:reviews
 * It will then output data on the top-level JSON keys
 */
public class AmazonContainsReview extends Configured implements Tool {
	// Just used for logging
	protected static final Logger LOG = LoggerFactory.getLogger(AmazonContainsReview.class);

	// This is the execution entry point for Java programs
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(HBaseConfiguration.create(), new AmazonContainsReview(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Need 3 argument (hdfs output path), got: " + args.length);
			return -1;
		}

		// Now we create and configure a map-reduce "job"     
		Job job1 = Job.getInstance(getConf(), "average");
		job1.setJarByClass(AmazonContainsReview.class);
    
		// By default we are going to can every row in the table
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs

		// This helper will configure how table data feeds into the "map" method
		TableMapReduceUtil.initTableMapperJob(
			"rfox12:reviews_10000",        	// input HBase table name
			scan,             				// Scan instance to control CF and attribute selection
			AverageMapReduceMapper.class,   		// Mapper class
			Text.class,             		// Mapper output key
			Text.class,				// Mapper output value
			job1,							// This job
			true							// Add dependency jars (keep this to true)
		);

		// Specifies the reducer class to used to execute the "reduce" method after "map"
		job1.setReducerClass(AverageMapReduceReducer.class);

		// For file output (text -> number)
		FileOutputFormat.setOutputPath(job1, new Path(args[0]));  // The first argument must be an output path
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		// What for the job to complete
		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(getConf(), "differnce");
		job2.setJarByClass(AmazonContainsReview.class);
		job2.setMapperClass(DiffMapReduceMapper.class);
		job2.setReducerClass(DiffMapReduceReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		job2.waitForCompletion(true);

		Job job3 = Job.getInstance(getConf(), "bins");
		job3.setJarByClass(AmazonContainsReview.class);
		job3.setMapperClass(TotalMapReduceMapper.class);
		job3.setReducerClass(TotalMapReduceReducer.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job3, new Path(args[1]));
		FileOutputFormat.setOutputPath(job3, new Path(args[2]));

		return job3.waitForCompletion(true) ? 0 : 1;

	}

	public static class AverageMapReduceMapper extends TableMapper<Text, Text> {
		private static final Logger LOG = LoggerFactory.getLogger(AverageMapReduceMapper.class);
    
    		// Here are some static (hard coded) variables
		private static final byte[] CF_NAME = Bytes.toBytes("cf");			// the "column family" name
		private static final byte[] QUALIFIER = Bytes.toBytes("review_data");	// the column name
		private Counter rowsProcessed;  	// This will count number of rows processed
		private JsonParser parser;		// This gson parser will help us parse JSON

		// This setup method is called once before the task is started
		@Override
		protected void setup(Context context) {
			parser = new JsonParser();
			rowsProcessed = context.getCounter("average", "Rows Processed");
    		}
  
  		// This "map" method is called with every row scanned.  
		@Override
		public void map(ImmutableBytesWritable rowKey, Result value, Context context) throws InterruptedException, IOException {
			try {
				// Here we get the json data (stored as a string) from the appropriate column
				String jsonString = new String(value.getValue(CF_NAME, QUALIFIER));
				
				// Now we parse the string into a JsonElement so we can dig into it
				JsonElement jsonTree = parser.parse(jsonString);
				
				JsonObject jsonObject = jsonTree.getAsJsonObject();
				
				String productID = jsonObject.get("asin").getAsString();
				String overall = jsonObject.get("overall").getAsString();
				String reviewText = jsonObject.get("reviewText").getAsString();
				String search1 = "reviews";
				String search2 = "comments";

				if (reviewText.toLowerCase().indexOf(search1.toLowerCase()) != -1 || reviewText.toLowerCase().indexOf(search2.toLowerCase()) != -1) {
					context.write(new Text(productID + "Contains"),new Text(overall));
				}
				context.write(new Text(productID),new Text(overall));

				rowsProcessed.increment(1);

			} catch (Exception e) {
				LOG.error("Error in MAP process: " + e.getMessage(), e);
			}
		}
	}
  
	// Reducer to simply sum up the values with the same key (text)
	// The reducer will run until all values that have the same key are combined
	public static class AverageMapReduceReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			int count = 0;
			for (Text value : values) {
				sum += Double.valueOf(value.toString());
				count++;
			}

			double average = sum/count;
			context.write(key, new Text(String.valueOf(average)));
		}
	}
	public static class DiffMapReduceMapper extends Mapper<Object, Text, Text, Text> {
		private static final Logger LOG = LoggerFactory.getLogger(DiffMapReduceMapper.class);
		private Counter rowsProcessed;  	// This will count number of rows processed

		// This "map" method is called with every row scanned.  
		@Override
		public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
			try {
				// Here we get the json data (stored as a string) from the appropriate column
				String [] kv = value.toString().split("\t");
				String productID = kv[0].replace("Contains","");
				String overall = kv[1];
				
				context.write(new Text(productID),new Text(overall));

				rowsProcessed.increment(1);

			} catch (Exception e) {
				LOG.error("Error in MAP process: " + e.getMessage(), e);
			}
		}
	}
  
	// Reducer to simply sum up the values with the same key (text)
	// The reducer will run until all values that have the same key are combined
	public static class DiffMapReduceReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double diff = 0.0;
			for (Text value : values) {
				diff = Math.abs(diff - Double.valueOf(value.toString()));
			}
			context.write(key, new Text(String.valueOf(diff)));
		}
	}

	public static class TotalMapReduceMapper extends Mapper<Object, Text, Text, Text> {
		private static final Logger LOG = LoggerFactory.getLogger(TotalMapReduceMapper.class);
		private Counter rowsProcessed;  	// This will count number of rows processed

		// This "map" method is called with every row scanned.  
		@Override
		public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
			try {
				// Here we get the json data (stored as a string) from the appropriate column
				String [] kv = value.toString().split("\t");
				String productID = kv[0];
				String diff = kv[1];
				int [] bins = new int[]{2,3,5};
				String [] bin_labels = new String[]{"0<x≤2", "2<x≤3","3<x≤5"};
				for (int i = 0; i <= bins.length; i++) {
					if(Double.valueOf(diff) <= bins[i]) {
						context.write(new Text("Difference:" + bin_labels[i]),new Text("1"));
						break;
					}
				}
				

				rowsProcessed.increment(1);

			} catch (Exception e) {
				LOG.error("Error in MAP process: " + e.getMessage(), e);
			}
		}
	}
  
	// Reducer to simply sum up the values with the same key (text)
	// The reducer will run until all values that have the same key are combined
	public static class TotalMapReduceReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (Text value : values) {
				sum++;
			}
			context.write(key, new Text(String.valueOf(sum)));
		}
	}
}

