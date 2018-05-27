package query2_v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WeatherDelays2  extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		
		JobConf job = new JobConf(conf, WeatherDelays2.class);
		Path in = new Path(arg0[0]);
		Path out = new Path(arg0[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJobName("WeatherDelays2");
		job.setMapperClass(WeatherDelays2Map.class);
		job.setReducerClass(WeatherDelays2Reduce.class);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WeatherDelayType.class);
        JobClient.runJob(job);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WeatherDelays2(), args);
        
        System.exit(res);
    }
}
