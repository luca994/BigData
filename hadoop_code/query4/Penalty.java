package bd_project.query4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Penalty extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		
		Configuration conf = getConf();

		JobConf jobConf = new JobConf(conf, Penalty.class);
		Path in = new Path(arg0[0]);
		Path out = new Path(arg0[1]);
		FileInputFormat.setInputPaths(jobConf, in);
		FileOutputFormat.setOutputPath(jobConf, out);

		jobConf.setJobName("Penalty");
		jobConf.setMapperClass(PenaltyMap.class);
		jobConf.setReducerClass(PenaltyReduce.class);
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(DoubleWritable.class);

		JobClient.runJob(jobConf);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Penalty(), args);
		System.exit(res);
	}

}
