package bd_project.query1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CanceledFlightMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable arg0, Text arg1, OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {
		String[] line = arg1.toString().split(",");
		if (!line[0].equals("Year")) {
			String key = line[0] + "-" + line[1] + "-" + line[2];
			Integer value = Integer.parseInt(line[21]);
			arg2.collect(new Text(key), new IntWritable(value));
		}
	}

}
