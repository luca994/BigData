package bd_project.query3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FlightGroupDelayMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable arg0, Text arg1, OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {
		String[] line = arg1.toString().split(",");
		if (!(line[0].equals("Year") || line[18].equals("NA") || line[14].equals("NA") || line[15].equals("NA"))) {
			int delay = Integer.parseInt(line[15]);
			if (delay > 0) {
				int distance = Integer.parseInt(line[18]);
				int groupId = (distance / 200) + 1;
				String key = "group" + groupId;
				int value = 0;
				int arrDelay = Integer.parseInt(line[14]);
				int depDelay = Integer.parseInt(line[15]);
				if (arrDelay <= depDelay / 2)
					value = 1;
				arg2.collect(new Text(key), new IntWritable(value));
			}
		}
	}

}
