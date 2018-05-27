package query5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DayOfWeekDelayMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

	String[] days = {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"};
	
	@Override
	public void map(LongWritable arg0, Text arg1, OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException {
		String[] line = arg1.toString().split(",");
		if(!(line[0].equals("Year") || line[4].equals("NA") || line[14].equals("NA"))) {
			int group = (Integer.parseInt(line[4])/600)+1;
			if(group==5)
				group=1;
			String key = line[0]+"-"+ days[Integer.parseInt(line[3])-1]+"-"+group;
			int value = 0;
			if(Integer.parseInt(line[14])>0)
				value = 1;
			arg2.collect(new Text(key), new IntWritable(value));
		}
		
	}
	
}
