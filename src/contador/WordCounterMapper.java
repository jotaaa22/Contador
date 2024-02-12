package contador;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	private final static Pattern SPLIT_PATTERN = Pattern.compile("\\s*\\b\\s*");
	private List<String> etiquetas = Arrays.asList("info", "warn", "severe");
	
	@Override
	public void map(LongWritable key, Text text, Context context) 
		throws IOException, InterruptedException{
		String line = value.toString();
		line = line.replaceAll("[^a-zA-z0-9 ]", "").toLowerCase();
		Text palabraActual = new Text();
		
		String workd[] = SPLIT_PATTERN.split(line);
		
		for(int i=0; i<workd.length; i++) {
			if(workd[i].isEmpty()) {
				continue;
			}
			if(etiquetas.contains(workd)) {
			palabraActual = new Text(workd[i]);
			context.write(palabraActual, one);
			}
		}
	}
}

