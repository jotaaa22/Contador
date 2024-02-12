package contador;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<IntWritable>values, 
			Reducer<Text, IntWritable, Text, IntWritable>, Context context) throws IOException, InterruptedExceptio{
		int sumador = 0;
		
		for(IntWritable count : values) {
			
			sumador += count.get();
		}
		context.write(key, new IntWritable(sumador));
	}

}
