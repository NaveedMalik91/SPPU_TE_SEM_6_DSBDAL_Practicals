import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");
        for (String token : tokens) {
            String cleanedToken = token.toLowerCase().replaceAll("[^a-zA-Z]", "");
            if (!cleanedToken.isEmpty()) {
                word.set(cleanedToken);
                context.write(word, ONE);
            }
        }
    }
}
