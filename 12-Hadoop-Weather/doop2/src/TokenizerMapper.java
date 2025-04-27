import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {

    private Text category = new Text();
    private FloatWritable temperature = new FloatWritable();
    private FloatWritable windSpeed = new FloatWritable();
    private FloatWritable dewPoint = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] cols = value.toString().split(" ");
        float temp = Float.parseFloat(cols[0]);
        float wind = Float.parseFloat(cols[1]);
        float dew = Float.parseFloat(cols[2]);

        category.set("Temperature");
        temperature.set(temp);
        context.write(category, temperature);

        category.set("WindSpeed");
        windSpeed.set(wind);
        context.write(category, windSpeed);

        category.set("DewPoint");
        dewPoint.set(dew);
        context.write(category, dewPoint);
    }
}
