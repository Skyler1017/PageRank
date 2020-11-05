package hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class Sort {
    public static class pageSortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        private static DoubleWritable keyInfo = new DoubleWritable();
        private static Text valueInfo = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context);

            //若A能连接到B，则说明B是A的一条出链
            if (!value.toString().equals("")) {
                String[] line = value.toString().split("\t");
                String curPageName = line[0];

                double pr = Double.parseDouble(line[1]);

                DecimalFormat df = new DecimalFormat("0.0000000000");
                String Prvalue = df.format(pr);
                String vout = "(" + curPageName + "," + Prvalue + ")";

                valueInfo.set(vout);
                keyInfo.set(pr);

                context.write(keyInfo, valueInfo);
            }

        }
    }

    public static class pageSortReducer extends Reducer<DoubleWritable, Text, NullWritable, Text> {

        private static Text info = new Text();

        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                info.set(value.toString());
                context.write(NullWritable.get(), info);
            }
        }
    }

}
