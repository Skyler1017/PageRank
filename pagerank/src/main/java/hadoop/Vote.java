package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Vote {
    public static class pageMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private static Text keyInfo = new Text();
        private static DoubleWritable valueInfo = new DoubleWritable();
        private static String[] lastWeightsInfo;

        @Override
        protected void setup(Context context) {
            try {
                //从全局配置获取配置参数
                Configuration conf = context.getConfiguration();
                lastWeightsInfo = conf.get("lastWeights").split(","); //这样就拿到了
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //分离邻接链表
            String[] line = value.toString().split("\t");
            if (line.length == 2) {
                String curPageName = line[0];
                String[] outPages = line[1].split(",");

                double outNum = outPages.length;
                double ww = 0.0;
                for (int i = 0; i < lastWeightsInfo.length; i++) {
                    if (curPageName.equals(lastWeightsInfo[i])) {
                        ww = Double.parseDouble(lastWeightsInfo[i + 1]) / outNum;
                        break;
                    }
                }
                for (String outPage : outPages) {
                    keyInfo.set(outPage);
                    valueInfo.set(ww);
                    context.write(keyInfo, valueInfo);
                }
            }
        }
    }

    public static class pageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private static DoubleWritable info = new DoubleWritable();
        //private static int totalNum;

        /* @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            try {
                //从全局配置获取配置参数
                Configuration conf = context.getConfiguration();
                //totalNum = conf.getInt("totalNum", 0);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }  */
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;// 统计PR
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            sum = 0.85 * sum + 0.15;
            info.set(sum);
            context.write(key, info);
        }
    }

}
