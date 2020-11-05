package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

    public static boolean calculate(Configuration conf, int i) {
        try {
            Job job = Job.getInstance(conf, "page_rank_calculate");
            job.setJarByClass(Vote.class);
            job.setMapperClass(Vote.pageMapper.class);
            job.setReducerClass(Vote.pageReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.setInputPaths(job, new Path("/pagerank/input"));
            // 指定处理完成之后的结果所保存的位置
            Path outputPath = new Path("/pagerank/output/output" + i);
            outputPath.getFileSystem(conf).delete(outputPath, true);
            FileOutputFormat.setOutputPath(job, new Path(outputPath.toString()));
            boolean f = job.waitForCompletion(true);
            return f;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void sortOutput(Configuration conf, int round) {
        try {
            Job job = Job.getInstance(conf, "page_rank_sort");

            job.setJarByClass(Util.class);
            job.setSortComparatorClass(Util.IntWritableDecreasingComparator.class);

            job.setMapperClass(Sort.pageSortMapper.class);
            job.setReducerClass(Sort.pageSortReducer.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            Path lastOutputPath = new Path("/pagerank/output/output" + round);
            FileInputFormat.setInputPaths(job, lastOutputPath);
            // 指定处理完成之后的结果所保存的位置

            Path finalOutputPath = new Path("/pagerank/output/final_output");
            finalOutputPath.getFileSystem(conf).delete(finalOutputPath, true);
            FileOutputFormat.setOutputPath(job, finalOutputPath);
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");


        //得到总的网页数，初始化网页权重为1 input/DataSet
        String remoteFilePath = "/pagerank/input/DataSet"; // HDFS路径
        String lastWeights = Util.catWeights(conf, remoteFilePath);
        //传入全局变量
        conf.set("lastWeights", lastWeights);

        int round = 0; //运行的总轮数

        //先单独运行一遍
        calculate(conf, ++round);

        for (int i = 0; i < 1; i++) {
            remoteFilePath = "/pagerank/output/output" + round + "/part-r-00000"; // 上一轮运行的结果文件
            // 先把上一轮的结果汇总写入到 Configuration 中
            lastWeights = Util.getLastWeights(conf, remoteFilePath);
            conf.set("lastWeights", lastWeights);
            calculate(conf, ++round);
        }
        //得到符合格式的降序排序输出
        sortOutput(conf, round);
    }
}
