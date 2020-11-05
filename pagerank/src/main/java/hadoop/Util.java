package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparator;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;


public class Util {

    public static int totalNum;

    public static String catWeights(Configuration conf, String remoteFilePath) {
        Path remotePath = new Path(remoteFilePath);
        String pweights = "";
        try (FileSystem fs = FileSystem.get(conf);
             FSDataInputStream in = fs.open(remotePath);
             BufferedReader d = new BufferedReader(new InputStreamReader(in));) {
            String line;
            int count = 0;
            while ((line = d.readLine()) != null) {
                String[] sstr = line.split("\t");
                if (sstr.length == 2) {
                    String ss = sstr[0];
                    pweights += ss + ",1.0,";
                    count++;
                }
            }
            totalNum = count;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return pweights;
    }

    public static String getLastWeights(Configuration conf, String remoteFilePath) {
        Path remotePath = new Path(remoteFilePath);
        //String Swords[] = new String[100];
        //ArrayList<String> strArray = new ArrayList<String> ();
        String pweights = "";
        try (FileSystem fs = FileSystem.get(conf);
             FSDataInputStream in = fs.open(remotePath);
             BufferedReader d = new BufferedReader(new InputStreamReader(in));) {
            String line;
            while ((line = d.readLine()) != null) {

                String[] sstr = line.split("\t");
                if (sstr.length == 2) {
                    String ss = sstr[0];
                    pweights += ss + "," + sstr[1] + ",";
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return pweights;
    }

    public class DescSort extends WritableComparator {

        public DescSort() {
            super(DoubleWritable.class, true);//注册排序组件
        }

        @Override
        @SuppressWarnings("all")
        public int compare(WritableComparable a, WritableComparable b) {
            double aa = ((DoubleWritable) a).get();
            double bb = ((DoubleWritable) b).get();
            if ((aa - bb) > 0) {
                return -1;
            } else {
                return 1;
            }
        }
    }

    public static class IntWritableDecreasingComparator extends IntWritable.Comparator {

        public int compare(WritableComparable a, WritableComparable b) {

            return -super.compare(a, b);

        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

            return -super.compare(b1, s1, l1, b2, s2, l2);

        }
    }

}