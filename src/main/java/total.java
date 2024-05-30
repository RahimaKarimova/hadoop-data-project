

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class total {

    public static class TotalSalaryMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable salary = new LongWritable();
        private final static Text word = new Text("Total");
        private boolean isFirstLine = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           
            if (isFirstLine) {
                isFirstLine = false;
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length > 6) {
                try {
                    long salaryValue = Long.parseLong(fields[6]);
                    salary.set(salaryValue);
                    context.write(word, salary);
                } catch (NumberFormatException e) {
                    
                    System.err.println("Invalid salary value: " + fields[6]);
                }
            } else {
                System.err.println("Invalid record: " + value.toString());
            }
        }
    }

    public static class TotalSalaryReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "total salary");
        job.setJarByClass(total.class);
        job.setMapperClass(TotalSalaryMapper.class);
        job.setCombinerClass(TotalSalaryReducer.class);
        job.setReducerClass(TotalSalaryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}