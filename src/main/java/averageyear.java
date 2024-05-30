

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class averageyear {

    public static class AverageYearMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static DoubleWritable salary = new DoubleWritable();
        private Text yearPartition = new Text();
        private boolean isFirstLine = true;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isFirstLine) {
                isFirstLine = false;
                return; 
            }

            String[] fields = value.toString().split(",");
            int workYear = Integer.parseInt(fields[0]); 
            double salaryValue = Double.parseDouble(fields[6]);

            if (workYear == 2024) {
                yearPartition.set("2024");
            } else if (workYear == 2023) {
                yearPartition.set("2023");
            } else {
                yearPartition.set("Before2023");
            }
            salary.set(salaryValue);
            context.write(yearPartition, salary);
        }
    }

    public static class AverageYearReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            double average = sum / count;
            context.write(key, new DoubleWritable(average));
        }
    }
    public static class YearPartitioner extends Partitioner<Text, DoubleWritable> {
        @Override
        public int getPartition(Text key, DoubleWritable value, int numPartitions) {
            String year = key.toString();
            if (year.equals("2024")) {
                return 0;
            } else if (year.equals("2023")) {
                return 1;
            } else {
                return 2;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AverageYear <input path> <output path>");
            System.exit(-1);
        }

      
        Configuration conf = new Configuration();

    
        Job job = Job.getInstance(conf, "Average Year Salary");

        job.setJarByClass(averageyear.class);

    
        job.setMapperClass(AverageYearMapper.class);
        job.setReducerClass(AverageYearReducer.class);
        job.setPartitionerClass(YearPartitioner.class);

        
        job.setNumReduceTasks(3);

    
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

      
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}