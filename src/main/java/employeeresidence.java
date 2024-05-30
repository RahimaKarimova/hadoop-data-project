

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


import java.io.IOException;

public class employeeresidence {

    public static class EmployeeResidenceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static DoubleWritable salary = new DoubleWritable();
        private Text residence = new Text();
        private boolean isFirstLine = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isFirstLine) {
                isFirstLine = false;
                return;
            }
            String[] fields = value.toString().split(",");
            String country = fields[7];
            double salaryValue = Double.parseDouble(fields[6]);
            if (country.equals("US")) {
                residence.set("US");
            } else {
                residence.set("Non-US");
            }
            salary.set(salaryValue);
            context.write(residence, salary);
        }
    }

    public static class EmployeeResidenceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private MultipleOutputs<Text, DoubleWritable> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
        }

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

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static class ResidencePartitioner extends Partitioner<Text, DoubleWritable> {
        @Override
        public int getPartition(Text key, DoubleWritable value, int numPartitions) {
            if (key.toString().equals("US")) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average salary by employee residence");
        job.setJarByClass(employeeresidence.class);
        job.setMapperClass(EmployeeResidenceMapper.class);
        job.setCombinerClass(EmployeeResidenceReducer.class);
        job.setReducerClass(EmployeeResidenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setPartitionerClass(ResidencePartitioner.class);
        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}