

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class titleexperince {

    public static class TitleExperienceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static DoubleWritable salary = new DoubleWritable();
        private Text titleExperience = new Text();
        private boolean isFirstLine = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isFirstLine) {
                isFirstLine = false;
                return;
            }
            String[] fields = value.toString().split(",");
            String title = fields[3]; 
            String experience = fields[1]; 
            double salaryValue = Double.parseDouble(fields[6]); 
            titleExperience.set(title + "_" + experience);
            salary.set(salaryValue);
            context.write(titleExperience, salary);
        }
    }

    public static class TitleExperienceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average salary by job title and experience");
        job.setJarByClass(titleexperince.class);
        job.setMapperClass(TitleExperienceMapper.class);
        job.setCombinerClass(TitleExperienceReducer.class); 
        job.setReducerClass(TitleExperienceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}