package org.toygar;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalaryAnalysis {

    public static class TotalSalaryMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable salary = new LongWritable(0);
        private final static Text total = new Text("total");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            if (!line[6].equals("salary_in_usd")) {
                salary.set(Long.parseLong(line[6].trim()));
                context.write(total, salary);
            }
        }
    }

    public static class TotalSalaryReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class JobTitleSalaryMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text jobTitle = new Text();
        private LongWritable salary = new LongWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            if (!line[3].equals("job_title")) {
                jobTitle.set(line[3].trim());
                salary.set(Long.parseLong(line[6].trim()));
                context.write(jobTitle, salary);
            }
        }
    }

    public static class JobTitleSalaryReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            int count = 0;
            for (LongWritable val : values) {
                sum += val.get();
                count++;
            }
            long average = sum / count;
            result.set(average);
            context.write(key, result);
        }
    }

    public static class TitleExperienceMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text titleExp = new Text();
        private LongWritable salary = new LongWritable();
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }
            String[] line = value.toString().split(",");
            if (!line[0].equals("job_title")) {
                String title = line[3].trim();
                String experience = line[1].trim();
                titleExp.set(title + "_" + experience);
                salary.set(Long.parseLong(line[6].trim()));
                context.write(titleExp, salary);
            }
        }
    }

    public static class TitleExperienceReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            int count = 0;
            for (LongWritable val : values) {
                sum += val.get();
                count++;
            }
            long average = sum / count;
            result.set(average);
            context.write(key, result);
        }
    }

    public static class EmployeeResidenceMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text residence = new Text();
        private LongWritable salary = new LongWritable();
        private boolean isHeader = true;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }
            String[] line = value.toString().split(",");
            if (line.length > 7 && !line[0].equals("work_year")) {
                String residenceCode = line[7].trim();
                if (residenceCode.equals("US")) {
                    residence.set("US");
                } else {
                    residence.set("nonUS");
                    System.out.println("Mapper nonUS Output: " + residence.toString() + " " + line[6].trim());
                }
                salary.set(Long.parseLong(line[6].trim()));
                System.out.println("Mapper Output: " + residence.toString() + " " + salary.toString());
                context.write(residence, salary);
            }
        }
    }



    public static class EmployeeResidenceReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();
        private MultipleOutputs<Text, LongWritable> multipleOutputs;

        @Override
        protected void setup(Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            int count = 0;
            for (LongWritable val : values) {
                sum += val.get();
                count++;
            }
            long average = sum / count;
            result.set(average);
            System.out.println("Reducer Output: " + key.toString() + " " + result.toString());
            if (key.toString().equals("US")) {
                multipleOutputs.write("US", key, result, "part-r-00000");
            } else {
                multipleOutputs.write("nonUS", key, result, "part-r-00001");
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }


    public static class AverageYearMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text year = new Text();
        private LongWritable salary = new LongWritable();
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }
            String[] line = value.toString().split(",");
            if (!line[0].equals("work_year")) {
                String workYear = line[0].trim();
                int yearInt = Integer.parseInt(workYear);
                if (yearInt == 2024) {
                    year.set("2024");
                } else if (yearInt == 2023) {
                    year.set("2023");
                } else {
                    year.set("before_2023");
                }
                salary.set(Long.parseLong(line[6].trim()));
                context.write(year, salary);
            }
        }
    }

    public static class AverageYearReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();
        private MultipleOutputs<Text, LongWritable> multipleOutputs;

        @Override
        protected void setup(Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            int count = 0;
            for (LongWritable val : values) {
                sum += val.get();
                count++;
            }
            long average = sum / count;
            result.set(average);
            if (key.toString().equals("2024")) {
                multipleOutputs.write("year2024", key, result, "part-r-00000");
            } else if (key.toString().equals("2023")) {
                multipleOutputs.write("year2023", key, result, "part-r-00001");
            } else {
                multipleOutputs.write("before2023", key, result, "part-r-00002");
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "salary analysis");
        job.setJarByClass(SalaryAnalysis.class);

        String task = args[1];
        Path inputPath = new Path(args[2]);
        Path outputPath = new Path(args[3]);

        switch (task) {
            case "total":
                job.setMapperClass(TotalSalaryMapper.class);
                job.setReducerClass(TotalSalaryReducer.class);
                break;
            case "jobtitle":
                job.setMapperClass(JobTitleSalaryMapper.class);
                job.setReducerClass(JobTitleSalaryReducer.class);
                break;
            case "titleexperience":
                job.setMapperClass(TitleExperienceMapper.class);
                job.setReducerClass(TitleExperienceReducer.class);
                break;
            case "employeeresidence":
                job.setMapperClass(EmployeeResidenceMapper.class);
                job.setReducerClass(EmployeeResidenceReducer.class);
                MultipleOutputs.addNamedOutput(job, "US", TextOutputFormat.class, Text.class, LongWritable.class);
                MultipleOutputs.addNamedOutput(job, "nonUS", TextOutputFormat.class, Text.class, LongWritable.class);
                break;
            case "averageyear":
                job.setMapperClass(AverageYearMapper.class);
                job.setReducerClass(AverageYearReducer.class);
                MultipleOutputs.addNamedOutput(job, "year2024", TextOutputFormat.class, Text.class, LongWritable.class);
                MultipleOutputs.addNamedOutput(job, "year2023", TextOutputFormat.class, Text.class, LongWritable.class);
                MultipleOutputs.addNamedOutput(job, "before2023", TextOutputFormat.class, Text.class, LongWritable.class);
                break;
            default:
                System.err.println("Invalid task specified.");
                System.exit(-1);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
