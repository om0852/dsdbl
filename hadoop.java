import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount
{

    // Mapper Class
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>
    {

        private final static IntWritable one =
                new IntWritable(1);

        private Text word =
                new Text();

        public void map(
                Object key,
                Text value,
                Context context
        ) throws IOException, InterruptedException
        {

            StringTokenizer itr =
                    new StringTokenizer(value.toString());

            while (itr.hasMoreTokens())
            {
                word.set(itr.nextToken());

                context.write(word, one);
            }
        }
    }

    // Reducer Class
    public static class IntSumReducer
            extends Reducer<Text, IntWritable,
            Text, IntWritable>
    {

        private IntWritable result =
                new IntWritable();

        public void reduce(
                Text key,
                Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException
        {

            int sum = 0;

            for (IntWritable val : values)
            {
                sum += val.get();
            }

            result.set(sum);

            context.write(key, result);
        }
    }

    // Driver Method
    public static void main(String[] args)
            throws Exception
    {

        Configuration conf =
                new Configuration();

        Job job =
                Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);

        job.setCombinerClass(IntSumReducer.class);

        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        System.exit(
                job.waitForCompletion(true) ? 0 : 1
        );
    }
}



javac -classpath `hadoop classpath` -d wordcount_classes WordCount.java

jar -cvf wordcount.jar -C wordcount_classes/ .

mkdir input

cp input.txt input/

rm -r output

hadoop jar wordcount.jar WordCount input output

cat output/part-r-00000






import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAnalysis
{

    // Mapper Class
    public static class WeatherMapper
            extends Mapper<Object, Text, Text, Text>
    {

        public void map(
                Object key,
                Text value,
                Context context
        ) throws IOException, InterruptedException
        {

            String line = value.toString();

            String[] parts = line.split(",");

            String year = parts[0];

            String temp = parts[1];

            String dew = parts[2];

            String wind = parts[3];

            context.write(
                    new Text(year),
                    new Text(temp + "," + dew + "," + wind)
            );
        }
    }

    // Reducer Class
    public static class WeatherReducer
            extends Reducer<Text, Text, Text, Text>
    {

        public void reduce(
                Text key,
                Iterable<Text> values,
                Context context
        ) throws IOException, InterruptedException
        {

            double tempSum = 0;

            double dewSum = 0;

            double windSum = 0;

            int count = 0;

            for (Text val : values)
            {

                String[] data =
                        val.toString().split(",");

                tempSum += Double.parseDouble(data[0]);

                dewSum += Double.parseDouble(data[1]);

                windSum += Double.parseDouble(data[2]);

                count++;
            }

            double avgTemp = tempSum / count;

            double avgDew = dewSum / count;

            double avgWind = windSum / count;

            context.write(
                    key,
                    new Text(
                            "AvgTemp=" + avgTemp +
                            ", AvgDew=" + avgDew +
                            ", AvgWind=" + avgWind
                    )
            );
        }
    }

    // Driver Class
    public static void main(String[] args)
            throws Exception
    {

        Configuration conf =
                new Configuration();

        Job job =
                Job.getInstance(conf, "Weather Analysis");

        job.setJarByClass(WeatherAnalysis.class);

        job.setMapperClass(WeatherMapper.class);

        job.setReducerClass(WeatherReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        System.exit(
                job.waitForCompletion(true) ? 0 : 1
        );
    }
}

Step 2: Create Input File

Create:

sample_weather.txt

Add:

2024,30,20,10
2024,32,22,12
2024,28,18,8
2024,35,25,15
Step 3: Compile Java Program
javac -classpath `hadoop classpath` -d weather_classes WeatherAnalysis.java
Step 4: Create JAR File
jar -cvf weather.jar -C weather_classes/ .
Step 5: Create Input Directory
mkdir weather_input
Step 6: Copy Input File
cp sample_weather.txt weather_input/
Step 7: Remove Old Output Folder
rm -r weather_output

Ignore error if folder does not exist.

Step 8: Run Hadoop Program
hadoop jar weather.jar WeatherAnalysis weather_input weather_output
Step 9: View Output
cat weather_output/part-r-00000
Expected Output
2024    AvgTemp=31.25, AvgDew=21.25, AvgWind=11.25
Explanation of Working
Mapper Output

Input:

2024,30,20,10

Mapper emits:

(2024, 30,20,10)
Reducer Processing

Reducer:

Adds all temperatures
Adds all dew points
Adds all wind speeds
Divides by total records
Formula for Average

Average=
n
∑x
	​


Complete Command List Together
javac -classpath `hadoop classpath` -d weather_classes WeatherAnalysis.java

jar -cvf weather.jar -C weather_classes/ .

mkdir weather_input

cp sample_weather.txt weather_input/

rm -r weather_output

hadoop jar weather.jar WeatherAnalysis weather_input weather_output

cat weather_output/part-r-00000
Viva Questions with Answers
1. What is Hadoop?
Answer

Hadoop is a framework for distributed storage and big data processing.

2. What is MapReduce?
Answer

MapReduce is a programming model for parallel data processing.

3. What does Mapper do?
Answer

Mapper processes input data and generates key-value pairs.

4. What does Reducer do?
Answer

Reducer aggregates values with same key.

5. What is distributed processing?
Answer

Processing data across multiple systems simultaneously.

6. What is HDFS?
Answer

Hadoop Distributed File System used for storage.

7. Why use Hadoop for weather data?
Answer

Because weather datasets are very large and require distributed processing.

8. What is the key in this program?
Answer

Year is used as key.

9. What are values in reducer?
Answer

Temperature, dew point, and wind speed.

10. Why remove output folder before rerun?
Answer

Hadoop does not overwrite existing output directories.

Common Practical Mistakes
Incorrect split delimiter
Missing Hadoop classpath
Wrong input path
Existing output directory
Forgetting reducer class





Hadoop MapReduce – System Log File Processing
Aim

To design a distributed application using Hadoop MapReduce that processes a system log file and counts occurrences of different log levels such as:

ERROR
INFO
WARN
Theory
What is Log File Processing?

System log files contain records of:

Errors
Warnings
Information messages

MapReduce can process large log files efficiently in distributed systems.

Objective

Read log file and count:

Number of ERROR messages
Number of INFO messages
Number of WARN messages
Sample Log File
File Name
system_log.txt
Sample Input
INFO User login successful
ERROR Database connection failed
WARN Disk space low
INFO File uploaded
ERROR Network timeout
WARN Memory usage high
INFO Logout successful
Expected Output
ERROR 2
INFO 3
WARN 2
Java Program

Save as:

LogProcessor.java
Complete Java Code
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogProcessor
{

    // Mapper Class
    public static class LogMapper
            extends Mapper<Object, Text, Text, IntWritable>
    {

        private final static IntWritable one =
                new IntWritable(1);

        private Text logType =
                new Text();

        public void map(
                Object key,
                Text value,
                Context context
        ) throws IOException, InterruptedException
        {

            String line =
                    value.toString();

            // Split line into words
            String[] words =
                    line.split(" ");

            // First word is log type
            logType.set(words[0]);

            context.write(logType, one);
        }
    }

    // Reducer Class
    public static class LogReducer
            extends Reducer<Text, IntWritable,
            Text, IntWritable>
    {

        private IntWritable result =
                new IntWritable();

        public void reduce(
                Text key,
                Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException
        {

            int sum = 0;

            for (IntWritable val : values)
            {
                sum += val.get();
            }

            result.set(sum);

            context.write(key, result);
        }
    }

    // Driver Class
    public static void main(String[] args)
            throws Exception
    {

        Configuration conf =
                new Configuration();

        Job job =
                Job.getInstance(conf, "Log Processing");

        job.setJarByClass(LogProcessor.class);

        job.setMapperClass(LogMapper.class);

        job.setCombinerClass(LogReducer.class);

        job.setReducerClass(LogReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        System.exit(
                job.waitForCompletion(true) ? 0 : 1
        );
    }
}
Step-by-Step Commands
Step 1: Compile Program
javac -classpath `hadoop classpath` -d log_classes LogProcessor.java
Step 2: Create JAR File
jar -cvf logprocessor.jar -C log_classes/ .
Step 3: Create Input Folder
mkdir log_input
Step 4: Copy Input File
cp system_log.txt log_input/
Step 5: Remove Old Output Folder
rm -r log_output

Ignore error if folder does not exist.

Step 6: Run Hadoop Program
hadoop jar logprocessor.jar LogProcessor log_input log_output
Step 7: View Output
cat log_output/part-r-00000
Final Output
ERROR 2
INFO 3
WARN 2
Working of Program
Mapper Phase

Input:

ERROR Database connection failed

Mapper emits:

(ERROR,1)
Reducer Phase

Reducer adds all counts:

ERROR 2
Formula Used

Count Calculation:

Count=∑1

Complete Commands Together
javac -classpath `hadoop classpath` -d log_classes LogProcessor.java

jar -cvf logprocessor.jar -C log_classes/ .

mkdir log_input

cp system_log.txt log_input/

rm -r log_output

hadoop jar logprocessor.jar LogProcessor log_input log_output

cat log_output/part-r-00000
Viva Questions with Answers
1. What is Hadoop?
Answer

Hadoop is a framework for distributed storage and processing of big data.

2. What is MapReduce?
Answer

MapReduce is a programming model for parallel data processing.

3. What is log processing?
Answer

Analyzing system logs to identify errors, warnings, and events.

4. What does Mapper do?
Answer

Mapper converts input data into key-value pairs.

5. What does Reducer do?
Answer

Reducer combines values having same key.

6. What is Combiner?
Answer

Combiner performs local aggregation before reducer.

7. What is HDFS?
Answer

Hadoop Distributed File System used for storing big data.

8. What is the key in this program?
Answer

Log type:

ERROR
INFO
WARN
9. Why use Hadoop for log processing?
Answer

Because log files can become extremely large.

10. What is distributed computing?
Answer

Processing data across multiple systems simultaneously.

Common Practical Mistakes
Wrong class name
Existing output folder
Missing Hadoop classpath
Incorrect file paths
Wrong split logic
One-Line Conclusion

The Hadoop MapReduce log processing application efficiently analyzes large system log files by counting occurrences of different log m