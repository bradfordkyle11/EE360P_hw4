import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.*;
import java.util.TreeMap;
import java.util.StringTokenizer;

// Do not change the signature of this class
public class TextAnalyzer extends Configured implements Tool {

    // Replace "?" with your own output key / value types
    // The four template data types are:
    //     <Input Key Type, Input Value Type, Output Key Type, Output Value Type>
    public static class TextMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            // Implementation of you mapper function
            String line = value.toString().toLowerCase();
            line = line.replaceAll("[^a-z0-9]", " ");
            
            StringTokenizer itr = new StringTokenizer(line);
            
            //count frequency of each word in the line
            MapWritable wordFreq = new MapWritable(); //Map<Text, IntWritable>
            while (itr.hasMoreTokens())
            {
                Text word = new Text(itr.nextToken());
                if (wordFreq.containsKey (word))
                {
                    IntWritable tmp = (IntWritable) wordFreq.get(word);
                    tmp.set (tmp.get() + 1);
                    wordFreq.put(word, tmp);
                }
                else
                    wordFreq.put (word, new IntWritable(1));
            }

            //map each contextword to queryword frequencies for the line
            itr = new StringTokenizer(line);
            for (Writable word : wordFreq.keySet())
            {
                MapWritable querywordFreq = new MapWritable(wordFreq);
                IntWritable tmp = (IntWritable) querywordFreq.get(word);
                tmp.set(tmp.get() - 1);
                if (tmp.get() == 0)
                    querywordFreq.remove(word);
                context.write((Text) word, querywordFreq);
            }
        }
    }

    // Replace "?" with your own key / value types
    // NOTE: combiner's output key / value types have to be the same as those of mapper
    // public static class TextCombiner extends Reducer<?, ?, ?, ?> {
    //     public void reduce(Text key, Iterable<Tuple> tuples, Context context)
    //         throws IOException, InterruptedException
    //     {
    //         // Implementation of you combiner function
    //     }
    // }

    // Replace "?" with your own input key / value types, i.e., the output
    // key / value types of your mapper function
    public static class TextReducer extends Reducer<Text, MapWritable, Text, Text> {
        private final static Text emptyText = new Text("");

        public void reduce(Text key, Iterable<MapWritable> queryTuples, Context context)
            throws IOException, InterruptedException
        {
            TreeMap<Text, Integer> totalFreqMap = new TreeMap<>();
            // Implementation of you reducer function
            for (MapWritable freq : queryTuples)
            {
                for (Writable writableWord : freq.keySet())
                {
                    Text word = (Text) writableWord;
                    int inc = ((IntWritable) freq.get(word)).get();
                    if (totalFreqMap.containsKey(word))
                    {
                        int sum = inc + totalFreqMap.get(word);
                        totalFreqMap.put(word, sum);
                    }
                    else
                        totalFreqMap.put(word, inc);
                }
            }

            
            Text max = totalFreqMap.firstKey();
            for (Text word : totalFreqMap.keySet())
            {
                if (totalFreqMap.get(word) > totalFreqMap.get(max))
                max = word;
            }
            
            // Write out the results; you may change the following example
            // code to fit with your reducer function.
            //   Write out the current context key
            Text maxCount = new Text(String.valueOf(totalFreqMap.get(max)));
            context.write(key, maxCount);
            context.write(max, maxCount);
            
            totalFreqMap.remove(max);

            //   Write out query words and their count
            for(Text queryWord: totalFreqMap.navigableKeySet()){
                String count = totalFreqMap.get(queryWord).toString() + ">";
                Text word = new Text("<" + queryWord.toString() + ",");
                context.write(word, new Text(count));
            }
            //   Empty line for ending the current context key
            context.write(emptyText, emptyText);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "KMB3534_STY223"); // Replace with your EIDs
        job.setJarByClass(TextAnalyzer.class);

        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);
        //   Uncomment the following line if you want to use Combiner class
        // job.setCombinerClass(TextCombiner.class);
        job.setReducerClass(TextReducer.class);

        // Specify key / value types (Don't change them for the purpose of this assignment)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //   If your mapper and combiner's  output types are different from Text.class,
        //   then uncomment the following lines to specify the data types.
        //job.setMapOutputKeyClass(?.class);
        job.setMapOutputValueClass(MapWritable.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // Do not modify the main method
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(res);
    }

    // You may define sub-classes here. Example:
    // public static class MyClass {
    //
    // }
}



