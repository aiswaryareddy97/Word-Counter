import java.io.*;
import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;

public class TopkCommonWords {
    public static class File1Mapper
            extends Mapper<Object, Text, Text, IntWritable> {

        ArrayList<String> stopwords = new ArrayList<>();

        // to read and store all the stopwords into an ArrayList
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0)
            {
                String line = "";

                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path getFilePath = new Path(cacheFiles[0].toString());

                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

                while ((line = reader.readLine()) != null)
                {
                    String[] words = line.split(" ");

                    for (int i = 0; i < words.length; i++)
                    {
                        // add the words to ArrayList
                        stopwords.add(words[i]);
                    }
                }
                //}
            }

        }

        private Text word = new Text();
        private final static IntWritable one_f1 = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //splitting the input in file1 with “(space)\t\n\r\f”
            String[] words = value.toString().split("[\\s\\t\\n\\r\\f]");

            for (String itr : words) {
                // if itr is not in the list of stopwords, write itr to context
                // implementation for stopwords removal
                if (!stopwords.contains(itr.toString())) {
                    word.set(itr);
                    context.write(word, one_f1);
                }

            }
        }

    }

    public static class File2Mapper
            extends Mapper<Object, Text, Text, IntWritable> {

        ArrayList<String> stopwords = new ArrayList<String>();

        // to read and store all the stopwords into an ArrayList
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0)
            {
                String line = "";

                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path getFilePath = new Path(cacheFiles[0].toString());

                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

                while ((line = reader.readLine()) != null)
                {
                    String[] words = line.split(" ");

                    for (int i = 0; i < words.length; i++)
                    {
                        // add the words to ArrayList
                        stopwords.add(words[i]);
                    }
                }
                //}
            }

        }

        private Text word = new Text();
        private final static IntWritable one_f2 = new IntWritable(2);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //splitting the input in file2 with “(space)\t\n\r\f”
            String[] words = value.toString().split("[\\s\\t\\n\\r\\f]");

            for (String itr : words) {
                if (!stopwords.contains(itr.toString())) {
                    word.set(itr);
                    context.write(word, one_f2);
                }

            }
        }
    }

    public static class FileReducer extends
            Reducer<Text, IntWritable, IntWritable, Text> {

        // TreeMap is used to sort based on keys
        private SortedMap<Integer, String> myMap =
                new TreeMap<Integer, String>(new Comparator<Integer>()
                {
                    @Override
                    public int compare(Integer o1, Integer o2)
                    {
                        // To sort keys in descending order
                        return o2.compareTo(o1);
                    }
                });

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            // Calculating the count of each common word in both the files
            int count = 0;
            int count_1 = 0;
            int count_2 = 0;
            for (IntWritable val : values) {
                String item = new String();
                item = val.toString();
                if (item.equals("1")) {
                    count_1 = count_1 + 1;
                } else if (item.equals("2")) {
                    count_2 = count_2 + 1;
                }
            }

            // Group A -  find the minimum count between the two files for each common word
            if (count_1 != 0 && count_2 != 0) {
                count = Math.min(count_1, count_2);

                // check if there are existing words with the same key(count),
                // if yes, add the words to sameCountWords and join by ','
                // process further to order same count words by desc. lexicographic ordering
                if (myMap.containsKey(count)) {
                    String sameCountWords = myMap.get(count) + "," + key.toString();
                    myMap.put(count, sameCountWords);
                } else {
                    myMap.put(count, key.toString());
                }
            }
        }

        // to sort by descending lexicographic ordering if there's words with same count
        protected void cleanup(Context context) throws IOException, InterruptedException {

            int n = 0;
            // to get the count of only top 20 words
            for (Integer key : myMap.keySet()) {
                if (n > 20)
                    break;
                else {
                    ArrayList<String> words = new ArrayList<>();
                    String[] tmp = myMap.get(key).split(",");
                    for (String itr : tmp)
                        words.add(itr);
                    for (String word : words){
                        if(!word.equals(""))
                            context.write(new IntWritable(key), new Text(word));
                    }
                    // increasing n by the number of words that have same count
                    n = n + myMap.get(key).split(",").length;
                }
            }
        }
    }

    public static void main(String [] args) throws Exception{
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(TopkCommonWords.class);

        job.setMapperClass(File1Mapper.class);
        job.setMapperClass(File2Mapper.class);
        job.setReducerClass(FileReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI(args[2]));

        // Using MultipleInputs as there are multiple mappers
        // and the inputs will be 2 different files
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, File1Mapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, File2Mapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        boolean i = job.waitForCompletion(true);
        System.exit(i ? 0 : 1);

    }
}
