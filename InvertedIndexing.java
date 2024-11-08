import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class InvertedIndexing {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        JobConf conf;
        public void configure( JobConf job ) {
            this.conf = job;
        }

        public void map(LongWritable docId, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            // retrieve # keywords from JobConf
            int argc = Integer.parseInt( conf.get( "argc" ) );

            // get the current file name
            FileSplit fileSplit = ( FileSplit )reporter.getInputSplit( );
            String filename = "" + fileSplit.getPath( ).getName( );

            // hashtable for keyword counts
            HashMap<String, int> keywordCount = new HashMap<String, int>;
            for (int i = 0; i < argc; i++) {
                keywordCount.put(conf.get("keyword" + i), 0); // initialize each keyword's count to 0
            }

            // iterate through words in line and update keyword counts
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                if (keywordCount.containsKey(word)) {
                    int updatedCount = keywordCount.get(word) + 1;
                    keywordCount.put(word, updatedCount);
                }
            }

            // pass to reduce
            for (String keyword : keywordCount) {
                if (keywordCount.get(keyword) > 0) {
                    output.collect(Text(keyword), Text(filename + " " + String.valueOf(keywordCount.get(keyword))));
                }
            }

    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            // actual computation is here
            // CONTINUE IMPLEMENTATION...

            // finally, print it out
            output.collect(key, docListText );

        }

    }

    public static void main(String[] args) throws Exception {

        // input format:
        // hadoop jar invertedindexes.jar InvertedIndexes input output keyword1 keyword2 ...

        JobConf conf = new JobConf(InvertedIndexing.class);
        conf.setJobName("invertedindexing");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0])); // input directory name
        FileOutputFormat.setOutputPath(conf, new Path(args[1])); // output directory name

        conf.set( "argc", String.valueOf( args.length - 2 ) ); // argc maintains #keywords
        for ( int i = 0; i < args.length - 2; i++ )
            conf.set( "keyword" + i, args[i + 2] ); // keyword1, keyword2, ...

        JobClient.runJob(conf);

    }

}
