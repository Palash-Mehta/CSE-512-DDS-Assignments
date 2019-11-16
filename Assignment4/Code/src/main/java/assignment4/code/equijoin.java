package assignment4.code;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.*;


public class equijoin
{
    //Method for Mapper function
    public static class Map extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        private Text valuepair = new Text();
        private DoubleWritable keypair = new DoubleWritable();
        public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException
        {
            StringTokenizer rowsiterator = new StringTokenizer(value.toString(), "\n");
            while (rowsiterator.hasMoreTokens())
            {
                String row = rowsiterator.nextToken();
                StringTokenizer item = new StringTokenizer(row, ", ");
                String tableName = item.nextToken();
                String id = item.nextToken();
                keypair.set(Double.parseDouble(id));
                valuepair.set(row);
                context.write(keypair, valuepair);
            }
        }
    }
    //Method for reduce function
    public static class Reduce extends Reducer<DoubleWritable, Text, Object, Text>
    {

        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws InterruptedException, IOException
        {
            List<String> rows = new ArrayList<>();
            Text output = new Text();
            String relation1;
            StringBuilder outputStringBuilder = new StringBuilder();
            List<String> relation1rows = new ArrayList<>();
            List<String> relation2rows = new ArrayList<>();


            for (Text value : values)
            {
                rows.add(value.toString());
            }
            if(rows.size() < 2)
            {
                return;
            }
            else
            {
                relation1 = rows.get(0).split(", ")[0];
                for (String tuple : rows)
                {
                    if (relation1.equals(tuple.split(", ")[0]))
                    {
                        relation1rows.add(tuple);
                    }
                    else
                    {
                        relation2rows.add(tuple);
                    }
                }

                if(relation1rows.size() == 0 || relation2rows.size() == 0)
                {
                    return;
                }
                else
                {
                    for (String relation1row : relation1rows)
                    {
                        for (String relation2row : relation2rows)
                        {
                            outputStringBuilder.append(relation1row).append(", ").append(relation2row).append("\n");
                        }
                    }
                    output.set(outputStringBuilder.toString().trim());
                    context.write(null, output);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration config = new Configuration();
        Job j = new Job(config, "equijoin");
        j.setJarByClass(equijoin.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);
        j.setMapOutputKeyClass(DoubleWritable.class);
        j.setMapOutputValueClass(Text.class);
        j.setOutputKeyClass(Object.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, new Path(args[1]));
        FileOutputFormat.setOutputPath(j, new Path(args[2]));
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
}