import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class PageRankInitiate
{
    public static class PageRankInitiateMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            String [] token = line.split("\t");
            String myValue = "1.0\t" + token[1];
            context.write(new Text(token[0]), new Text(myValue));
        }
    }
    public static void main(String [] filepath)
            throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRankInitiate");
        job.setJarByClass(PageRankInitiate.class);
        job.setMapperClass(PageRankInitiate.PageRankInitiateMapper.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(filepath[0]));
        FileOutputFormat.setOutputPath(job, new Path(filepath[1]));
        job.waitForCompletion(true);
    }
}

class PageRankIterator
{
    public static class PageRankIteratorMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            String [] token = line.split("\t");
            String pageKey = token[0];
            double prvalue = Double.parseDouble(token[1]);
            if(token.length > 2)
            {
                String [] linkpeople = token[2].split(" ");
                for(String linkperson : linkpeople)
                {
                    String [] innersplit = linkperson.split(":");
                    Double singlepr = Double.parseDouble(innersplit[1]);
                    String pageValue = pageKey + "\t" + String.valueOf(prvalue * singlepr);
                    context.write(new Text(innersplit[0]), new Text(pageValue));
                }
                context.write(new Text(pageKey), new Text("#" + token[2]));
            }
        }
    }
    public static class PageRankIteratorReducer extends Reducer<Text, Text, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            double pagerank = 0;
            double d = 0.85;
            String suffix = "";
            for(Text value : values)
            {
                String temp = value.toString();
                if(temp.startsWith("#"))
                    suffix = "\t" + temp.substring(temp.indexOf("#") + 1);
                else
                {
                    String [] token = temp.split("\t");
                    if(token.length > 1)
                        pagerank += Double.parseDouble(token[1]);
                }
            }
            pagerank = (1 - d) + d * pagerank;
            context.write(new Text(key), new Text(String.valueOf(pagerank) + suffix));
        }
    }
    public static void main(String [] filepath)
        throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRank");
        job.setJarByClass(PageRankIterator.class);
        job.setMapperClass(PageRankIterator.PageRankIteratorMapper.class);
        job.setReducerClass(PageRankIterator.PageRankIteratorReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(filepath[0]));
        FileOutputFormat.setOutputPath(job, new Path(filepath[1]));
        job.waitForCompletion(true);
    }
}

class PageRankSorting
{
    public static class PageRankSortingMapper extends Mapper<LongWritable, Text, myDoubleWritable, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            String [] token = line.split("\t");
            double pagerank = Double.parseDouble(token[1]);
            String myValue = token[0] + "#" + token[2];
            context.write(new myDoubleWritable(pagerank), new Text(myValue));
        }
    }
    public static class PageRankSortingReducer extends Reducer<myDoubleWritable, Text, Text, Text>
    {
        @Override
        public void reduce(myDoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            String strpr = key.toString();
            for(Text value : values)
            {
                String line = value.toString();
                String [] token = line.split("#");
                String myValue = strpr + "\t" + token[1];
                context.write(new Text(token[0]), new Text(myValue));
            }
        }
    }

//    public static class PageRankSortingMapper extends Mapper<LongWritable, Text, myDoubleWritable, Text>
//    {
//        @Override
//        public void map(LongWritable key, Text value, Context context)
//                throws IOException, InterruptedException
//        {
//            String line = value.toString();
//            String [] token = line.split("\t");
//            double pagerank = Double.parseDouble(token[1]);
//            String myValue = token[0];
//            context.write(new myDoubleWritable(pagerank), new Text(myValue));
//        }
//    }
//    public static class PageRankSortingReducer extends Reducer<myDoubleWritable, Text, Text, Text>
//    {
//        @Override
//        public void reduce(myDoubleWritable key, Iterable<Text> values, Context context)
//                throws IOException, InterruptedException
//        {
//            String strpr = key.toString();
//            for(Text value : values)
//            {
//                String myKey = value.toString();
//                context.write(new Text(myKey), new Text(strpr));
//            }
//        }
//    }

    public static class myDoubleWritable implements WritableComparable
    {
        double value;

        public myDoubleWritable()
        {
            super();
        }

        public myDoubleWritable(double value)
        {
            super();
            this.value = value;
        }

        public double getValue()
        {

            return value;
        }

        public void setValue(double value)
        {
            this.value = value;
        }

        @Override
        public void write(DataOutput out) throws IOException
        {
            out.writeDouble(value);
        }

        @Override
        public void readFields(DataInput in) throws IOException
        {
            this.value = in.readDouble();
        }

        @Override
        public int compareTo(Object val)
        {
            myDoubleWritable other = (myDoubleWritable)val;
            if(value > other.getValue())
                return -1;
            else if(value < other.getValue())
                return 1;
            else
                return 0;
        }

        @Override
        public String toString()
        {
            return Double.toString(value);
        }
    }
    public static void main(String [] filepath)
            throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRankSorting");
        job.setJarByClass(PageRankSorting.class);
        job.setMapperClass(PageRankSorting.PageRankSortingMapper.class);
        job.setReducerClass(PageRankSorting.PageRankSortingReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(myDoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(filepath[0]));
        FileOutputFormat.setOutputPath(job, new Path(filepath[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

public class PageRank
{
    public static void main(String[] args)
            throws Exception
    {
        String [] filepath = {"", ""};

        //pre processing, setting initial pagerank value 1.0 for each vertices
        filepath[0] = args[0];
        filepath[1] = args[1] + "/Folder0";
        PageRankInitiate.main(filepath);

        int times = Integer.parseInt(args[2]); //pagerank iterate times
        //pagerank iteration
        for(int i = 0; i < times; i++)
        {
            filepath[0] = args[1] + "/Folder" + String.valueOf(i);
            filepath[1] = args[1] + "/Folder" + String.valueOf(i + 1);
            PageRankIterator.main(filepath);
        }

        //sorting pagerank values
        filepath[0] = args[1] + "/Folder" + String.valueOf(times);
        //filepath[1] = args[1] + "/Folder" + String.valueOf(times + 1);
        filepath[1] = args[1] + "/PageRankOutcome";
        PageRankSorting.main(filepath);
    }
}
