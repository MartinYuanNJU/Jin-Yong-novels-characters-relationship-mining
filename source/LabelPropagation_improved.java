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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

class LPAInitiate
{
    private static int lablenumber = 1;
    public static class LPAInitiateMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            String [] token = line.split("\t");
            String label = String.valueOf(lablenumber);
            String myValue = label + "\t" + token[1] + "\t" + token[2];
            context.write(new Text(token[0]), new Text(myValue));
            lablenumber++;
        }
    }
    public static void main(String [] filepath)
            throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LPAInitiate");
        job.setJarByClass(LPAInitiate.class);
        job.setMapperClass(LPAInitiate.LPAInitiateMapper.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(filepath[0]));
        FileOutputFormat.setOutputPath(job, new Path(filepath[1]));
        job.waitForCompletion(true);
    }
}

class LPAIterator
{
    private static HashMap<String, String> personmap = new HashMap<String, String>();
    public static class LPAIteratorMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            String [] token = line.split("\t");
            String Key = token[0];
            String label = token[1];
            String noteValue = Key + "#label:" + label;
            if(token.length > 3)
            {
                String [] linkpeople = token[3].split(" ");
                for(String linkperson : linkpeople)
                {
                    String [] innersplit = linkperson.split(":");
                    //emit B A#label:1
                    context.write(new Text(innersplit[0]), new Text(noteValue));
                    //emit A B#weight:0.3
                    context.write(new Text(Key), new Text(innersplit[0] + "#weight:" + innersplit[1]));
                }
                context.write(new Text(Key), new Text("#" + token[2] + "\t" + token[3]));
            }
        }
    }
    public static class LPAIteratorReducer extends Reducer<Text, Text, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            String suffix = "";
            List<String> cache = new ArrayList<String>();
            for(Text value : values)
            {
                String temp = value.toString();
                cache.add(temp);
                if(temp.startsWith("#"))
                    suffix = "\t" + temp.substring(temp.indexOf("#") + 1);
                else
                {
                    String [] token = temp.split("#");
                    String nowperson = token[0];
                    if(token[1].startsWith("label"))
                    {
                        String [] innertoken = token[1].split(":");
                        if(!personmap.containsKey(nowperson))
                            personmap.put(nowperson, innertoken[1]);
                    }
                }
            }
            HashMap<String, Double> labelmap = new HashMap<String, Double>();
            Iterator<String> iter = cache.iterator();
            while(iter.hasNext())
            {
                String temp = (String)iter.next();
                if(temp.startsWith("#"))
                {
                    //do nothing
                }
                else
                {
                    String [] token = temp.split("#");
                    String nowperson = token[0];
                    if(token[1].startsWith("weight"))
                    {
                        String nowlabel = personmap.get(nowperson);
                        String [] innertoken = token[1].split(":");
                        Double nowweight = Double.parseDouble(innertoken[1]);
                        Double nowvalue = new Double(0);
                        if(labelmap.containsKey(nowlabel))
                            nowvalue = labelmap.get(nowlabel) + nowweight;
                        else
                            nowvalue = nowweight;
                        labelmap.put(nowlabel, nowvalue);
                    }
                }
            }

            Iterator mapkeys = labelmap.keySet().iterator();
            Object nowmaxkey = mapkeys.next();
            Double nowmaxvalue = labelmap.get(nowmaxkey);
            while (mapkeys.hasNext())
            {
                Object tempkey = mapkeys.next();
                Double tempvalue = labelmap.get(tempkey);
                if(tempvalue > nowmaxvalue)
                {
                    nowmaxkey = tempkey;
                    nowmaxvalue = tempvalue;
                }
            }
            String maxlabel = nowmaxkey.toString();
            personmap.put(key.toString(), maxlabel);
            context.write(new Text(key), new Text(maxlabel + suffix));
        }
    }
    public static void main(String [] filepath)
            throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LabelPropagation");
        job.setJarByClass(LPAIterator.class);
        job.setMapperClass(LPAIterator.LPAIteratorMapper.class);
        job.setReducerClass(LPAIterator.LPAIteratorReducer.class);

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

class LPAGrouping
{
    private static int newlabelnumber = 1;

    public static class MySortType implements WritableComparable<MySortType>
    {
        private int first;
        private double second;

        public MySortType()
        {
            super();
            first = 0;
            second = 0;
        }

        public void setFirst(String value)
        {
            this.first = Integer.parseInt(value);
        }

        public void setSecond(String value)
        {
            this.second = Double.parseDouble(value);
        }

        public Integer getFirst()
        {
            return this.first;
        }

        public Double getSecond()
        {
            return this.second;
        }

        @Override
        public void write(DataOutput out) throws IOException
        {
            out.writeInt(this.first);
            out.writeDouble(this.second);
        }

        @Override
        public void readFields(DataInput in) throws IOException
        {
            this.first = in.readInt();
            this.second = in.readDouble();
        }

        @Override
        public int compareTo(MySortType val)
        {
            if(this.first == val.first)
            {
                if(this.second == val.second)
                    return 0;
                else if(this.second < val.second)
                    return 1;
                else
                    return -1;
            }
            else if(this.first < val.first)
                return -1;
            else
                return 1;
        }
    }

    public static class MySortComparator extends WritableComparator
    {
        public MySortComparator()
        {
            super(MySortType.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b)
        {
            MySortType p = (MySortType)a;
            MySortType q = (MySortType)b;
            int pfirst = p.getFirst();
            int qfirst = q.getFirst();
            if(pfirst == qfirst)
                return 0;
            else if(pfirst < qfirst)
                return -1;
            else
                return 1;
        }
    }

    public static class LPAGroupingMapper extends Mapper<LongWritable, Text, MySortType, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            String [] token = line.split("\t");
            String myValue = token[0];
            MySortType mysorttype = new MySortType();
            mysorttype.setFirst(token[1]);
            mysorttype.setSecond(token[2]);
            context.write(mysorttype, new Text(myValue));
        }
    }
    public static class LPAGroupingReducer extends Reducer<MySortType, Text, Text, Text>
    {
        @Override
        public void reduce(MySortType key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            String newlabel = String.valueOf(newlabelnumber);
//            String myValue = "";
//            boolean first = true;
//            for(Text value : values)
//            {
//                if(first)
//                    first = false;
//                else
//                    myValue += ", ";
//                myValue += value.toString();
//            }
//            context.write(new Text(newlabel), new Text(myValue));
            for(Text value : values)
            {
                String name = value.toString();
                String pagerank = String.valueOf(key.getSecond());
                context.write(new Text(name), new Text(newlabel + "\t" + pagerank));
            }
            newlabelnumber++;
        }
    }
    public static void main(String [] filepath)
            throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LPAGrouping");
        job.setJarByClass(LPAGrouping.class);
        job.setMapperClass(LPAGrouping.LPAGroupingMapper.class);
        job.setReducerClass(LPAGrouping.LPAGroupingReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(MySortType.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(MySortComparator.class);

        FileInputFormat.addInputPath(job, new Path(filepath[0]));
        FileOutputFormat.setOutputPath(job, new Path(filepath[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

public class LabelPropagation
{
    public static void main(String[] args)
            throws Exception
    {
        String [] filepath = {"", ""};

        filepath[0] = args[0] + "/PageRankOutcome";
        filepath[1] = args[1] + "/LPAFolder0";
        LPAInitiate.main(filepath);

        int times = Integer.parseInt(args[2]);
        for(int i = 0; i < times; i++)
        {
            filepath[0] = args[1] + "/LPAFolder" + String.valueOf(i);
            filepath[1] = args[1] + "/LPAFolder" + String.valueOf(i + 1);
            LPAIterator.main(filepath);
        }

        filepath[0] = args[1] + "/LPAFolder" + String.valueOf(times);
        filepath[1] = args[1] + "/LPAOutcome";
        LPAGrouping.main(filepath);
    }
}
