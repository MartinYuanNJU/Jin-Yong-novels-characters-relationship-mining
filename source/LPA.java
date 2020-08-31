import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.*;

class LPAInitiate
{
    public static class LPAInitiateMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            String [] token = line.split("\t");
            String myValue = token[1] + "\t" + key.toString();
            context.write(new Text(token[0]), new Text(myValue));
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
    public static class LPAIteratorMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            String [] token = line.split("\t");
            String LPAKey = token[0];
            if(token.length > 2)
            {
                String [] linkpeople = token[1].split(" ");
                for(String linkperson : linkpeople)
                {
                    String [] innersplit = linkperson.split(":");
                    context.write(new Text(innersplit[0]), new Text(LPAKey + ";" + token[2]));
                    context.write(new Text(LPAKey), new Text(linkperson));
                }
            }
        }
    }
    public static class LPAIteratorReducer extends Reducer<Text, Text, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            List<Text> sortedValues = new LinkedList<Text>();
            for(Text v : values) {
                sortedValues.add(new Text(v));
            }
            Collections.sort(sortedValues);

            Map<String, Double> label_weight_map = new HashMap<String, Double>();
            Iterator<Text> it = sortedValues.iterator();
            StringBuilder weightInfo = new StringBuilder();
            while(it.hasNext()) {
                String name_weight = it.next().toString();
                String name_label = "";
                if(it.hasNext())
                    name_label = it.next().toString();
                else {
                    System.err.println("######## missed name_label! ########" + name_weight);
                    System.exit(-1);
                }

                if(name_weight.indexOf(':') == -1) {
                    System.err.println("######## no ':' in name_weight! ########" + name_weight + name_label);
                    System.exit(-1);
                }
                if(name_label.indexOf(';') == -1) {
                    System.err.println("######## no ';' in name_label! ########" + name_weight + name_label);
                    System.err.println(sortedValues.toString());
                    System.exit(-1);
                }

                String[] sp0 = name_weight.split(":");
                String[] sp1 = name_label.split(";");
                if(!sp0[0].equals(sp1[0])) {
                    System.err.println("######## name error! ########" + name_weight + name_label);
                    System.exit(-1);
                }

                weightInfo.append(name_weight).append(" ");

                String label = sp1[1];
                double weight = Double.parseDouble(sp0[1]);
                Double weightSum = label_weight_map.get(label);
                if(weightSum != null) {
                    label_weight_map.put(label, weightSum + weight);
                }
                else {
                    label_weight_map.put(label, weight);
                }
            }

            double max = 0;
            String max_label = "";
            for(String k : label_weight_map.keySet()) {
                double temp = label_weight_map.get(k);
                if(temp > max) {
                    max = temp;
                    max_label = k;
                }
            }
            context.write(new Text(key), new Text(weightInfo + "\t" + max_label));
        }
    }
    public static void main(String [] filepath)
            throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LPA");
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

class LPASorting
{
    public static class LPASortingMapper extends Mapper<LongWritable, Text, ClassDegreeWritable, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            String [] token = line.split("\t");
            int class_ = Integer.parseInt(token[2]);
            int degree = token[1].split(" ").length;
            String myValue = token[0];
            context.write(new ClassDegreeWritable(class_, degree), new Text(myValue));
        }
    }
    public static class LPASortingReducer extends Reducer<ClassDegreeWritable, Text, Text, Text>
    {
        int class_index = -1;
        int prev_class = -1;
        @Override
        public void reduce(ClassDegreeWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            String class_degree = key.toString();
            int class_ = Integer.parseInt(class_degree.split("\t")[0]);
            if(class_ != prev_class) {
                prev_class = class_;
                class_index++;
            }
            key.setClass_(class_index);
            class_degree = key.toString();
            for(Text value : values)
            {
                String myKey = value.toString();
                context.write(new Text(myKey), new Text(class_degree));
            }
        }
    }
    public static class ClassDegreeWritable implements WritableComparable
    {
        int class_, degree;

        public ClassDegreeWritable()
        {
            super();
        }

        public ClassDegreeWritable(int class_, int degree)
        {
            super();
            this.class_ = class_;
            this.degree = degree;
        }

        public int getClass_()
        {
            return class_;
        }

        public int getDegree() {
            return degree;
        }

        @Override
        public void write(DataOutput out) throws IOException
        {
            out.writeInt(class_);
            out.writeInt(degree);
        }

        @Override
        public void readFields(DataInput in) throws IOException
        {
            this.class_ = in.readInt();
            this.degree = in.readInt();
        }

        @Override
        public int compareTo(Object val)
        {
            ClassDegreeWritable other = (ClassDegreeWritable)val;
            if(class_ < other.getClass_())
                return -1;
            else if(class_ > other.getClass_())
                return 1;
            else {
                if(degree > other.getDegree())
                    return -1;
                else if(degree < other.getDegree())
                    return 1;
                else
                    return 0;
            }
        }

        @Override
        public String toString()
        {
            return Integer.toString(class_);
        }

        public void setClass_(int class_) { this.class_ = class_; }
    }
    public static void main(String [] filepath)
            throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LPASorting");
        job.setJarByClass(LPASorting.class);
        job.setMapperClass(LPASorting.LPASortingMapper.class);
        job.setReducerClass(LPASorting.LPASortingReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(ClassDegreeWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(filepath[0]));
        FileOutputFormat.setOutputPath(job, new Path(filepath[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

public class LPA
{
    public static void main(String[] args)
            throws Exception
    {
        if(args.length != 3) {
            System.err.println("Usage: <in> <out> <times>");
            System.exit(-1);
        }
        int times = Integer.parseInt(args[2]); //LPA iterate times

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[1] + "/tempResult");
        fs.mkdirs(path);
        fs.close();

        String [] filepath = {"", ""};

        //pre processing, setting initial LPA label for each vertices
        filepath[0] = args[0];
        filepath[1] = args[1] + "/tempResult/Folder0";
        LPAInitiate.main(filepath);

        //LPA iteration
        for(int i = 0; i < times; i++)
        {
            filepath[0] = args[1] + "/tempResult/Folder" + String.valueOf(i);
            filepath[1] = args[1] + "/tempResult/Folder" + String.valueOf(i + 1);
            LPAIterator.main(filepath);
        }

        //sorting LPA labels
        filepath[0] = args[1] + "/tempResult/Folder" + String.valueOf(times);
        filepath[1] = args[1] + "/LPAOutcome";
        LPASorting.main(filepath);

        fs.delete(path, true);
        fs.close();
    }
}
