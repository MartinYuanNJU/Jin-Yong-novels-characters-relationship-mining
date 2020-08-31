import com.sun.deploy.cache.BaseLocalApplicationProperties;
import org.ansj.domain.Result;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.ansj.domain.Term;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.omg.PortableInterceptor.INACTIVE;

import java.io.*;
import java.net.URI;
import java.util.*;

public class preprocessing {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("namelistpath", "/user/2020st40/task/People_List_unique.txt");
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf,"PreProcessing");
        job.addCacheFile(new Path("/user/2020st40/task/People_List_unique.txt").toUri());
        job.setJarByClass(preprocessing.class);
        job.setMapperClass(preprocessing.PreProcessingMapper.class);
        job.setReducerClass(preprocessing.PreProcessingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class PreProcessingReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for(NullWritable value:values){
                context.write(key,NullWritable.get());
            }

        }
    }

    public static class PreProcessingMapper extends Mapper<Object, Text, Text, NullWritable> {

        private HashSet<String>allnames=new HashSet<String>();

        @Override
        public void setup(Context context) throws IOException {
            //create name dic
//            String nameListPath = context.getConfiguration().get("namelistpath");
//            //FileSystem fileSystem =FileSystem.get(context.getConfiguration());
//            BufferedReader reader = new BufferedReader(new FileReader(nameListPath));
//            String name;
//            while((name = reader.readLine()) != null){
//                DicLibrary.insert(DicLibrary.DEFAULT,name);
//            }
            //String nameListPath = context.getConfiguration().get("namelistpath");
            Configuration conf = context.getConfiguration();
            Path[] catchFiles = DistributedCache.getLocalCacheFiles(conf);
            String ipLocation = catchFiles[0].toString();

            BufferedReader reader = new BufferedReader(new FileReader(
                    ipLocation));

            String line;
            while ((line = reader.readLine()) != null) {
                allnames.add(line);
            }
            reader.close();
        }

        @Override
        public void map(Object key, Text value, Mapper<Object, Text, Text,  NullWritable>.Context context) throws IOException, InterruptedException {
//            String str = value.toString();
//            Result result = DicAnalysis.parse(str);
//            List<Term> terms = result.getTerms();
//            StringBuilder sb = new StringBuilder();
//            List<String> names=new ArrayList<String>();
//            for(Term term:terms){
//                if(term.getNatureStr().equals("userDefine")){
//                    names.add(term.toString());
//                }
//            }
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filename=fileSplit.getPath().getName();
            if(!filename.startsWith("金庸")) return;
            String str = value.toString();
            String []terms=str.split(" " );
            List<String> names=new ArrayList<String>();
            for(String term:terms){
                if(allnames.contains(term)){
                    names.add(term);
                }
            }
            if (names.size()==0) {return;}
            String res=org.apache.commons.lang.StringUtils.join(names.toArray(),' ');
            context.write(new Text(res), NullWritable.get());
        }
    }
}
