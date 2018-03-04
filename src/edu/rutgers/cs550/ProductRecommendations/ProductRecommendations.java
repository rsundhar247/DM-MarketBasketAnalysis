package edu.rutgers.cs550.ProductRecommendations;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ProductRecommendations extends Configured implements Tool {

	public static final String CLASSNAME = "ProductRecommendations";
	

	public static void main(String[] args) {
		String METHODNAME = "main()";
		
		System.out.println(CLASSNAME + " : "+ METHODNAME + " " + "Hello you are in main() of ProductRecommendations !!!");
		
		try {
			ToolRunner.run(new Configuration(), new ProductRecommendations(), args);
			calculateSupport(args);
		} catch (Exception e) {
			System.out.println(CLASSNAME + " : "+ METHODNAME + " " + "Exception occured in main() -> ToolRunner :: " + e);
			e.printStackTrace();
		}
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "SocialNetworking");
		job.setJarByClass(ProductRecommendations.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce1.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		
		
		@SuppressWarnings("deprecation")
		Job job1 = new Job(getConf(), "SocialNetworking");
		job1.setJarByClass(ProductRecommendations.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setMapperClass(Map2.class);
		job1.setReducerClass(Reduce2.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		job1.waitForCompletion(true);
		
		return 0;
	}
	
	public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			final IntWritable ONE = new IntWritable(1);
			Text word = new Text();
			
			for (String token: value.toString().split(" ")) {
	            word.set(token);
	            context.write(word, ONE);
	         }
			
		}
	}

	public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
	      
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if(sum >= 100)
				context.write(key, new IntWritable(sum));			
		}
	}
	
	public static class Map2 extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			final IntWritable ONE = new IntWritable(1);
			Text word = new Text();
			
			String opFileLastPass = context.getConfiguration().get("fs.default.name") + "/home/cloudera/workspace/ProductRecommendations/OnePair/part-r-00000";
			Path pt=new Path(opFileLastPass);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader fis=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String currLine = null;
			HashMap<String, Integer> freqItemsMap = new HashMap<String, Integer>();
			while ((currLine = fis.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(currLine, "\t");
				freqItemsMap.put(st.nextToken(), 0);
			}
			
			
			List<String> inputList = new ArrayList<String>(); 
			List<String> freqList = new ArrayList<String>(); 
			for (String token: value.toString().split(" ")) {
				inputList.add(token);
	         }
			
			for(int i = 0; i<inputList.size(); i++){
				if(freqItemsMap.containsKey(inputList.get(i))){
					freqList.add(inputList.get(i));
				}
			}
			
			for(int i = 0; i<freqList.size()-1; i++){
				for(int j = i+1; j<freqList.size(); j++){
					if(freqList.get(i).compareToIgnoreCase(freqList.get(j)) > 0) {
						word.set(freqList.get(i) + "," + freqList.get(j));
					} else {
						word.set(freqList.get(j) + "," + freqList.get(i));
					}
					context.write(word, ONE);
				}
			}
		}
	}

	public static class Reduce2 extends Reducer<Text, IntWritable, Text, IntWritable> {
	      
		public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable val : value) {
				sum += val.get();
			}
			if(sum >= 100)
				context.write(key, new IntWritable(sum));
		}
	}
	
	
	public static void calculateSupport(String[] args) throws IOException{
		
		System.out.println("calculateSupport");
			
		String opFileLastPass = "file:////home/cloudera/workspace/ProductRecommendations/OnePair/part-r-00000";
		Path pt = new Path(opFileLastPass);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String currLine = null;
		HashMap<String, Long> freqItemsMap = new HashMap<String, Long>();
		while ((currLine = fis.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(currLine, "\t");
			freqItemsMap.put(st.nextToken(), Long.parseLong(st.nextToken()));
		}
		
		
		String opFileLastPass1 = "file:////home/cloudera/workspace/ProductRecommendations/TwoPair/part-r-00000";
		Path pt1 = new Path(opFileLastPass1);
        FileSystem fs1 = FileSystem.get(new Configuration());
        BufferedReader fis1 = new BufferedReader(new InputStreamReader(fs1.open(pt1)));
		HashMap<String, Long> freqItemsMap1 = new HashMap<String, Long>();
		while ((currLine = fis1.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(currLine, "\t");
			freqItemsMap1.put(st.nextToken(), Long.parseLong(st.nextToken()));
		}
		
		HashMap<String, Float> finalSupportMap = new HashMap<String, Float>();
		
		for (Map.Entry<String, Long> entry : freqItemsMap1.entrySet()) {
			
			String mapKey = entry.getKey();
			Long mapValue = entry.getValue();
			String[] mapIndividualKey = mapKey.split(",");
			
			float support = (float) mapValue / freqItemsMap.get(mapIndividualKey[0]);
			finalSupportMap.put(mapIndividualKey[0]+"->"+mapIndividualKey[1], support);
			System.out.println(mapIndividualKey[1]+"->"+mapIndividualKey[0] + "    "+ mapValue + "    " + freqItemsMap.get(mapIndividualKey[1]));
			
			support = (float) mapValue / freqItemsMap.get(mapIndividualKey[1]);
			finalSupportMap.put(mapIndividualKey[1]+"->"+mapIndividualKey[0], support);
		}
		
		Map<String, Float> sortedMap = sortByValue(finalSupportMap);  
		System.out.println("Sorted Map :::: " + sortedMap);
		
	}
	
	private static Map<String, Float> sortByValue(Map<String, Float> unsortMap) {

        List<Map.Entry<String, Float>> list = new LinkedList<Map.Entry<String, Float>>(unsortMap.entrySet());

        Collections.sort(list, new Comparator<Map.Entry<String, Float>>() {
            public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2) {
            	if(o1.getValue().compareTo(o2.getValue()) > 0)
                	return -1;
                else if(o1.getValue().compareTo(o2.getValue()) < 0)
                	return 1;
                else if(o1.getKey().compareTo(o2.getKey()) > 0)
                	return 1;
                else
                	return -1;
            }
        });

        Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
        for (Map.Entry<String, Float> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
}