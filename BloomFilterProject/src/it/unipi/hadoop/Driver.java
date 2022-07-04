package it.unipi.hadoop;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import it.unipi.hadoop.writable.BloomFilter;

public class Driver {

	public static void main(final String[] args) throws Exception {
        
		if (args.length!=3) {
			System.err.println("Wrong number of parameter passed: <dataset-inputPath> <output-statsPath> <numberLinesPerSplit> ");
	        System.exit(1);
			return;
		}
		
		int N_LINES=0;
		try {
			N_LINES=Integer.parseInt(args[2]);
		}
		catch(Exception ex) {
			 System.err.println("<numberLinesPerSplit> is not a number");
			 System.exit(1);
			 return;
		}
		
		
		
		FileSystem fs = FileSystem.get(new Configuration());
        
		if (fs.exists(new Path(GlobalConfig.OUTPUT_SET_FILTERS_FOLDER))){
	            fs.delete(new Path(GlobalConfig.OUTPUT_SET_FILTERS_FOLDER), true);
	            System.out.println("Old output-BloomFilterSet folder delated!");
	    }
		//fs.mkdirs(new Path(GlobalConfig.OUTPUT_SET_FILTERS_FOLDER));
		if (fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]), true);
            System.out.println("Old output-"+args[1]+" folder delated!");
        }
		
		
		/* Stage nr. 1 - Costruction of bloom filters set */
		
		filtersCostruction(args[0],N_LINES);
		System.out.println("Filter costruction terminated!");
		
		/* Stage nr. 2 - Test FPR on each input rating */
		
		testFPR(args[0],args[1],N_LINES);
		System.out.println("Test stage terminated!");
      
		
    }
	
	
	public static void filtersCostruction(String inputPathFile,Integer N_LINES) throws IOException, ClassNotFoundException, InterruptedException {
		
		final Configuration conf = new Configuration();
		
		conf.setInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"1", 2000);
		conf.setFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"1", Float.parseFloat("0.1"));
		
		conf.setInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"2", 6100);
		conf.setFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"2", Float.parseFloat("0.1"));
		
		conf.setInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"3", 13400);
		conf.setFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"3", Float.parseFloat("0.1"));
		
		conf.setInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"4", 41800);
		conf.setFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"4", Float.parseFloat("0.01"));
		
		conf.setInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"5", 82000);
		conf.setFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"5", Float.parseFloat("0.01"));
		
		conf.setInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"6", 204200);
		conf.setFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"6", Float.parseFloat("0.001"));
		
		conf.setInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"7", 277300);
		conf.setFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"7", Float.parseFloat("0.001"));
		
		conf.setInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"8", 294000);
		conf.setFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"8", Float.parseFloat("0.001"));
		
		conf.setInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"9", 69100);
		conf.setFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"9", Float.parseFloat("0.01"));
		
		conf.setInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"10", 10100);
		conf.setFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"10", Float.parseFloat("0.1"));
		
		conf.setInt(NLineInputFormat.LINES_PER_MAP, N_LINES);
		
		
	 	Job job = new Job(conf, "BloomFiltersCostruction");
        
        job.setJarByClass(Driver.class);
        job.setMapperClass(ConstructionMapper.BloomFilterInsertionMapper.class);
        job.setReducerClass(ConstructionReducer.BloomFilterInsertionReducer.class);
        
        
        job.setInputFormatClass(NLineInputFormat.class);
       
        
        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BloomFilter.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1); // Decided to use single reducer

        FileInputFormat.addInputPath(job, new Path(inputPathFile));
        FileOutputFormat.setOutputPath(job, new Path(GlobalConfig.OUTPUT_SET_FILTERS_FOLDER));

        job.waitForCompletion(true);
       
		
	}
	
	private static void testFPR(String inputPath,String outputPath,Integer N_LINES) throws IOException, ClassNotFoundException, InterruptedException {
		
		final Configuration conf = new Configuration();
	 	
		conf.setInt(NLineInputFormat.LINES_PER_MAP, N_LINES);
		
		FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(new Path(GlobalConfig.OUTPUT_SET_FILTERS_FOLDER+"/part-r-00000"));
        
        //Classical input stream usage
        String fileContent= IOUtils.toString(inputStream, "UTF-8");
		conf.set(GlobalConfig.BLOOMFILTER_FILE_CONTENT, fileContent);
		
		Job job = new Job(conf, "TestFPR");
        
        job.setJarByClass(Driver.class);
        job.setMapperClass(TestMapper.ComputeFPRMapper.class);
        job.setReducerClass(TestReducer.ComputeFPRReducer.class);
        
      
        job.setInputFormatClass(NLineInputFormat.class);
       
        
        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
       
		
	}
	
	
}
