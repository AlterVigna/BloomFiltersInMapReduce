package it.unipi.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import it.unipi.hadoop.serializable.BloomFilterSerializable;
import it.unipi.hadoop.writable.BloomFilter;


/**
 * This class contains methods dedicated to the reducer of the first stage "Construction BloomFilters".
 * @author Davide
 *
 */
public class ConstructionReducer {


	public static class BloomFilterInsertionReducer extends Reducer<Text, BloomFilter, Text, Text> {
		
		

		private List<Integer> listNumElements;
		private List<Double> listProbElements;
		
		private BloomFilter bloomFilter;

		Text outputKey=new Text();
		Text outputValue=new Text();
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException
        {
			// Reading the values from the context.
			
			int n1 = context.getConfiguration().getInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"1",2000);
			double p1 = context.getConfiguration().getFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"1", Float.parseFloat("0.1"));
			
			int n2 = context.getConfiguration().getInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"2",6100);
			double p2 = context.getConfiguration().getFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"2", Float.parseFloat("0.1"));
			
			int n3 = context.getConfiguration().getInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"3",13400);
			double p3 = context.getConfiguration().getFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"3", Float.parseFloat("0.1"));
			
			int n4 = context.getConfiguration().getInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"4",41800);
			double p4 = context.getConfiguration().getFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"4", Float.parseFloat("0.01"));
			
			int n5 = context.getConfiguration().getInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"5",82000);
			double p5 = context.getConfiguration().getFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"5", Float.parseFloat("0.01"));
			
			int n6 = context.getConfiguration().getInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"6",204200);
			double p6 = context.getConfiguration().getFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"6", Float.parseFloat("0.001"));
			
			int n7 = context.getConfiguration().getInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"7",277300);
			double p7 = context.getConfiguration().getFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"7", Float.parseFloat("0.001"));
			
			int n8 = context.getConfiguration().getInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"8",294000);
			double p8 = context.getConfiguration().getFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"8", Float.parseFloat("0.001"));
			
			int n9 = context.getConfiguration().getInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"9",69100);
			double p9 = context.getConfiguration().getFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"9", Float.parseFloat("0.01"));
			
			int n10 = context.getConfiguration().getInt(GlobalConfig.NUMBER_OF_ELEMENT_IN_BLOOM_FILTER_+"10",10100);
			double p10 = context.getConfiguration().getFloat(GlobalConfig.PROBABILITY_FALSE_POSITIVE_+"10", Float.parseFloat("0.1"));
			
			// Save information in to arrayList because in this point you don't know what is the value received as key for the reducer.
			listNumElements=  new ArrayList<Integer>(Arrays.asList(n1, n2, n3,
					n4, n5, n6, n7, n8, n9, n10));
			listProbElements= new ArrayList<Double>(Arrays.asList(p1, p2, p3,
					p4, p5, p6, p7, p8, p9, p10));
        }
		
		@Override
		protected void reduce(Text key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {

			int filterIndex = Integer.parseInt(key.toString());	// Determining what filter is responsible for this reducer.
			
			// Instantiate new empty filter, depending on the index.
			bloomFilter= new BloomFilter(listNumElements.get(filterIndex-1), listProbElements.get(filterIndex-1));	
			
			for (final BloomFilter bf : values) {
				bloomFilter.or(bf);		// or bit a bit
			}
			
			outputValue.set(new BloomFilterSerializable(bloomFilter).serializeBase64BloomFilter());
			context.write(key, outputValue);

		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

		}
		
		
		
		
		
	}
	
	
}
