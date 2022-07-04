package it.unipi.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.unipi.hadoop.writable.BloomFilter;

/**
 * This class contains methods dedicated to the mappers of the first stage "Construction BloomFilters".
 * 
 * @author Davide
 *
 */
public class ConstructionMapper {

	public static class BloomFilterInsertionMapper extends Mapper<Object, Text, Text, BloomFilter> {

		private final Text key = new Text();
		private List<BloomFilter> bloomFiltersSet;

		// Variable declared globally to be reused during execution.
		private String ratingString;
		private String idRating;
		private double avgRating;
		private int avgRounded;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			
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
			
			BloomFilter bloomFilter1 = new BloomFilter(n1, p1);
			BloomFilter bloomFilter2 = new BloomFilter(n2, p2);
			BloomFilter bloomFilter3 = new BloomFilter(n3, p3);
			BloomFilter bloomFilter4 = new BloomFilter(n4, p4);
			BloomFilter bloomFilter5 = new BloomFilter(n5, p5);
			BloomFilter bloomFilter6 = new BloomFilter(n6, p6);
			BloomFilter bloomFilter7 = new BloomFilter(n7, p7);
			BloomFilter bloomFilter8 = new BloomFilter(n8, p8);
			BloomFilter bloomFilter9 = new BloomFilter(n9, p9);
			BloomFilter bloomFilter10 = new BloomFilter(n10, p10);

			bloomFiltersSet = new ArrayList<BloomFilter>(Arrays.asList(bloomFilter1, bloomFilter2, bloomFilter3,
					bloomFilter4, bloomFilter5, bloomFilter6, bloomFilter7, bloomFilter8, bloomFilter9, bloomFilter10));
		}

		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {

			
			parseInput(value.toString());	
			
			avgRounded = (int) Math.round(avgRating);
			BloomFilter bloomFilter = bloomFiltersSet.get(avgRounded-1);
			bloomFilter.addKey(idRating);
			
		}
		
		
		/**
		 * Ad Hoc function for managing the raw input.
		 * @param rawInput
		 */
		private void parseInput(String rawInput) {
			
			final StringTokenizer itr = new StringTokenizer(rawInput);
			
			int i = 0;
			while (itr.hasMoreTokens()) {
				ratingString = itr.nextToken();
				if (i == 0) {	// The first information represents the id of the rated film
					idRating = ratingString;
				}
				
				if (i == 1) {	// The second information represents the avgRatingValue in double.
					avgRating = Double.parseDouble(ratingString);
					break;
				}
				// The third information represents the numOfVotes but it is not interested.
				i++;
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			int vote=1;
			for (Iterator<BloomFilter> iterator = bloomFiltersSet.iterator(); iterator.hasNext();vote++) {
				BloomFilter bloomFilter = (BloomFilter) iterator.next();
				
				key.set(vote+"");
				context.write(key, bloomFilter); // Emit each rounded ranking value as key and a bloom filter as a value.
			}

		}

	}

}
