package it.unipi.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.unipi.hadoop.serializable.BloomFilterSerializable;
import it.unipi.hadoop.writable.BloomFilter;

public class TestMapper {

	public static class ComputeFPRMapper extends Mapper<Object, Text, Text, Text> {

		private final Text key = new Text();
		private final Text value= new Text();
		
		private List<BloomFilter> bloomFiltersSet;

		List<Integer> counterListTP=null;
		List<Integer> counterListFP=null;
		List<Integer> counterListTN=null;
		List<Integer> counterListFN=null;
		
		private String ratingString;
		private String idRating;
		private double avgRating;
		private int avgRounded;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			
			// Read from config the content of Bloom Filters set, reloaded from HDFS.
			String fileContent = context.getConfiguration().get(GlobalConfig.BLOOMFILTER_FILE_CONTENT);
			
			// Initialize all the counters to 0.
			counterListTP=Arrays.asList(0,0,0,0,0,0,0,0,0,0);
			counterListFP=Arrays.asList(0,0,0,0,0,0,0,0,0,0);
			counterListTN=Arrays.asList(0,0,0,0,0,0,0,0,0,0);
			counterListFN=Arrays.asList(0,0,0,0,0,0,0,0,0,0); // Expected to be all 0.
			
			bloomFiltersSet= loadBloomFiltersFromHDFS(fileContent);

		}

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			parseInput(value.toString());
			avgRounded = (int) Math.round(avgRating);
			
			for (int i = 0; i < bloomFiltersSet.size(); i++) {
				
				BloomFilter bf = bloomFiltersSet.get(i);
				
				if (i==(avgRounded-1)) { // This is the case when the filter should say information present because insert in previous stage.
					counterListTP.set(i, counterListTP.get(i)+1);
					if (!bf.containsKey(idRating)) {
						counterListFN.set(i, counterListFN.get(i)+1);
					};
				}
				else {	// This is the case when the filter should say information NOT present because it is NOT insert in previous stage.
					counterListTN.set(i, counterListTN.get(i)+1);
					if (bf.containsKey(idRating)) {
						counterListFP.set(i, counterListFP.get(i)+1);
					}
				}
			}
			
// 			De-comment if u want to test a dataset never seen before		
			
//			for (int i = 0; i < bloomFiltersSet.size(); i++) {
//				
//				BloomFilter bf = bloomFiltersSet.get(i);
//				
//				// Used for dataset never seen before
//				counterListTN.set(i, counterListTN.get(i)+1);
//				if (bf.containsKey(idRating)) {
//					counterListFP.set(i, counterListFP.get(i)+1);
//				}
//			}
			
			
			
		}

		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			for (int i = 0; i < bloomFiltersSet.size(); i++) {
				key.set((i+1)+"");
				String concatenation=counterListTP.get(i)+"§§"+
									 counterListFP.get(i)+"§§"+
									 counterListTN.get(i)+"§§"+
									 counterListFN.get(i);
				value.set(concatenation);
				context.write(key, value);	// Emit as a key the rounded ranking value and as value a concatenation of counters information.
			}

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

		
		
		/**
		 * Utility method to load in one shot the entire set of bloomfilters read from HDFS before.
		 * @param loadedFileContent
		 * @return
		 * @throws IOException
		 */
		private List<BloomFilter> loadBloomFiltersFromHDFS(String loadedFileContent) throws IOException {
			
			String[] rowSplitted = loadedFileContent.split("\\r?\\n|\\r");
			
			BloomFilter bfArray[]= new BloomFilter[10];
			
			for (int i = 0; i < rowSplitted.length; i++) {
				
				 String data = rowSplitted[i];
		    	 String[] splitted = data.split("\t");
		    	  
		    	 String vote=splitted[0];
		    	 int voteInt = Integer.parseInt(vote);
		    	  
		    	 String base64= splitted[1];
		    	 bfArray[voteInt-1]= BloomFilterSerializable.deserializeBase64BloomFilter(base64);
			}
			
			return Arrays.asList(bfArray);
		}
		
		
		

	}

}
