package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TestReducer {


	public static class ComputeFPRReducer extends Reducer<Text, Text, Text, Text> {
		
		Text outputValue=new Text();
		
		int counterTP;
		int counterFP;
		int counterTN;
		int counterFN;
		double fpr;
		
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException
        {
			counterTP=0;
			counterFP=0;
			counterTN=0;
			counterFN=0;
			fpr=0;
        }
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			counterTP=0;
			counterFP=0;
			counterTN=0;
			counterFN=0;
			fpr=0;
			
			for (final Text stats : values) {
				
				String concat=stats.toString();
				String[] splitted = concat.split("§§");
				counterTP+=Integer.parseInt(splitted[0]);
				counterFP+=Integer.parseInt(splitted[1]);
				counterTN+=Integer.parseInt(splitted[2]);
				counterFN+=Integer.parseInt(splitted[3]);
			}
			
			double counterFP_double=counterFP;
			double counterTN_double=counterTN;
			
			fpr= counterFP_double/(counterFP_double+counterTN_double);
			
			outputValue.set(concatStringOutput());
			context.write(key, outputValue);

		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

		}
		
		private String concatStringOutput() {
			return " TP : "+counterTP+ "\t"+ "FP : "+counterFP+"\t"+"TN: "+counterTN+"\t"+"FN: "+counterFN+" \t \t"+"FPR: "+fpr;
		}
		
		
		
	}
	
	
}
