package it.unipi.hadoop.test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import it.unipi.hadoop.serializable.BloomFilterSerializable;
import it.unipi.hadoop.writable.BloomFilter;


/**
 * Class used to make tests.
 * @author Davide
 *
 */
public class Main {
	
	
	public static void main (String[] args)  {
		
		BloomFilter bloomFilter= new BloomFilter(1000, 0.01);
		
	    String idFilm="";
		double avgRating;
		int avgRounded;

		List<BloomFilter> listaBloomFilter= new ArrayList<BloomFilter>();
		List<Integer> listaPresentiSicuri=Arrays.asList(0,0,0,0,0,0,0,0,0,0);
		List<Integer> listaBFFalsePositive=Arrays.asList(0,0,0,0,0,0,0,0,0,0);
		List<Integer> listaBFTrueNegative=Arrays.asList(0,0,0,0,0,0,0,0,0,0);
		
		List<Integer> listaBFFalseNegative=Arrays.asList(0,0,0,0,0,0,0,0,0,0);
		
		 BloomFilter nuovo = new BloomFilter(13400, 0.05);
		
		BloomFilter bfArray[]= new BloomFilter[10];
		
		System.getProperty("user.dir");
		  try {
			   
		      File myObj = new File("outputTEST1");
		      Scanner myReader = new Scanner(myObj);
		     
		      //int dato=0;
		      while (myReader.hasNextLine()) {
		    	  String data = myReader.nextLine();
		    	  String[] splitted = data.split("\t");
		    	  
		    	  String voto=splitted[0];
		    	  int votoInt = Integer.parseInt(voto);
		    	  
		    	  String testoLungo= splitted[1];
		    	  byte [] data2 = Base64.getDecoder().decode( testoLungo );
		 	     ObjectInputStream ois;
		 		try {
		 			ois = new ObjectInputStream( new ByteArrayInputStream(  data2 ) );
		 	     BloomFilterSerializable bloomFilter3  = (BloomFilterSerializable) ois.readObject();
		 	     BloomFilter bfNecessario= new BloomFilter(bloomFilter3.getN(), bloomFilter3.getP());
		 	     
		 	    bfNecessario.setBits(bloomFilter3.getBits());
		 	    bfArray[votoInt-1]=bfNecessario;
		 	     ois.close();
		 		} catch (IOException e1) {
		 			// TODO Auto-generated catch block
		 			e1.printStackTrace();
		 		} catch (ClassNotFoundException e) {
		 			// TODO Auto-generated catch block
		 			e.printStackTrace();
		 		}
		      }
		      myReader.close();
		      
		      listaBloomFilter= Arrays.asList(bfArray);
		    } catch (FileNotFoundException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		    }
		  
		  
		  try {
			  
		      File myObj = new File("data.tsv");
		      Scanner myReader = new Scanner(myObj);
		      
		      int totaleRighe=0;
		      while (myReader.hasNextLine()) {
		    	  totaleRighe++;
		    	  String data  = myReader.nextLine();
		    	  System.out.println(data);
		    	  String[] splitted = data.split("\t");
		    	  
		    	  String id= splitted[0];
		    	  String ratingString=splitted[1];
		    	  
		    	  avgRating = Double.parseDouble(ratingString);
		    	  avgRounded = (int) Math.round(avgRating);
		    	  
		    	  
		    	  // QUA CALCOLARE PER CIASCUN FILTRO CONSIDERANDO CHE I TRUE NEGATIVE AUMENTANO
		    	  int i=0;
		    	  for (Iterator iterator = listaBloomFilter.iterator(); iterator.hasNext();i++) {
		    		  BloomFilter bf = (BloomFilter) iterator.next();
		    		 
		    		  avgRating = Double.parseDouble(ratingString);
			    	  avgRounded = (int) Math.round(avgRating);
		    		  
			    	  if (i == (avgRounded-1)) {
			    		 
			    		  if (bf.containsKey(id)) {
			    			  listaPresentiSicuri.set(i, listaPresentiSicuri.get(i)+1);
			    		  }
			    		  else {
			    			  listaBFFalseNegative.set(i, listaBFFalseNegative.get(i)+1);
			    		  }
			    		  
			    		  continue;
			    	  }
			    	  
			    	  if (i != (avgRounded-1)) {
			    		  listaBFTrueNegative.set(i, listaBFTrueNegative.get(i)+1);
			    	  }
			    	  
			    	  if (bf.containsKey(id)) {
			    		  listaBFFalsePositive.set(i, listaBFFalsePositive.get(i)+1);
			    	  }
 
		    		  
				}
		    	  
		    	  
				/*
				 * bloomFilter=listaBloomFilter.get(avgRounded-1); if
				 * (!bloomFilter.containsKey(id)) { listaBFTrueNegative.set(avgRounded-1,
				 * listaBFTrueNegative.get(avgRounded-1)+1); };
				 */
		    	  
		    	  
		      }
		      myReader.close();
		      
		  } 
		  catch (FileNotFoundException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		   }
		  
	}
	
}
