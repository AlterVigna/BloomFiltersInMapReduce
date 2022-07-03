package it.unipi.hadoop.serializable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Base64;
import java.util.BitSet;

import it.unipi.hadoop.writable.BloomFilter;


/**
 * Class used just to save in a compact way on HDFS a bloom filter. 
 * 
 * @author d.vigna
 *
 */
public class BloomFilterSerializable implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1314364007695564740L;
	
	
	private int n;
	private double p;
	private int m;
	private int k;
	private BitSet bits;
	
	
	public BloomFilterSerializable(BloomFilter bf) {
		this.n=bf.getN();
		this.p=bf.getP();
		this.m=bf.getM();
		this.k=bf.getK();
		this.bits=bf.getBits();
	}
	
	
	public BloomFilterSerializable(int n,double p,int m,int k, BitSet bits ) {
		this.n=n;
		this.p=p;
		this.m=m;
		this.k=k;
		this.bits=bits;
	}
	
	

	public int getN() {
		return n;
	}

	public void setN(int n) {
		this.n = n;
	}

	public double getP() {
		return p;
	}

	public void setP(double p) {
		this.p = p;
	}

	public int getM() {
		return m;
	}

	public void setM(int m) {
		this.m = m;
	}

	public int getK() {
		return k;
	}

	public void setK(int k) {
		this.k = k;
	}

	public BitSet getBits() {
		return bits;
	}

	public void setBits(BitSet bits) {
		this.bits = bits;
	}
	
	
	/**
	 * Custom Base64 serialization in order to save memory disk occupation.
	 * 
	 * @param bloomFilter
	 * @return
	 */
	public String serializeBase64BloomFilter() {
		
		//BloomFilterSerializable bfs= new BloomFilterSerializable(bloomFilter.getN(), bloomFilter.getP(), bloomFilter.getM(), bloomFilter.getK(), bloomFilter.getBits());
		
		String serialization="";
		try {
	    	 ByteArrayOutputStream baos = new ByteArrayOutputStream();
		     ObjectOutputStream oos = new ObjectOutputStream( baos );
		     oos.writeObject( this );
		     oos.close();
		     serialization=Base64.getEncoder().encodeToString(baos.toByteArray()); 
	     }
	     catch(Exception ex) { // In case of error print the stack trace into the variable.
	    	 StringWriter sw = new StringWriter(); 
	    	 PrintWriter pw = new PrintWriter(sw);
	    	 ex.printStackTrace(pw);
	    	 serialization=sw.toString(); 
	     }
		return serialization;
	}
	
	public static BloomFilter deserializeBase64BloomFilter(String base64) {
		
		BloomFilter bf= new BloomFilter();
		
	  	  byte [] data2 = Base64.getDecoder().decode( base64 );
	  	  ObjectInputStream ois;
	      try {
	    	  ois = new ObjectInputStream( new ByteArrayInputStream(  data2 ) );
	    	  BloomFilterSerializable bfs  = (BloomFilterSerializable) ois.readObject();
	    	  bf.setN(bfs.getN());
	    	  bf.setP(bfs.getP());
	    	  bf.setM(bfs.getM());
	    	  bf.setK(bfs.getK());
	    	  bf.setBits(bfs.getBits());
	    	  ois.close();
	      } catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return null;
	      } catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
	      }
		
		return bf;
		
	}
	
	
	
	
	
	
}
