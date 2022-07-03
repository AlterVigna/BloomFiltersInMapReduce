package it.unipi.hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.Math;
import java.util.BitSet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;

public class BloomFilter implements Writable , Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6192466868304493265L;
	
	private int n;		// Number of elements "keys" designed for the filter.
	private double p;	// False positive rate, probability between 0 and 1.
	private int m;		// Number of bits in the bit-vector.
	private int k;		// Number of hash functions.

	private BitSet bits;	// The bit-vector

	private Hash murmurInstance;	// Hash family function used to hash keys.
	
	// bit mask to do operation on bits.
	private static final byte[] bitvalues = new byte[] { (byte) 0x01, (byte) 0x02, (byte) 0x04, (byte) 0x08,
			(byte) 0x10, (byte) 0x20, (byte) 0x40, (byte) 0x80 };

	public BloomFilter() {
		murmurInstance = MurmurHash.getInstance();
	}

	public BloomFilter(int n, double p) {

		this.n = n;	// Saved even if never used.
		this.p = p; // Saved even if ever used.
		this.m = calculateM(n, p);
		this.k = calculateK(m, n);

		bits = new BitSet(m);
		murmurInstance = MurmurHash.getInstance();
	}

	private int calculateM(int n, double p) {
		double m = (-n * Math.log(p)) / Math.pow(Math.log(2), 2);
		return (int) Math.ceil(m);
	}

	private int calculateK(int m, int n) {
		double k = (m / n) * Math.log(2);
		return (int) Math.ceil(k);
	}

	public boolean containsKey(String x) {
		for (int n = 0; n < k; n++) {
			long h = murmurInstance.hash(x.getBytes(), (n + 1));
			int index = (int) (h % m);
			index = index < 0 ? -index : index;
			if (!bits.get(index))
				return false;
		}
		return true;
	}

	public void addKey(String x) {
		for (int n = 0; n < k; n++) {
			long h = murmurInstance.hash(x.getBytes(), (n + 1));
			int index = (int) (h % m);
			index = index < 0 ? -index : index;
			bits.set(index);
		}
	}


	public void or(BloomFilter bf) {

		for (int i = 0; i < this.bits.size(); i++) {
			this.bits.set(i, this.getBits().get(i) || bf.getBits().get(i));
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		n = in.readInt();
		p = in.readDouble();
		m = in.readInt();
		k = in.readInt();
		bits = new BitSet(m);

		byte[] bytes = new byte[getNBytes()];
		in.readFully(bytes);
		for (int i = 0, byteIndex = 0, bitIndex = 0; i < m; i++, bitIndex++) {
			if (bitIndex == 8) {
				bitIndex = 0;
				byteIndex++;
			}
			if ((bytes[byteIndex] & bitvalues[bitIndex]) != 0) {
				bits.set(i);
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(n);
		out.writeDouble(p);
		out.writeInt(m);
		out.writeInt(k);

		// out.write(bits.toByteArray());

		byte[] bytes = new byte[getNBytes()];
		for (int i = 0, byteIndex = 0, bitIndex = 0; i < m; i++, bitIndex++) {
			if (bitIndex == 8) {
				bitIndex = 0;
				byteIndex++;
			}
			if (bitIndex == 0) {
				bytes[byteIndex] = 0;
			}
			if (bits.get(i)) {
				bytes[byteIndex] |= bitvalues[bitIndex];
			}
		}
		out.write(bytes);
	}
	
	// Return the number of byte the cast in byte of m (number of bits).
	private int getNBytes() {
		return (m + 7) / 8;
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
	
}
