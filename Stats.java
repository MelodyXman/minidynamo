package dynamo;

/*
 * Utility functions for testing
 */
import java.util.*;
import java.lang.Math;


public class Stats {
	private double total;
	private ArrayList<Double> values;
	private double mean;
	private double variance;
	
	public Stats() {
		this.total = 0.0;
		this.values = new ArrayList<Double>();
		this.mean = -1.0;			
		this.variance = -1.0;
	}
	
	public void add(double value) {
		this.values.add((double) value);
		this.total += (double) value;
		this.mean = -1.0;			
		this.variance = -1.0;		
	}
	
	public double mean() {
		if (this.mean == -1.0) {
			this.mean = this.total / (double) this.values.size();
		}
		return this.mean;
	}
	
	public double variance() {
		if (this.variance == -1.0) {
			mean = this.mean();
			this.variance = 0.0;
			for (double value : this.values) {
				double diffval = value - mean;
				this.variance += diffval * diffval;
			}
			this.variance = this.variance / (double) (this.values.size() - 1);
		}
		return this.variance;
	}
	
	public double stddev() {
		return Math.sqrt(this.variance());
	}
	
	public String random3Letters() {
		char[] letters = new char[3];
		Random r = new Random();
		letters[0] = (char) (65 + r.nextInt(26));
		letters[1] = (char) (65 + r.nextInt(26));
		letters[2] = (char) (65 + r.nextInt(26));
	    return new String(letters);
	}
}