package dynamo;

import java.util.*;

/*public class VectorClock {

	TreeMap<String, Integer> clock;
	
	public static VectorClock converge(ArrayList<VectorClock> vcs)
	{
		return null;
	}
	
	public static HashSet<ResponseTuples> coalesce2(HashSet<ResponseTuples> vcs)
	{
		return null;
	}
	
	public void update(String name, int seqno)
	{
		
	}
	
	public String toString()
	{
		return null;
	}
}*/

import java.util.*;
public class VectorClock {
	
	TreeMap<String, Integer> clock;
	
	public VectorClock() {
		this.clock = new TreeMap<String, Integer>();	// node => counter
	}
	
	public void update(String node, int counter) {
		/* Add a new node:counter value to a VectorClock.*/
		if (this.clock.containsKey(node) && counter <= this.clock.get(node)) {
			System.out.println("Node " + node + " has gone backwards from " + this.clock.get(node) + " to " + counter);			
			return;
		}
		
		//else update
		this.clock.put(node, counter);
		return;
	}
	
	//public TreeMap<String, Integer> getClock() {
	//	return this.clock;
	//}
	
	public VectorClock deepCopy() {
		VectorClock copy = new VectorClock();
		for (String node : clock.keySet()) {
			copy.update(node, clock.get(node));
		}
		
		return copy;
	}
	
	public String toString() {
		StringBuffer sbuffer = new StringBuffer();
		
		sbuffer.append('{');
		
		for(String node : clock.keySet()) {
			sbuffer.append(node).append(':').append(clock.get(node)).append(',');
		}
		if(this.clock.size() > 0) {
			sbuffer.deleteCharAt(sbuffer.length()-1);
		}
		
		sbuffer.append('}');
		return sbuffer.toString();
	}
	
	public int size()
	{
		return clock.size(); 
	}
	
	public Set<String> getNodes() {
		return clock.keySet();
	}
	
	
	public int getTimeStamp(String node)
	{
		int ts = -1;
		
		if(clock.containsKey(node))
		{
			ts = clock.get(node);
		}
		
		return ts;
	}
	
	public boolean containNode(String node)
	{
		if(clock.keySet().contains(node))
		{
			return true;
		}
		
		return false;
	}
	
	/* Comparison operations. Vector clocks are partially ordered, but not totally ordered. */
	public boolean eq(VectorClock other) {
		
		if(size() != other.size())
		{
			return false;
		}
		
		for(String node : clock.keySet())
		{
			if(clock.get(node) != other.getTimeStamp(node))
			{
				return false;
			}
		}
		
		return true;
	}
	
	public boolean ne(VectorClock other) {
		return !this.eq(other);
	}
	
	public boolean lt(VectorClock other) {
		for (String node : clock.keySet()) {
			if (!other.containNode(node)) {
				return false;
			}
			if (clock.get(node) > other.getTimeStamp(node)) {
				return false;
			}
		}
		
		return true;
	}
	
	public boolean le(VectorClock other) {
		return this.eq(other) || this.lt(other);
	}
	
	public boolean gt(VectorClock other) {
		return other.lt(this);
	}
	
	public boolean ge(VectorClock other) {
		return this.eq(other) || this.gt(other);
	}
	
	/* 
	 * Coalesce a container of VectorClock objects.
	 * 
	 * The result is a list of VectorClocks; each input VectorClock is a direct
     * ancestor of one of the results, and no result entry is a direct ancestor
     * of any other result entry.
    */
	public static ArrayList<VectorClock> coalesce (ArrayList<VectorClock> vcs) {
		ArrayList<VectorClock> results = new ArrayList<VectorClock>();
		for (VectorClock vc : vcs) {
			/* See if this vector-clock subsumes or is subsumed by anything already present */
			boolean subsumed = false;
			for (int i = 0; i < results.size(); i++) {
				VectorClock result = results.get(i);
				if (vc.le(result)) {	// subsumed by existing answer
					subsumed = true;
					break;
				}
				if (result.lt(vc)) {	// subsumes existing answer so replace it
					results.set(i, vc.deepCopy());
					subsumed = true;
					break;
				}
			}
			if (!subsumed) {
				results.add(vc.deepCopy());
			}
		}
		return results;
	}
	
	/* 
	 * Coalesce a container of (Object, VectorClock) tuples.
	 * 
	 * The result is a list of (Object, VectorClock) tuples; each input VectorClock is a direct
     * ancestor of one of the results, and no result entry is a direct ancestor
     * of any other result entry.
    */
	public static HashSet<ResponseTuples> coalesce2 (HashSet<ResponseTuples> vcs) {
		HashSet<ResponseTuples> results = new HashSet<ResponseTuples>();
		for (ResponseTuples tuple : vcs) {
			
			VectorClock vc = tuple.metadata;
			if(vc == null) //to be done ???
			{
				//log
			}
			
			/* See if this vector-clock subsumes or is subsumed by anything already present */
			boolean subsumed = false;
			for (ResponseTuples result:results) {
				
				VectorClock resultvc = result.metadata;
				if (vc.le(resultvc)) {	// subsumed by existing answer
					subsumed = true;
					break;
				}
				if (resultvc.lt(vc)) {	// subsumes existing answer so replace it
					
					results.remove(result);
					results.add(tuple);
					subsumed = true;
					break;
				}
			}
			
			if (!subsumed) {
				results.add(tuple);
			}
		}
		
		return results;
	}
	
	public static VectorClock converge(ArrayList<VectorClock> vcs)
	{
		VectorClock result = new VectorClock();
		
		for(VectorClock vc:vcs) {
			if(vc.size() == 0)
			{
				continue;
			}
			
			Set<String> nodeSet = vc.getNodes();
			for(String node:nodeSet) {
				
				int timestamp = vc.getTimeStamp(node);				
				if(result.getTimeStamp(node) < timestamp)
				{
					result.update(node, timestamp);
				}
			}
		}
		
		return result;
	}
}