package dynamo;

import java.util.*;

public class TestSimpleConsistentHashTable {
	public static void main(String[] args) {
		TestSimpleConsistentHashTable tc = new TestSimpleConsistentHashTable();
		List<String> nl = new ArrayList<>();
		nl.add("A");
		nl.add("B");
		nl.add("C");
		SimpleConsistentHashTable c = new SimpleConsistentHashTable(nl);
		Set<String> nodeset = new HashSet<>();
		int num_nodes = 50;
		String node;
		while(nodeset.size() < num_nodes){
			Stats tu = new Stats();
			node = tu.random3Letters();
			nodeset.add(node);
		}
		List<String> list = new ArrayList<>(nodeset);
		SimpleConsistentHashTable c2 = new SimpleConsistentHashTable(list);
		
		tc.testSmallExact(c);
		tc.testLarge(c2);
		tc.testDistribution(c2, nodeset);
		tc.testFailover(c2, nodeset);
	}
	
	public void setUp(){
		List<String> nl = new ArrayList<>();
		nl.add("A");
		nl.add("B");
		nl.add("C");
		int repeat = 2;
		SimpleConsistentHashTable c = new SimpleConsistentHashTable(nl);
		Set<String> nodeset = new HashSet<>();
		int num_nodes = 50;
		String node;
		while(nodeset.size() < num_nodes){
			Stats tu = new Stats();
			node = tu.random3Letters();
			nodeset.add(node);
		}
		List<String> list = new ArrayList<>(nodeset);
		SimpleConsistentHashTable c2 = new SimpleConsistentHashTable(list);
	}
	
	public void testSmallExact(SimpleConsistentHashTable c){
		String self = c.toString();
		String s = "[0d61f8370cad1d412f80b84d143e1257,C],"
		+ "[7fc56270e7a70fa81a5935b72eacbe29,A],"
		+ "[9d5ed678fe57bcca610140957afab571,B]";
		boolean ret = self.equals(s);
		System.out.println(ret);
		List<String> result = c.find_nodes("splurg", 2, null);
		System.out.println("result is " + result);	
		System.out.println("result should be {A, B}");
		System.out.println(c.avoided);
		System.out.println("avoided should be null");
		Set<String> set = new HashSet<>();
		set.add("A");
		List<String> result2 = c.find_nodes("splurg", 2, set);
		System.out.println("result is " + result2);	
		System.out.println("result should be {B, C}");
		System.out.println(c.avoided);
		System.out.println("avoided should be {A}");
		set.add("B");
		List<String> result3 = c.find_nodes("splurg", 2, set);
		System.out.println("result is " + result3);	
		System.out.println("result should be {C}");
		System.out.println(c.avoided);
		System.out.println("avoided should be {A, B}");
		set.add("C");
		List<String> result4 = c.find_nodes("splurg", 2, set);
		System.out.println("result is " + result4);	
		System.out.println("result should be {}");
		System.out.println(c.avoided);
		System.out.println("avoided should be {A, B, C}");
	}
	
	public void testLarge(SimpleConsistentHashTable c){
		List<String> l = c.find_nodes("splurg", 15, null);
		String s = l.get(0);
		System.out.println(l.size() == 15);
	}
	
	public void testDistribution(SimpleConsistentHashTable c, Set<String> nodeset){
		//generate a lot og hash values and see how even the distribution is
		int numkeys = 10000;
		List<String> nodecount = new ArrayList<>(nodeset);
		Map<String, Integer> map = new HashMap<>();
		for(int i=0; i<nodecount.size(); i++){
			map.put(nodecount.get(i), 0);
		}
		
		for(int i=0; i<numkeys; i++) {
			Stats t = new Stats();
			List<String> result = c.find_nodes(t.random3Letters(), 1, null);
			String node = result.get(0);
			if(!map.containsKey(node)){
				map.put(node, 0);
			}else{
				map.put(node, map.get(node) + 1);
			}
		}
		Stats st = new Stats();
		Collection<Integer> l = map.values();
		for(Integer i : l){
			st.add(i+0.0);
		}
		System.out.println("\n"+numkeys + " random hash keys assigned to "
				+ nodeset.size() + " nodes \n"
               +"are distributed across the nodes "
               +"\nwith a standard deviation of " + st.stddev() 
               +" (compared to a mean of "+ numkeys / nodeset.size()+")");
	}
	
	public void testFailover(SimpleConsistentHashTable c, Set<String> nodeset) {
		//For a given unavailable node, see what other nodes get new traffic
		String test_node = null;
		int total_transfers = 0;
		List<String> nodecount = new ArrayList<>(nodeset);
		
		Map<String, Integer> map = new HashMap<>();
		for(int i=0; i<nodecount.size(); i++){
			map.put(nodecount.get(i), 0);
		}
		int numkeys = 1000;
		for(int i=0; i<numkeys; i++) {
			Stats st = new Stats();
			String key = st.random3Letters();
			List<String> node_pair = c.find_nodes(key, 2, null);
			
			if(test_node == null){
				test_node = node_pair.get(0);
			}
			if(test_node.equals(node_pair.get(0))){
				String next_node = node_pair.get(1);
				if(map.containsKey(next_node)){
					map.put(next_node, map.get(next_node)+1);
				}else{
					map.put(next_node, 1);
				}
				total_transfers += 1;
			}
		}
		for(int i=0; i<nodecount.size(); i++){
			String node = nodecount.get(i);
			if(map.containsKey(node) && map.get(node)>0){
				System.out.println("\nNode "+ node + " gets " + map.get(node) +
						" of " + total_transfers +"("+
						100 * map.get(node)/total_transfers +"%)"
						+ " transfers.");
			}
		}
	}
}
