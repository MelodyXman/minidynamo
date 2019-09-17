package dynamo;

import java.util.*;
public class TestConsistentHashTable {

	static int NODE_REPEAT = 10;
	public static void main(String[] args) {
		TestConsistentHashTable tc = new TestConsistentHashTable();
		List<String> nl = new ArrayList<>();
		nl.add("A");
		nl.add("B");
		nl.add("C");
		int repeat = 2;
		ConsistentHashTable c = new ConsistentHashTable(nl, repeat);
		Set<String> nodeset = new HashSet<>();
		int num_nodes = 50;
		String node;
		while(nodeset.size() < num_nodes){
			Stats tu = new Stats();
			node = tu.random3Letters();
			nodeset.add(node);
		}
		List<String> list = new ArrayList<>(nodeset);
		ConsistentHashTable c2 = new ConsistentHashTable(list, NODE_REPEAT);
		
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
		//System.out.println(nl.size());
		ConsistentHashTable c = new ConsistentHashTable(nl, repeat);
		//System.out.println(c.nodelist.size());
		Set<String> nodeset = new HashSet<>();
		int num_nodes = 50;
		String node;
		while(nodeset.size() < num_nodes){
			Stats tu = new Stats();
			node = tu.random3Letters();
			nodeset.add(node);
		}
		List<String> list = new ArrayList<>(nodeset);
		ConsistentHashTable c2 = new ConsistentHashTable(list, NODE_REPEAT);
		
	}
	
	public void testSmallExact(ConsistentHashTable c){
		String self = c.toString();
		
		String s = "[0ec9e6875e4c6e6702e1b81813a0b70d,B],"
		+ "[1aa81a7562b705fb6779655b8e407ee3,A],"
		+ "[1d1eeea52e95de7227efa6e226563cd2,C],"
		+ "[2af91581036572478db2b2c90479c73f,B],"
		+ "[57e1e221c0a1aa811bc8d4d8dd6deaa7,A],"
		+ "[8b872364fb86c3da3f942c6346f01195,C]";
		boolean ret = self.equals(s);
		System.out.println(ret);
		List<String> result = c.find_nodes("splurg", 2, null);
		System.out.println("result is " + result);
		System.out.println("result should be {A, C}");
		System.out.println(c.avoided);
		System.out.println("avoided should be null");

		Set<String> set = new HashSet<>();
		set.add("A");
		List<String> result2 = c.find_nodes("splurg", 2, set);
		System.out.println("result is " + result2);	
		System.out.println("result should be {C, B}");
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
	
	public void testLarge(ConsistentHashTable c){
		List<String> l = c.find_nodes("splurg", 15, null);
		String s = l.get(0);
		//System.out.println(s);
		System.out.println(l.size() == 15);
	}
	
	public void testDistribution(ConsistentHashTable c, Set<String> nodeset){
		//generate a lot of hash values and see how even the distribution is
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
		System.out.println(numkeys + " random hash keys assigned to "
				+ nodeset.size() + " nodes \neach repeated 10 times "
               +"are distributed across the nodes "
               +"\nwith a standard deviation of " + st.stddev() 
               +" (compared to a mean of "+ numkeys / nodeset.size()+")");
	}
	
	public void testFailover(ConsistentHashTable c, Set<String> nodeset) {
		//For a given unavailable node, see what other nodes get new traffic
		List<String> l = new ArrayList<>(nodeset);
		//System.out.println(l);
		Map<List<String>, Integer> transfer = new HashMap<>();
		for(int i=0; i<l.size(); i++) {
			List<String> ln = new ArrayList<>();
			ln.add(l.get(i));
			for(int j=0; j<l.size(); j++) {
				ln.add(l.get(j));
				transfer.put(ln, 0);
				ln.remove(l.get(j));
			}
		}
		int numkeys = 10000;
		for(int i=0; i<numkeys; i++) {
			Stats st = new Stats();
			String key = st.random3Letters();
			//System.out.println(key);
			List<String> node_pair = c.find_nodes(key, 2, null);
			List<String> np = new ArrayList<>();
			np.add(node_pair.get(0));
			np.add(node_pair.get(1));
			if(transfer.containsKey(np)){
				transfer.put(node_pair, transfer.get(node_pair) + 1);
			}else{
				transfer.put(np, 1);
			}	
		}
		Stats st = new Stats();
		for(int i=0; i<l.size(); i++) {
			int num_des_nodes = 0;
			List<String> ll = new ArrayList<>();
			ll.add(l.get(i));
			for(int j=0; j<l.size(); j++) {
				ll.add(l.get(j));
				if(transfer.containsKey(ll) && transfer.get(ll) > 0){
					num_des_nodes += 1;
				}
				ll.remove(l.get(j));
			}
			st.add(num_des_nodes);
		}
		System.out.println("On failure of a single node, "+
				st.mean() + " other nodes (on average) "+
               "handle the transferred traffic from that node.");
	}
}