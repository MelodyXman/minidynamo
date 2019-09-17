package dynamo;

/*import java.util.*;

public class ConsistentHashTable {
	
	ConsistentHashTable(ArrayList<Node> nodelist, int T)
	{
		
	}

	ArrayList<Node> find_nodes(String key, int N)
	{
		return null;
	}
	
	ArrayList<Node> find_nodes(String key, int N, HashSet<Node> failed_nodes) 
	{
		return null;
	}
	
	
	ArrayList<Node> find_nodes_avoided(String key, int N, HashSet<Node> failed_nodes) 
	{
		return null;
	}
}*/

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ConsistentHashTable {
	
	//TreeMap and TreeSet keep the order
	TreeMap<String, String> nodelist = new TreeMap<String, String>(); //List<List<String>> nodelist;
	TreeSet<String> hashlist = new TreeSet<String>(); //List<String> hashlist = new ArrayList<>();
	int repeat;
	public ConsistentHashTable(){}
	public ConsistentHashTable(List<String> nl, int repeat){
		this.repeat = repeat;
		for(String node : nl) {
			for(int i=0; i<repeat; i++){
				String hashvalue = generateHash(node + ":" + Integer.toString(i));				
				nodelist.put(hashvalue, node);
				hashlist.add(hashvalue);
			}
		}
	}
	
	public String generateHash(String s){	
	     MessageDigest md;
	     StringBuffer sb = new StringBuffer();
	     try {
	          md = MessageDigest.getInstance("MD5");
	          md.update(s.getBytes());
	          byte[] data = md.digest();
	          int index;
	          for(byte b : data) {
	               index = b;
	               if(index < 0) index += 256;
	               if(index < 16) sb.append("0");
	               sb.append(Integer.toHexString(index));
	          }
	     } catch (NoSuchAlgorithmException e) {
	      e.printStackTrace();
	     }
	     return sb.toString();
		 
	}
	
	public ArrayList<String> avoided = new ArrayList<>(); //to be done 
	
	public ArrayList<String> find_nodes_avoided(String key, int count, Set<String> avoid) {
		ArrayList<String> avoided_nodes = new ArrayList<>();
		
		if(avoid == null) {
			avoid = new HashSet<>();
		}
		//hash the key to find where is belongs on the ring;
		String hashVal = generateHash(key);
		
		//find the node after this hash value around the ring, as an index
		//into self hashlist/self, nodelist;
		int inital_index = -1;
		int next_index = -1;
		String node;
	
		String[] hashArray = hashlist.toArray(new String[hashlist.size()]);
		
		for(int i=0; i<hashArray.length; i++){
			int a = hashArray[i].compareTo(hashVal);
			if(a>0){
				inital_index = i;
				next_index = inital_index;
				break;
			}
		}
		
		ArrayList<String> results = new ArrayList<>();
		while(results.size() < count) {
			if(next_index >= nodelist.size() || next_index < 0){
				next_index = 0;
			}
			node = nodelist.get(hashArray[next_index]);
			
			if(avoid.contains(node)){
				if(!avoided_nodes.contains(node)){
					avoided_nodes.add(node);
				}
			}else if(!results.contains(node)){
				results.add(node);
			}
			next_index = next_index+1;
			if(next_index == inital_index){
				break;
			}
		}	
		
		return avoided_nodes;
	}
	
	public ArrayList<String> find_nodes(String key, int count, Set<String> avoid){
		if(avoid == null) {
			avoid = new HashSet<>();
		}
		//hash the key to find where is belongs on the ring;
		String hashVal = generateHash(key);
		
		//find the node after this hash value around the ring, as an index
		//into self hashlist/self, nodelist;
		int inital_index = -1;
		int next_index = -1;
		String node;
		//String[] hashArray = (String[])(hashlist.toArray());
		
		String[] hashArray = hashlist.toArray(new String[hashlist.size()]);
		
		for(int i=0; i<hashArray.length; i++){
			int a = hashArray[i].compareTo(hashVal);
			if(a>0){
				inital_index = i;
				next_index = inital_index;
				break;
			}
		}
		
		avoided.clear();
		ArrayList<String> results = new ArrayList<>();
		while(results.size() < count) {
			if(next_index >= nodelist.size() || next_index < 0){
				next_index = 0;
			}
			node = nodelist.get(hashArray[next_index]);
			
			if(avoid.contains(node)){
				if(!avoided.contains(node)){
					avoided.add(node);
				}
			}else if(!results.contains(node)){
				results.add(node);
			}
			next_index = next_index+1;
			if(next_index == inital_index){
				break;
			}
		}
		return results;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();

		for(String hash : hashlist){
			sb.append('[').append(hash).append(',').append(nodelist.get(hash)).append(']').append(',');
		}
		sb.deleteCharAt(sb.length()-1);
		return sb.toString();
	}
	
	public void Print(String s){
		System.out.println(s);
	}
}
