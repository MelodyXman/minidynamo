package dynamo;

import java.util.*;

//Node that can send and receive messages.
public class Node extends Thread {

  static int node_count;
  static HashMap<String, Node> name_node = new HashMap<String, Node>(); //name --> node
  static HashMap<Node, String> node_name = new HashMap<Node, String>(); //node --> name
  
  String name;
  int next_sequence_number;
  boolean included;   //Whether this node is included in lists of nodes
  boolean failed;     //Indicates current failure

  public Node()
  {
	  this.name = next_name();
	  
	  next_sequence_number = 0;
	  included = true;  //Whether this node is included in lists of nodes
	  failed = false;   //Indicates current failure
	  
	  name_node.put(this.name, this);
	  node_name.put(this, this.name);
      
	  System.out.println(System.currentTimeMillis() + ": AddNode: " + this.getClass().getName() +"(" + this.name + ")");
  }  
  
  public Node(String name)
  {
	  this.name = name;
	  
	  next_sequence_number = 0;
	  included = true;  //Whether this node is included in lists of nodes
	  failed = false;   //Indicates current failure
	  
	  name_node.put(this.name, this);
	  node_name.put(this, this.name);
      
	  System.out.println(System.currentTimeMillis() + ": AddNode: " + this.getClass().getName() +"(" + this.name + ")");
  }

  public static void resetNode(){
	  node_count = 0;
	  name_node.clear();
	  node_name.clear();
  }

  public String next_name(){
	  StringBuffer sbuffer = new StringBuffer();
	  
      if (node_count < 26) {
          sbuffer.append((char)('A' + node_count));
      } else if (node_count < 26 * 26) {
          int hi = node_count/26;
          int lo = node_count%26;
          sbuffer.append((char)('A' + hi - 1));
          sbuffer.append((char)('A' + lo));         

      } else {
    	  System.out.println("Not Implemented");
      }

      node_count++;
      return sbuffer.toString();
  }

  public int getNodePriority()
  {
	  return -1;
  }
  
  public void retry_failed_node(String reason)
  {
	  
  }

  public String get_contents(){
      return null;
  }

  public String toString(){
	  return this.name;
  }

  //Mark this Node as currently failed; all messages to it will be dropped
  public void fail(){
      this.failed = true;
      System.out.println("Node fail: " + this.name);
  }

  //Mark this Node as not failed
  public void recover(){
      this.failed = false;
      System.out.println("Node recover: " + this.name);

  }

  //Remove this Node from the system-wide lists of Nodes
  public void remove(){
      this.included = false;
      System.out.println("Node remove: " + this.name);
  }

  //Restore this Node to the system-wide lists of Nodes
  public void restore(){
      this.included = true;
      System.out.println("Node restore: " + this.name);
  }

  //Generate next sequence number for this Node
  public int generate_sequence_number(){
	  this.next_sequence_number = this.next_sequence_number + 1; 
      return this.next_sequence_number++;
  }
  
  public void rsp_timer_pop(DynamoRequestMessage reqmsg) {}

  //Subclasses need to implement rcvmsg to allow processing of messages
  public void rcvmsg(Message msg) {}
  
  //Subclasses need to implement rcvmsg to allow other thread to sending messages
  public void sndmsg(Message msg) {}

  //Subclasses need to implement rcvmsg to allow processing of timer pops
  public void timer_pop(String reason) {}
     
}