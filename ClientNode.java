package dynamo;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ClientNode extends Node {

	static int timer_priority = 17;
	Message last_msg = null;
	HashMap<Node,String> nodelist = null;
	Deque<Message> queue = new ConcurrentLinkedDeque<Message>();
	
	public ClientNode()
	{
		super();
		last_msg = null;
	}
	
	public ClientNode(String name)
	{
		super(name);
		last_msg = null;
	}
	
	public ClientNode(HashMap<Node,String> nodelist, String name)
	{
		super(name);
		this.nodelist = nodelist;
		last_msg = null;
	}
	
	//TreeMap<String, Integer> metadata
	public void put(String key, ArrayList<VectorClock> vcs, String value)
	{
		VectorClock metadata = null;
		
		Random rand = new Random();		
		rand.setSeed(0);
		Node[] nodeArray = nodelist.keySet().toArray(new Node[nodelist.keySet().size()]);
		Node destnode = nodeArray[rand.nextInt(nodelist.size())];
		
		if(vcs == null)
		{
			metadata = new VectorClock();
		}
		else 
		{
			metadata = VectorClock.converge(vcs);
		}
		
		ClientPut putmsg = new ClientPut(this, destnode, key, value, metadata);
		Framework.send_message(putmsg);  		 
	}

	//TreeMap<String, Integer> metadata 
	public void put(String key, ArrayList<VectorClock> vcs, String value, Node destnode)
	{
		VectorClock metadata = null;
		if(vcs == null)
		{
			metadata = new VectorClock();
		}
		else 
		{
			metadata = VectorClock.converge(vcs);
		}
		
		ClientPut putmsg = new ClientPut(this, destnode, key, value, metadata);
		Framework.send_message(putmsg);      		
	}
	
	void put(String key, VectorClock metadata, String value)
	{
		Random rand = new Random();
		rand.setSeed(0);
		Node[] nodeArray = nodelist.keySet().toArray(new Node[nodelist.keySet().size()]);
		Node destnode = nodeArray[rand.nextInt(nodelist.size())];
		
		
		ClientPut putmsg = new ClientPut(this, destnode, key, value, metadata);
		Framework.send_message(putmsg);  		 
	}

	void put(String key, VectorClock metadata, String value, Node destnode)
	{
		ClientPut putmsg = new ClientPut(this, destnode, key, value, metadata);
		Framework.send_message(putmsg);      		
	}
	
	public void get(String key)
	{
		Random rand = new Random();
		rand.setSeed(0);
		Node[] nodeArray = nodelist.keySet().toArray(new Node[nodelist.keySet().size()]);
		Node destnode = nodeArray[rand.nextInt(nodelist.size())];  
		
		ClientGet getmsg = new ClientGet(this, destnode, key);
		Framework.send_message(getmsg);
	}
	
	public void get(String key, Node destnode)
	{
		ClientGet getmsg = new ClientGet(this, destnode, key);
		Framework.send_message(getmsg);
	}
	
	// to be done
	public void rsp_timer_pop(ClientPut reqmsg)
	{
		//_logger.info("Put request timed out; retrying")
		ArrayList<VectorClock> vcs = new ArrayList<VectorClock>();
		vcs.add(reqmsg.metadata);
		put(reqmsg.key, vcs, reqmsg.value);
	}

	public void rsp_timer_pop(ClientGet reqmsg)
	{
		//_logger.info("Get request timed out; retrying")
		get(reqmsg.key);
	}

    //API for other threads to send message
    @Override
    public void sndmsg(Message msg)
    {
    	queue.add(msg); 
    }	
	
	@Override
	public void rcvmsg(Message msg)
	{
		last_msg = msg;
	}
	
    public void run(){  
    	System.out.println(System.currentTimeMillis() + ": Thread:" + this.name +"(" + this.getClass().getSimpleName() + ")" + " is running...");
    	
    	while(true) {
        	if(Thread.interrupted())
        	{
        		System.out.println(System.currentTimeMillis() + ": Thread:" + this.name +"(" + this.getClass().getSimpleName() + ")" + " is exiting...");
        		return;
        	}
        	
        	//check if there is message in the deque
        	if(!queue.isEmpty())
        	{
        		Message msg = queue.remove();
        		
        		System.out.println(System.currentTimeMillis() + ": Thread:"+ this.name +"(" + this.getClass().getSimpleName() + ")"
        				+ ": Receive: " + msg.from_node.name +"->" + msg.to_node.name + " " 
   	                   + msg.getClass().getSimpleName() +"(" + msg.toString() +")");
        		
        		rcvmsg(msg);
        	}
        	else {
        	     try {
        	    	 Thread.sleep(1000);
        	     }
        	     catch(InterruptedException e)
        	     {
        	    	 System.out.println(System.currentTimeMillis() + ": Thread:" + this.name +"(" + this.getClass().getSimpleName() + ")" + " is exiting...");
        	    	 return;
        	     }  
        	}
        	
    	}

    }  
    
}
