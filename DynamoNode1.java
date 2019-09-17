package dynamo;

import java.util.*;

//import dynamo.DynamoNode.ResponseTriples;
//import dynamo.DynamoNode.ResponseTuples;

public class DynamoNode1 extends Node {

	static int T = 10;    //Number of repeats for nodes in consistent hash table
	static int N = 3;     //Number of nodes to replicate at
	static int W = 2;     //Number of nodes that need to reply to a write operation
	static int R = 2;  //Number of nodes that need to reply to a read operation
        
    static HashMap<Node,String> nodelist = new HashMap<Node,String>();
    static HashMap<String, Node> namelist = new HashMap<String, Node>();
    static ConsistentHashTable chash = null;

   /******/
    static HashMap<String, Boolean> healthTable = new HashMap<>();
    static HashMap<String, HashSet<Node>> downList = new HashMap<>();
	static HashMap<String, HashSet<Node>> upList = new HashMap<>();

    //key => (value, metadata): divideded into two HashMap or tuples
    //https://stackoverflow.com/questions/4956844/hashmap-with-multiple-values-under-the-same-key
    HashMap<String, String> local_store = new HashMap<String, String>(); 
    HashMap<String, VectorClock> local_store_meta = new HashMap<String, VectorClock>();
    
    //to be done
    HashMap<Integer, HashSet<Node>> pending_put_rsp = new HashMap<Integer, HashSet<Node>>();  // seqno => set of nodes that have stored
    HashMap<Integer, Message> pending_put_msg = new HashMap<Integer, Message>();  // seqno => original client message
    HashMap<Integer, HashSet<ResponseTriples>> pending_get_rsp = new HashMap<Integer, HashSet<ResponseTriples>>();  //seqno => set of (node, value, metadata) tuples
    HashMap<Integer, Message> pending_get_msg = new HashMap<Integer, Message>();  // seqno => original client message
    
    DynamoNode1()
    {    	
    	super();
    	
    	nodelist.put(this, super.name);
    	namelist.put(super.name, this);
    	chash = new ConsistentHashTable(new ArrayList<String>(namelist.keySet()), T);

		// Create producer thread
		Thread t1 = new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				try
				{
					sendHeartbeat();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		});

		// Create consumer thread
		Thread t2 = new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				try
				{
					gossipMsg();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		});

		// Start both threads
		t1.start();
		t2.start();


		healthTable.put(super.name, true);
		//System.out.println("thread created " + this.name);
		//System.out.println("healthtable：" + healthTable.toString());
		//System.out.println("downList: " + downList.toString());
	}




	void sendHeartbeat(){
		while(true){
			//report self is alive
			//System.out.println( Thread.currentThread().getName() + "report health");
			report(this, null,this.name, true);

			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}

	void gossipMsg() {
		while(true) {

			//forward the heartmsg getting from the other node
			HeartMessage fwd_msg = Framework.healthQ.peek();
			if (fwd_msg != null && fwd_msg.from_node != this && fwd_msg.to_node != this) {
				forward(this, fwd_msg);
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean forward(Node fromnode, HeartMessage fwd_msg) {

		if(++fwd_msg.forwardtimes >= 3 * nodelist.size() || isNodeUp(fwd_msg) || isNodeDown(fwd_msg)){
			// not forwarding anymore
			synchronized (Framework.healthQ) {
				Framework.healthQ.remove(fwd_msg);
			}
			return true;
		}

		Random rand = new Random();

		int randNo = rand.nextInt(nodelist.size());
		while((String.valueOf('A'+randNo)).equals(fromnode.name)) {
			randNo = rand.nextInt(nodelist.size());
		}

		Node tonode  = namelist.get(String.valueOf((char)('A'+randNo)));
		fwd_msg.from_node = fromnode;
		fwd_msg.to_node = tonode;

		Framework.sendHeartbeatTo(fwd_msg); //goosip

		return true;
	}

	public void report(Node fromnode, Node tonode,String aboutNode, boolean nodeStatus){
		if(fromnode.name.equals(tonode)){
			return;
		}

		Random rand = new Random();

		int randNo = rand.nextInt(nodelist.size());
		while((String.valueOf('A'+randNo)).equals(fromnode.name) ) {
			randNo = rand.nextInt(nodelist.size());
		}

		Node destnode  = namelist.get(String.valueOf((char)('A'+randNo)));
		HeartMessage msg = new HeartMessage(fromnode, destnode,aboutNode,nodeStatus);
		Framework.sendHeartbeatTo(msg); //notice other node I am alive

	}

	public boolean isNodeDown(HeartMessage msg) {
		HashSet<Node> members;
		DynamoNode1 destnode = (DynamoNode1) msg.to_node;

		if(msg.aboutNodeStatus == false) {
			String failnode =  msg.aboutNode;
			if(downList.containsKey(failnode)) {
				members = downList.get(failnode);
				members.add(msg.from_node);
				members.add(msg.to_node);

			} else {
				members = new HashSet<Node>();
				members.add(msg.from_node);
				members.add(msg.to_node);
				downList.put(failnode, members);
			}

			System.out.println("Foward Message is: " + this.name+ " report node " + failnode + "is failed");

			if(members.size() == nodelist.size()) {
				System.out.println("all member knows node " + failnode + " dies");
				namelist.get(failnode).fail();


				Node failed = namelist.get(failnode);
				nodelist.remove(failed);
				node_name.remove(failed);
				namelist.remove(failnode);
				name_node.remove(failnode);
				return true;
			}

		}
		return false;
	}

	public boolean isNodeUp(HeartMessage msg) {
		HashSet<Node> members;
		String goodnode =  msg.aboutNode;
		if(msg.aboutNodeStatus == true) {
			if(upList.containsKey(goodnode)) {
				members = upList.get(goodnode);
				members.add(msg.from_node);
				//members.add(msg.to_node);

			} else {
				members = new HashSet<Node>();
				members.add(msg.from_node);
				members.add(msg.to_node);
				upList.put(goodnode, members);
			}

			System.out.println("Foward Message is: " + this.name+ " report node " + goodnode + "is good");

			if(members.size() == nodelist.size()) {
				members.clear();
				System.out.println("now all member knows node " + goodnode + " is good\n");
				return true;
			}
		}
		return false;
	}

	public boolean receiveReport(HeartMessage htmsg, Node dest_node, int htmsg_id){
		sendResponse(htmsg,dest_node,htmsg_id);
		//forward(this,dest_node,htmsg.aboutNode,htmsg.aboutNodeStatus);
		return true;
	}

	public boolean sendResponse(HeartMessage htmsg, Node to_node, int htmsg_id){
		System.out.println(this.name + "send htmsg "+ htmsg.heartmsg_id + " to node "+ to_node.name);
		return true;
	}


//	public void getResponse(){
//		System.out.println("forwording health message");
//	}


	 
    //@Override
    public static void reset()
    {    	
    	nodelist.clear();
    	namelist.clear();
    	
    	chash = null;	
    }
    
    public void store(String key, String value, VectorClock metadata)
    { 
    	local_store.put(key, value);
    	local_store_meta.put(key, metadata);
    }
    
    //To be done: use structure
    public String retrieve(String key)
    {
    	String value = null;
    	
    	if(local_store.containsKey(key))
    	{
    		value = local_store.get(key);
    	}
    	
    	return value;
    }

    public VectorClock retrieveMeta(String key)
    {
    	VectorClock metadata = null;
    	
    	if(local_store_meta.containsKey(key))
    	{
    		metadata = local_store_meta.get(key);
    	}
    	
    	return metadata;
    }

    public ArrayList<Node> find_preference_list(String key)
    {
		
		ArrayList<String> preference_list_name = null;
		ArrayList<Node> preference_list = new ArrayList<Node>();

		preference_list_name = chash.find_nodes(key, N, null); //to be done
		for(String name : preference_list_name)
		{
			preference_list.add(namelist.get(name));
		}   	
		
		return preference_list;
    }
    
    //to be done
    public void rcv_clientput(ClientPut msg)
    {
    	ArrayList<Node> preference_list = find_preference_list(msg.key); //to be done
    	
    	if(preference_list.contains(this))
    	{
    		int seqno = generate_sequence_number();    		
    		//_logger.info("%s, %d: put %s=%s", self, seqno, msg.key, msg.value)
    		
    		//# For now, metadata is just sequence number at coordinator
    		VectorClock metadata = new VectorClock();
    		metadata.update(name, seqno);
    		
    		//# Send out to preference list, and keep track of who has replied
    		pending_put_rsp.put(seqno, new HashSet<Node>());
    		pending_put_msg.put(seqno, msg);
    		
    		int reqcount = 0;
    		for(Node node : preference_list) {
    			//# Send message to get node in preference list to store
    			PutReq putmsg = new PutReq(this, node, msg.key, msg.value, metadata, seqno, null);
    			Framework.send_message(putmsg);
    			
    			reqcount = reqcount + 1;
    			if(reqcount >= N)
    				break;
    		}
    		
    	}
    	else {
    		// Forward to the coordinator for this key
    		//_logger.info("put(%s=%s) maps to %s", msg.key, msg.value, preference_list)
    		
    		Node coordinator = preference_list.get(0);
    		Framework.forward_messsage(msg, coordinator);  		
    	}
    }
    
    public void rcv_clientget(ClientGet msg)
    {
    	ArrayList<Node> preference_list = find_preference_list(msg.key); //to be done
    	
    	//# Determine if we are in the list
    	if(preference_list.contains(this)) {
    		int seqno = generate_sequence_number(); 
    		
    		pending_get_rsp.put(seqno, new HashSet<ResponseTriples>());
    		pending_get_msg.put(seqno, msg);
    		
    		int reqcount = 0;
    		for(Node node : preference_list) {
    			//# Send message to get node in preference list to store
    			GetReq getmsg = new GetReq(this, node, msg.key, seqno);
    			Framework.send_message(getmsg);
    			
    			reqcount = reqcount + 1;
    			if(reqcount >= N)
    				break;
    		}
    	}
    	else {
    		//# Forward to the coordinator for this key
            //_logger.info("get(%s=?) maps to %s", msg.key, preference_list)
            
    		Node coordinator = preference_list.get(0);
    		Framework.forward_messsage(msg, coordinator);     		
    	}
    	
    }
    
    public void rcv_put(PutReq putmsg)
    {
    	//_logger.info("%s: store %s=%s", self, putmsg.key, putmsg.value)
    	
    	store(putmsg.key, putmsg.value, putmsg.metadata);
    	PutRsp putrsp = new PutRsp(putmsg);
    	
    	Framework.send_message(putrsp);
    }
    
    public void rcv_putrsp(PutRsp putrsp)
    {
    	int seqno = putrsp.msg_id;
    	
    	if(pending_put_rsp.containsKey(seqno)) {
    		HashSet<Node> nodeSet = pending_put_rsp.get(seqno);
    		nodeSet.add(putrsp.from_node);
    		
    		if(nodeSet.size() >= W) {
    			
                //_logger.info("%s: written %d copies of %s=%s so done", self, DynamoNode.W, putrsp.key, putrsp.value)
                //_logger.debug("  copies at %s", [node.name for node in self.pending_put_rsp[seqno]])
    			
    			//# Tidy up tracking data structures
    			ClientPut original_msg = (ClientPut)pending_put_msg.get(seqno);
    			pending_put_rsp.remove(seqno);
    			pending_put_msg.remove(seqno);
    			
    			//Reply to the original client
    			ClientPutRsp client_putrsp = new ClientPutRsp(original_msg);
    			Framework.send_message(client_putrsp);
    		}
    	}
    }
    
    public void rcv_get(GetReq getmsg)
    {
    	//_logger.info("%s: retrieve %s=?", self, getmsg.key)
    	
    	String value;
    	VectorClock metadata;
    	
    	value = retrieve(getmsg.key);
    	metadata = retrieveMeta(getmsg.key);
    	
    	GetRsp getrsp = new GetRsp(getmsg, value, metadata); 
    	Framework.send_message(getrsp);
    }
    
    public void rcv_getrsp(GetRsp getrsp)
    {
    	int seqno = getrsp.msg_id;
    	
    	if(pending_get_rsp.containsKey(seqno)) {
    		
    		HashSet<ResponseTriples> responseSet = pending_get_rsp.get(seqno);
    		ResponseTriples response = new ResponseTriples(getrsp.from_node, getrsp.value.get(0), getrsp.metadata.get(0));
    		responseSet.add(response);
    		
    		if(responseSet.size() >= R)
    		{
    			//_logger.info("%s: read %d copies of %s=? so done", self, DynamoNode.R, getrsp.key)
                //_logger.debug("  copies at %s", [(node.name, value) for (node, value, _) in self.pending_get_rsp[seqno]])
    			
    			//# Build up all the distinct values/metadata values for the response to the original request
    			HashSet<ResponseTuples> results = new HashSet<ResponseTuples>();
    			for(ResponseTriples item : responseSet) {
    				
    				ResponseTuples value_metadata = new ResponseTuples(item.value, item.metadata);
    			    results.add(value_metadata);
    			}
    			
    			//# Tidy up tracking data structures
    			ClientGet original_msg = (ClientGet)pending_get_msg.get(seqno);
    			pending_get_rsp.remove(seqno);
    			pending_get_msg.remove(seqno);
    			
    			//# Reply to the original client, including all received values
    			ArrayList<String> value_list = new ArrayList<String>();
    			ArrayList<VectorClock> meta_list = new ArrayList<VectorClock>();
    			
    			for(ResponseTuples item : results) {
    				value_list.add(item.value);
    				meta_list.add(item.metadata);   //to be done???
    			}
    			
    			ClientGetRsp client_getrsp = new ClientGetRsp(original_msg, value_list, meta_list);
    			Framework.send_message(client_getrsp);
    		}    		
    	}
    }
    
    @Override
    public void rcvmsg(Message msg)
    {
    	if(msg instanceof ClientPut)
    	{
    		rcv_clientput((ClientPut)msg);
    	}
    	else if(msg instanceof PutReq) 
        {
        	rcv_put((PutReq)msg);
        }
    	else if(msg instanceof PutRsp)
    	{
        	rcv_putrsp((PutRsp)msg);
        }
    	else if(msg instanceof ClientGet)  
    	{
        	rcv_clientget((ClientGet)msg);
        }
    	else if(msg instanceof GetReq)
    	{
        	rcv_get((GetReq)msg);
        } 
    	else if(msg instanceof GetRsp)
    	{
        	rcv_getrsp((GetRsp)msg);
        }
        else
        {
        	//raise TypeError("Unexpected message type %s", msg.__class__)
        }
    	
    }
    
    public String get_contents()
    {
    	StringBuffer result = new StringBuffer();
    	   	    
    	for(String key : local_store.keySet()) {
    	    String value = local_store.get(key);
    	    
    	    result.append(key).append(':').append(value).append(", ");
    	}
    	
    	return result.toString();
    }


    public static boolean restartNode(String nodeName){

    	DynamoNode1 dynamoNode = new DynamoNode1();

    	downList.remove(nodeName);
    	healthTable.put(nodeName, true);
		nodelist.remove(namelist.get(nodeName));
		nodelist.put(dynamoNode,nodeName);
		namelist.put(nodeName, dynamoNode);
    	return true;
	}

    public class DynamoClientNode extends Node
    {

		public DynamoClientNode(String name){
    		super(name);
		}


		void put(String key, VectorClock metadata, String value)
    	{
    		Random rand = new Random();
    		
    		//--Node[] nodeArray = (Node[])(nodelist.keySet().toArray());
			Node[] nodeArray = new Node[nodelist.size()];
			for(int i = 0; i < nodelist.size();i++){
				Iterator<Node> it = nodelist.keySet().iterator();
				nodeArray[i] = it.next();
			}

			Node destnode = nodeArray[rand.nextInt(nodelist.size())];
			System.out.println("put msg to a random node:" + this.name + "---> "+ destnode.name);
    		
    		ClientPut putmsg = new ClientPut(this, destnode, key, value, metadata);
    		Framework.send_message(putmsg);  		 
    	}

    	void put(String key, VectorClock metadata, String value, Node destnode)
    	{
    		ClientPut putmsg = new ClientPut(this, destnode, key, value, metadata);
    		Framework.send_message(putmsg);      		
    	}
    	
    	void get(String key)
    	{
    		Random rand = new Random();
			//--Node[] nodeArray = (Node[])(nodelist.keySet().toArray());
			Node[] nodeArray = new Node[nodelist.size()];
			for(int i = 0; i < nodelist.size();i++){
				Iterator<Node> it = nodelist.keySet().iterator();
				nodeArray[i] = it.next();
			}

			Node destnode = nodeArray[rand.nextInt(nodelist.size())];
    		
    		ClientGet getmsg = new ClientGet(this, destnode, key);
    		Framework.send_message(getmsg);
    	}

    	void get(String key, Node destnode)
    	{
    		ClientGet getmsg = new ClientGet(this, destnode, key);
    		Framework.send_message(getmsg);
    	}
    	
    	@Override
    	public void rcvmsg(Message msg)
    	{
    		
    	}
    }
    
}
