package dynamo;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

public class Framework {
    static HashMap<Node, HashSet<Node>> cuts = new HashMap<Node, HashSet<Node>>();
    static Deque<Message> queue = new ConcurrentLinkedDeque<Message>();
    static HashMap<Message,Timer> pending_timers = new HashMap<Message,Timer>();
    boolean expect_reply = true;
    static boolean enableThreads = false;
    static Queue<HeartMessage> healthQ = new LinkedList<HeartMessage>();


    public static void reset(){
        cuts.clear();
        queue.clear();
        pending_timers.clear();
    }

    public static void cut_wires(List<Node> from_nodes, List<Node> to_nodes){
        for(Node node:from_nodes)
        {
        	HashSet<Node> nodeSets = new HashSet<Node>();
        	nodeSets.addAll(to_nodes);
        	cuts.put(node, nodeSets);
        	
        	//print
        	for(Node dst:to_nodes)
        	{
        		System.out.println(System.currentTimeMillis() + ": Cut: " + node.name +" -> " + dst.name);
        	}
        }                
    }

    public static boolean reachable(Node from_node, Node to_node){
       
       HashSet<Node> nodeSets = null;
       nodeSets = cuts.get(from_node);
       if(nodeSets == null)
          return true;
       
       if(nodeSets.contains(to_node))
       {
    	   return false;
       }
       
       return true;
    }

    public static void send_message(Message msg){
    	
        //Send a message
    	System.out.println(System.currentTimeMillis() + ": Send:    " + msg.from_node.name +"->" + msg.to_node.name + " " 
                + msg.getClass().getSimpleName() +"(" + msg.toString() +")");
    	
    	if(enableThreads)
    	{
    		msg.to_node.sndmsg(msg);
    		return;
    	}
    	
    	queue.add(msg);        
                
        // Automatically run timers for request messages if the sender can cope
        //with retry timer pops
        if (!(msg instanceof ResponseMessage)){
        	Timer timer = null;
            
        	timer = TimerManager.start_timer(msg.from_node, "msg", "rsp_timer_pop", -1); 
            timer.msg = msg;
            pending_timers.put(msg, timer);
        }
    }

    public static void remove_req_timer(Message reqmsg){
        if (pending_timers.containsKey(reqmsg)) {
            //Cancel request timer as we've seen a response
            TimerManager.cancel_timer(pending_timers.get(reqmsg));
            pending_timers.remove(reqmsg);
        }
    }

    //Cancel all pending-request timers destined for the given node.
    //Returns a list of the request messages whose timers have been cancelled.
    public static ArrayList<Message> cancel_timers_to(Node destnode){
        ArrayList<Message> failed_requests = new ArrayList<Message>();

        for(Message reqmsg : pending_timers.keySet()){
            if (reqmsg.to_node == destnode) {
                TimerManager.cancel_timer(pending_timers.get(reqmsg));
                pending_timers.remove(reqmsg);
                failed_requests.add(reqmsg);
            }
        }
        return failed_requests;
    }


    public static void rsp_timer_pop(Message reqmsg) {
        //Remove the record of the pending timer
        pending_timers.remove(reqmsg);

        // Call through to the node's rsp_timer_pop() method
        reqmsg.from_node.rsp_timer_pop((DynamoRequestMessage)reqmsg);
    }

	public static void forward_messsage(ClientGet msg, Node coordinator) {
        //Forward a message
        System.out.println(System.currentTimeMillis() + ": Forward: " + msg.to_node.name +"->" + coordinator.name + " " 
                + msg.getClass().getSimpleName() +"(" + msg.toString() +")");
        
		ClientGet fwd_msg = new ClientGet(msg.from_node, msg.to_node, msg.key, msg.msg_id);
		fwd_msg.intermediate_node = fwd_msg.to_node;
		fwd_msg.original_msg = msg;
		fwd_msg.to_node = coordinator;
		
    	if(enableThreads)
    	{
    		fwd_msg.to_node.sndmsg(fwd_msg);
    		return;
    	}
		
		queue.add(fwd_msg);
	}

	public static void forward_messsage(ClientPut msg, Node coordinator) {
        //Forward a message
        System.out.println(System.currentTimeMillis() + ": Forward: " + msg.to_node.name +"->" + coordinator.name + " " 
                + msg.getClass().getSimpleName() +"(" + msg.toString() +")");
        
		ClientPut fwd_msg = new ClientPut(msg.from_node, msg.to_node, msg.key, msg.value, msg.metadata, msg.msg_id);
		fwd_msg.intermediate_node = fwd_msg.to_node;
		fwd_msg.original_msg = msg;
		fwd_msg.to_node = coordinator;
		
    	if(enableThreads)
    	{
    		fwd_msg.to_node.sndmsg(fwd_msg);
    		return;
    	}		
		
		queue.add(fwd_msg);
		
	}
    
	public static void schedule() {
		schedule(32678,32678);
	}
	
	//!!! to be done
	public static void schedule(int msgs_to_process) {
		schedule(msgs_to_process,32678);
	}
	
    public static void schedule(int msgs_to_process, int timers_to_process) {
        //Schedule given number of pending messages
        while (_work_to_do()){

            //Process all the queued up messages (which may enqueue more along the way)
            while (!queue.isEmpty()) {
                Message msg = queue.remove();
                if (msg.to_node.failed) {
                	System.out.println(System.currentTimeMillis() + ": Drop: " + msg.from_node.name +"->" + msg.to_node.name + " as destination down " 
     	                   + msg.getClass().getSimpleName() +"(" + msg.toString() +")");
                } else if(!reachable(msg.from_node, msg.to_node)) {
                	System.out.println(System.currentTimeMillis() + ": Drop: " + msg.from_node.name +"->" + msg.to_node.name + " as route down " 
      	                   + msg.getClass().getSimpleName() +"(" + msg.toString() +")");
                } else {
           
                    if (msg instanceof ResponseMessage) {
                        //figure out the original request this is a response to
                        Message reqmsg;
                        if(((ResponseMessage) msg).response_to.original_msg != null)
                        {
                            reqmsg = ((ResponseMessage) msg).response_to.original_msg;
                        } 
                        else {
                        	reqmsg = ((ResponseMessage) msg).response_to;
                        }

                        // cancel any timer associated with the original request
                        remove_req_timer(reqmsg);
                    }
                    System.out.println(System.currentTimeMillis() + ": Receive: " + msg.from_node.name +"->" + msg.to_node.name + " " 
      	                   + msg.getClass().getSimpleName() +"(" + msg.toString() +")");
                    
                    msg.to_node.rcvmsg(msg);

                }
                msgs_to_process = msgs_to_process - 1;
                if (msgs_to_process == 0) {
                    return;
                }

                // No pending messages; potentially pop a (single) timer
                if (TimerManager.pending_count() > 0 && timers_to_process > 0) {
                    //Pop the first pending timer; this may enqueue work
                    TimerManager.pop_timer();
                    timers_to_process = timers_to_process - 1;
                }
                if (timers_to_process == 0) {
                    return;
                }
            }
        }
    }

    //Indicate whether there is work to do
    public static boolean _work_to_do(){
        if (!queue.isEmpty()){
            return true;
        }
        if (TimerManager.pending_count() > 0){
            return true;
        }
        return false;
    }

    //Reset all message and other history
    public static void reset2(){
        Framework.reset();
        TimerManager.reset();
    }

    //Reset all message and other history, and remove all nodes
    public static void reset_all(){
        reset2();
        Node.resetNode();
    }

    //push heartbeat message into healthQ, for other nodes to forward
    public static void sendHeartbeatTo(HeartMessage msg){
        if(msg instanceof HeartMessage){
            System.out.println(msg.from_node + " reports "+ msg.aboutNode + ": "+ msg.aboutNodeStatus
                    + " forward times: "+msg.forwardtimes+ " msg_id: "+ msg.heartmsg_id);
            synchronized (Framework.healthQ) {
                healthQ.offer(msg);
            }
        }
    }

    public static boolean sendForwardResponse(Node sendingNode,HeartMessage msg, Node destNode){
//        ((DynamoNode1)originNode).getResponse();
        ((DynamoNode1)sendingNode).receiveReport(msg, destNode, msg.msg_id);
        System.out.println("gossiping!!!!!!");
        return true;
    }
    public static boolean getForwardResponse(){
        return true;
    }

    
}