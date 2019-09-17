package dynamo;

import java.util.*;

public class TimerManager {

	int DEFAULT_PRIORITY = 10;
	
	//# List of pending timers, maintained in order of priority then insertion
	//# list of (priority, tmsg) tuples
	static TreeMap<Integer, HashSet<Timer>> pending = new TreeMap<Integer, HashSet<Timer>>();
	
	public int _priority(Timer tmsg)
	{
		int priority;
		Node node = tmsg.from_node;
		
		priority = node.getPriority();
		if(priority == -1)
		{
			priority = DEFAULT_PRIORITY;
		}
		return priority;
	}
	
	synchronized public static int pending_count()
	{
		int len = 0;
		Set<Integer> keys = pending.keySet();
		
		for(int priority:keys) {
			HashSet<Timer> timerSet = null;
			timerSet = pending.get(priority);
			
			if(timerSet != null) {
				len = len + timerSet.size();
			}
		}
		
		return len;
		
	}
	
	synchronized public static void reset()
	{
		pending.clear();
	}	
	
	//"""Start a timer for the given node, with an option reason code"""
	synchronized public static Timer start_timer(Node node, String reason, String callback, int priority) {
		Timer tmsg = null;
		
		tmsg = new Timer(node, reason, callback, priority);
		
		//History.add("start", tmsg);
        //if priority is None:  # default to priority of the node
        //priority = _priority(tmsg)
        //_logger.debug("Start timer %s prio %d for node %s reason %s", id(tmsg), priority, node, reason)
		
		//# Figure out where in the list to insert
		if(pending.containsKey(priority)){
			HashSet<Timer> timerSet = pending.get(priority);
			timerSet.add(tmsg);
			pending.put(priority, timerSet);
		}
		else {
			HashSet<Timer> timerSet = new HashSet<Timer>();
			timerSet.add(tmsg);
			pending.put(priority, timerSet);
		}
		
		return tmsg;
	}
	
	//"""Cancel the given timer"""
	synchronized public static void cancel_timer(Timer tmsg)
	{  
		HashSet<Timer> timerSet = null;
		int priority = tmsg.priority;
		
		if(pending.containsKey(priority)) {
			timerSet = pending.get(priority);
			timerSet.remove(tmsg);
			
			if(timerSet.size() == 0)
			{
				pending.remove(priority);
			}
			else {
				pending.put(priority, timerSet);
			}
			//_logger.debug("Cancel timer %s for node %s reason %s", id(tmsg), tmsg.from_node, tmsg.reason)
			//History.add("cancel", tmsg)
		}
		
	}
	
	//"""Pop the first pending timer"""
	synchronized public static void pop_timer()
	{
		int priority;
		HashSet<Timer> timerSet = null;
		Timer tmsg = null;
		
		while(true) {
			if(pending.size() > 0) {
				priority = pending.firstKey();
				timerSet = pending.get(priority);
				
				if(timerSet.size() == 0)
				{
					pending.remove(priority);
					continue;
				}
				
				//pop
				tmsg = (Timer)(timerSet.toArray()[0]);
				timerSet.remove(tmsg);
				pending.put(priority, timerSet);
								
	            //_logger.debug("Pop timer %s for node %s reason %s", id(tmsg), tmsg.from_node, tmsg.reason)
	            //History.add("pop", tmsg)
				
				if(tmsg.from_node.failed) {
					continue;
				}
				
				if(tmsg.reason == "retry") {
					tmsg.from_node.retry_failed_node("retry");
				}
				else if(tmsg.reason == "msg") {
					Framework.rsp_timer_pop(tmsg.msg);
				}
				else {
					//error
				}
				
				
			}
			else {
				break;
			}
		}

		 
	}
	
}
