package dynamo;

import javax.swing.*;
import java.util.*;
import java.util.Timer;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

//add use of vector clocks for metadata
public class DynamoNode2 extends Node
{
    static int timer_priority = 20;
    static int T = 10;    //Number of repeats for nodes in consistent hash table
    static int N = 3;     //Number of nodes to replicate at
    static int W = 2;     //Number of nodes that need to reply to a write operation
    static int R = 2;  //Number of nodes that need to reply to a read operation

    static HashMap<Node,String> nodelist = new HashMap<Node,String>();
    static HashMap<String, Node> namelist = new HashMap<String, Node>();
    static ConsistentHashTable chash = null;

    //key => (value, metadata): divideded into two HashMap or use tuples/class
    //https://stackoverflow.com/questions/4956844/hashmap-with-multiple-values-under-the-same-key
    HashMap<String, String> local_store = new HashMap<String, String>();
    //TreeMap<String, Integer>
    HashMap<String, VectorClock> local_store_meta = new HashMap<String, VectorClock>();

    //to be done
    HashMap<Integer, HashSet<Node>> pending_put_rsp = new HashMap<Integer, HashSet<Node>>();  // seqno => set of nodes that have stored
    HashMap<Integer, Message> pending_put_msg = new HashMap<Integer, Message>();  // seqno => original client message
    HashMap<Integer, HashSet<ResponseTriples>> pending_get_rsp = new HashMap<Integer, HashSet<ResponseTriples>>();  //seqno => set of (node, value, metadata) tuples
    HashMap<Integer, Message> pending_get_msg = new HashMap<Integer, Message>();  // seqno => original client message

    //# seqno => set of requests sent to other nodes, for each message class ???
    HashMap<Integer, HashSet<PutReq>> pending_put_req = new HashMap<Integer, HashSet<PutReq>>();
    HashMap<Integer, HashSet<GetReq>> pending_get_req = new HashMap<Integer, HashSet<GetReq>>();

    Set<Node> failed_nodes = new LinkedHashSet<Node>();

    HashMap<Node, HashSet<String>> pending_handoffs = new HashMap<Node, HashSet<String>>();

    Deque<Message> queue = new ConcurrentLinkedDeque<Message>();

    /******/
    //each node has a list of heartbeats record
    HashMap<String, HeartBeat> heartBeats = new HashMap<>();

    //heartbeat receive from others
    Queue<HashMap<String, HeartBeat>> pengding_get_hb = new LinkedList<>();
    HashMap<String, Node> healthylist = new HashMap<String, Node>();
    public boolean healthstatus = true;
    int count = 0;

    //receive heartbeat from other nodes
    public void pull_HeartBeat(){
        while (true) {
            if (this.healthstatus) {
                System.out.println(this.name + " now checking income info");
                if (!pengding_get_hb.isEmpty()) {

                    HashMap<String, HeartBeat> receiving = pengding_get_hb.remove();

                    for (String node : receiving.keySet()) {
                        if (!healthylist.containsKey(node)) {
                            healthylist.put(node, (receiving.get(node)).from_node);
                        }
                    }

                    updateHeartBeat(receiving);

                    for (String s : namelist.keySet()) {
//                        if (!healthylist.containsKey(s)) {
//                            healthylist.put(s, (receiving.get(s)).from_node);
//                            continue;
//                        }
                        if(receiving.get(s) != null) {
                            if (((DynamoNode2) (receiving.get(s)).from_node).healthstatus == true) {
                                System.out.println(this.name + " get heart beats from " + receiving.get(s).from_node);
                            } else {
                                healthylist.remove((receiving.get(s)).from_node);
                            }
                        }
                    }
                } else {
                    try {
                        sleep(1000);
                    } catch (Exception e) {
                    }
                }
            }
        }
    }

    //sending heartbeat
    public void push_HeartBeat(){

        while(true) {
        //if(this.healthstatus || count == 0) {
            try {
                sleep(1000);

            } catch (Exception e) {
            }

            if(!this.healthstatus && count > 1) continue;
            DynamoNode2 destnode;
            Random rand = new Random();

            if (count == 0) {
                int randNo = rand.nextInt(nodelist.size());
                //do {
                while ((String.valueOf((char) ('A' + randNo))).equals(this.name)) {
                    randNo = rand.nextInt(nodelist.size());
                }

                destnode = (DynamoNode2) name_node.get(String.valueOf((char) ('A' + randNo)));
                //} while(!destnode.healthstatus);
                destnode.pengding_get_hb.add(heartBeats);
                System.out.println(this.name + " first send to " + destnode);

                try {
                    count++;
                    sleep(1000);
                } catch (Exception e) {

                }
                //continue;
            } else if(healthstatus){

                int randNo = rand.nextInt(healthylist.size());
                System.out.println(this.name + " healthlist: " + healthylist.entrySet());
                //do {
                while ((String.valueOf((char) ('A' + randNo))).equals(this.name)) {
                    randNo = rand.nextInt(healthylist.size());
                }

                destnode = (DynamoNode2) healthylist.get(String.valueOf((char) ('A' + randNo)));
                //} while(!destnode.healthstatus);

                //healthy node report its new heartbeat
                if (destnode!=null &&destnode.healthstatus) {
                    HeartBeat newhb = new HeartBeat(this, destnode, this.name);
                    newhb.version = heartBeats.get(this.name).version + 1;
                    heartBeats.put(this.name, newhb);
                    System.out.println("gossiping - push heartbeat from node " + this.name + " to node " + destnode.name);
                    destnode.pengding_get_hb.add(heartBeats);
                }
            }
        }
    }

    public void updateHeartBeat(HashMap<String, HeartBeat> getHbs) {
        for (String s: name_node.keySet()) {
            HeartBeat received_hb = getHbs.get(s);

            if(received_hb != null) {

                if( !heartBeats.containsKey(s)){
                    heartBeats.put(s,received_hb);
                }

                if(!healthylist.containsKey(received_hb.aboutNode) && received_hb.aboutNode_status){
                    healthylist.put(s,received_hb.from_node);
                }

                HeartBeat myRecord_hb = heartBeats.get(s);
                if (myRecord_hb == null ||received_hb.version >= myRecord_hb.version) {
                    //update only newer version or first heartbeat
                    heartBeats.put(s, received_hb);

                    if (!received_hb.aboutNode_status) {
                        System.out.println("get   "+ received_hb.aboutNode +"   "+received_hb.aboutNode_status);
                        if(healthylist.containsKey(received_hb.aboutNode)) {
                            healthylist.remove(received_hb.aboutNode);
                        }
                        System.out.println(this.name + " noticed " + received_hb.aboutNode + " dies");
                    }
                }

                String res = (received_hb.aboutNode_status) ? "alive" : "died";
                System.out.println("gossiping - " + this.name + " knows about " + received_hb.aboutNode + " is " + res
                        + " - timestamp " + received_hb.version);

            }
        }
    }

    public void gererateheartBeat(){
        HeartBeat first = new HeartBeat(this, null, this.name);
        first.aboutNode = this.name;
        first.aboutNode_status = this.healthstatus;
        first.status = this.healthstatus;
        heartBeats.put(this.name,first);
        if(healthstatus) {
            healthylist.put(super.name, this);
        }
        System.out.println("*******\n" + this.name+ " starts "+" status : "+ this.healthstatus + "\n*****");
        System.out.println(this.name+" 's healthlist: "+ healthylist.entrySet().toString());
    }


    DynamoNode2()
    {
        super();
        nodelist.put(this, super.name);
        namelist.put(super.name, this);
        chash = new ConsistentHashTable(new ArrayList<String>(namelist.keySet()), T);
        retry_failed_node("retry");


        if(healthstatus){
            Thread t2 = new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        sleep(2000);
                        pull_HeartBeat();
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    }
                }
            });

            t2.start();
            System.out.println(this.name+ "'s checking thread starts");
        }
    }

    @Override
    public int getNodePriority()
    {
        return timer_priority;
    }

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

    // Permanently repeating timer
    public void retry_failed_node(String reason) {
        if(failed_nodes.size() > 0)
        {
            //set.toArray()[ set.size()-1 ]
            Node node = (Node)(failed_nodes.toArray()[0]);

            //# Send a test message to the oldest failed node ???
            PingReq pingmsg = new PingReq(this, node);
            Framework.send_message(pingmsg);
        }

        //# Restart the timer: to be done
    }

    public void rcv_pingreq(PingReq pingmsg)
    {
        //# Always reply to a test message
        PingRsp pingrsp = new PingRsp(pingmsg);
        Framework.send_message(pingrsp);
    }

    public void rcv_pingrsp(PingRsp pingmsg)
    {
        Node recovered_node = pingmsg.from_node;

        if(failed_nodes.contains(recovered_node))
        {
            failed_nodes.remove(recovered_node);
        }

        if(pending_handoffs.containsKey(recovered_node))
        {
            HashSet<String> keySet = pending_handoffs.get(recovered_node);
            for(String key: keySet)
            {
                //Send our latest value for this key
                String value = retrieve(key);
                VectorClock metadata = retrieveMeta(key);

                //to be done: how about msg_id???
                PutReq putmsg = new PutReq(this, recovered_node, key, value, metadata);
                Framework.send_message(putmsg);
            }
            pending_handoffs.remove(recovered_node);
        }
    }

    public ArrayList<Node> find_preference_list(String key)
    {
        Set<String> failed_nodes_name = new LinkedHashSet<String>();

        ArrayList<String> preference_list_name = null;
        ArrayList<Node> preference_list = new ArrayList<Node>();

        for(Node node:failed_nodes) {
            failed_nodes_name.add(nodelist.get(node));
        }

        preference_list_name = chash.find_nodes(key, N, failed_nodes_name); //to be done
        for(String name : preference_list_name)
        {
            preference_list.add(namelist.get(name));
        }

        return preference_list;
    }

    public ArrayList<Node> find_avoided_list(String key)
    {
        Set<String> failed_nodes_name = new LinkedHashSet<String>();

        ArrayList<String> avoided_list_name = null;
        ArrayList<Node> avoided_list = new ArrayList<Node>();

        for(Node node:failed_nodes) {
            failed_nodes_name.add(nodelist.get(node));
        }

        avoided_list_name = chash.find_nodes_avoided(key, N, failed_nodes_name); //to be done
        for(String name : avoided_list_name)
        {
            avoided_list.add(namelist.get(name));
        }

        return avoided_list;
    }

    public void retry_request(DynamoRequestMessage reqmsg)
    {
        //??? need check???
        if(reqmsg instanceof DynamoRequestMessage)
        {
            ArrayList<Node> preference_list = find_preference_list(reqmsg.key);

            if(reqmsg instanceof PutReq)
            {
                if(pending_put_req.containsKey(reqmsg.msg_id)) {

                    ArrayList<Node> current_list = new ArrayList<Node>();
                    HashSet<PutReq> putReqSet = pending_put_req.get(reqmsg.msg_id);

                    for(PutReq req : putReqSet) {
                        current_list.add(req.to_node);
                    }

                    for(Node node : preference_list) {
                        if(current_list.contains(node)) {
                            continue;
                        }
                        else {
                            PutReq newreqmsg = new PutReq(this, node, reqmsg.key,
                                    ((PutReq)reqmsg).value, ((PutReq)reqmsg).metadata,
                                    reqmsg.msg_id, ((PutReq)reqmsg).handoff);
                            pending_put_req.get(reqmsg.msg_id).add(newreqmsg);
                            Framework.send_message(newreqmsg);
                            break; //??to be done??
                        }
                    }
                }
            }
            else if(reqmsg instanceof GetReq)
            {
                if(pending_get_req.containsKey(reqmsg.msg_id)) {

                    ArrayList<Node> current_list = new ArrayList<Node>();
                    HashSet<GetReq> getReqSet = pending_get_req.get(reqmsg.msg_id);

                    for(GetReq req : getReqSet) {
                        current_list.add(req.to_node);
                    }

                    for(Node node : preference_list) {
                        if(current_list.contains(node)) {
                            continue;
                        }
                        else {
                            GetReq newreqmsg = new GetReq(this, node, reqmsg.key, reqmsg.msg_id);
                            pending_get_req.get(reqmsg.msg_id).add(newreqmsg);
                            Framework.send_message(newreqmsg);
                            break; //??to be done??
                        }
                    }
                }
            }
            else {
                //error: to be done
            }
        }
    }

    //to be done
    public void rcv_clientput(ClientPut msg)
    {
        ArrayList<Node> preference_list = find_preference_list(msg.key); //to be done
        ArrayList<Node> avoided_list = find_avoided_list(msg.key); //???
        ArrayList<Node> avoided = new ArrayList<Node>();
        ArrayList<Node> handoff;
        int count, non_extra_count, reqcount;

        count = 0;
        for(Node node: avoided_list)
        {
            avoided.add(node);
            count++;
            if(count >= N)
                break;
        }
        non_extra_count = N - avoided.size();

        if(preference_list.contains(this))
        {
            int seqno = generate_sequence_number();

            //The metadata for a key is passed in by the client, and updated by the coordinator node.
            VectorClock metadata;
            if(msg.metadata == null) {
                metadata = new VectorClock();
            }
            else {
                metadata = msg.metadata.deepCopy();
            }
            metadata.update(name, seqno);

            //Send out to preference list, and keep track of who has replied
            pending_put_req.put(seqno, new HashSet<PutReq>());
            pending_put_rsp.put(seqno, new HashSet<Node>());
            pending_put_msg.put(seqno, msg);

            reqcount = 0;
            for(Node node : preference_list) {

                if(reqcount >= non_extra_count) {
                    handoff = avoided;
                }
                else {
                    handoff = null;
                }
                //Send message to get node in preference list to store
                PutReq putmsg = new PutReq(this, node, msg.key, msg.value, metadata, seqno, handoff);

                pending_put_req.get(seqno).add(putmsg);
                Framework.send_message(putmsg);

                reqcount = reqcount + 1;
                if(reqcount >= N)
                    break;
            }

        }
        else {
            // Forward to the coordinator for this key
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

            pending_get_req.put(seqno, new HashSet<GetReq>());
            pending_get_rsp.put(seqno, new HashSet<ResponseTriples>());
            pending_get_msg.put(seqno, msg);

            int reqcount = 0;
            for(Node node : preference_list) {
                //# Send message to get node in preference list to store
                GetReq getmsg = new GetReq(this, node, msg.key, seqno);
                pending_get_req.get(seqno).add(getmsg);
                Framework.send_message(getmsg);

                reqcount = reqcount + 1;
                if(reqcount >= N)
                    break;
            }
        }
        else {
            // Forward to the coordinator for this key
            Node coordinator = preference_list.get(0);
            Framework.forward_messsage(msg, coordinator);
        }

    }

    public void rcv_put(PutReq putmsg)
    {
        store(putmsg.key, putmsg.value, putmsg.metadata);

        if((putmsg.handoff != null) && (putmsg.handoff.size() > 0))
        {
            for(Node failed_node : putmsg.handoff)
            {
                failed_nodes.add(failed_node);

                if(!pending_handoffs.containsKey(failed_node))
                {
                    pending_handoffs.put(failed_node, new HashSet<String>());
                }

                pending_handoffs.get(failed_node).add(putmsg.key);
            }
        }

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

                //# Tidy up tracking data structures
                ClientPut original_msg = (ClientPut)pending_put_msg.get(seqno);
                pending_put_req.remove(seqno);
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
                //# Build up all the distinct values/metadata values for the response to the original request
                HashSet<ResponseTuples> results = new HashSet<ResponseTuples>();
                for(ResponseTriples item : responseSet) {

                    ResponseTuples value_metadata = new ResponseTuples(item.value, item.metadata);
                    results.add(value_metadata);
                }

                //# Coalesce all compatible (value, metadata) pairs across the responses
                results =  VectorClock.coalesce2(results);

                //# Tidy up tracking data structures
                ClientGet original_msg = (ClientGet)pending_get_msg.get(seqno);
                pending_get_req.remove(seqno);
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

    //API for other threads to send message
    @Override
    public void sndmsg(Message msg)
    {
        queue.add(msg);
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
        else if(msg instanceof PingReq)
        {
            rcv_pingreq((PingReq)msg);
        }
        else if(msg instanceof PingRsp)
        {
            rcv_pingrsp((PingRsp)msg);
        }
        else
        {
            System.out.println("Unexpected message type " + msg.getClass().getSimpleName());
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

    public void run(){
        System.out.println(System.currentTimeMillis() + ": Thread:" + this.name +"(" + this.getClass().getSimpleName() + ")" + " is running...");
        gererateheartBeat();
        push_HeartBeat();

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
