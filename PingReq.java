package dynamo;

public class PingReq extends Message {
	
    public PingReq(Node from_node, Node to_node){
        this(from_node, to_node, -1);
    }

    public PingReq(Node from_node, Node to_node, int msg_id){
    	super(from_node,to_node, msg_id);
    }

}
