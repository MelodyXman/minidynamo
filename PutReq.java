package dynamo;

import java.util.*;

public class PutReq extends DynamoRequestMessage {
	
	String value;                
	VectorClock  metadata;
	ArrayList<Node> handoff = null;
	
	boolean _show_metadata = true;	
    
	public PutReq(Node from_node, Node to_node, String key, String value, VectorClock metadata) {
		this(from_node, to_node, key, value, metadata, -1, null);
	}
    public PutReq(Node from_node, Node to_node, String key, String value, VectorClock metadata, 
    		      int msg_id, ArrayList<Node> handoff) {
        super(from_node, to_node, key, msg_id);
        this.value = value;
        this.metadata = metadata;
        this.handoff = handoff;
    }

    public String _show_value(String value, VectorClock metadata){
    	StringBuffer sbuffer = new StringBuffer();
    	        
        if (_show_metadata) {
            if(metadata == null) {
            	return value; 
            }
            else {
            	return sbuffer.append(value).append('@').append(metadata.toString()).toString();
            } 
        } else{  
            return value;           
        }
    }   
    
    public String toString(){
    	StringBuffer sbuffer = new StringBuffer();
        if (handoff == null) {
            return sbuffer.append("PutReq(").append(key).append('=').append(_show_value(this.value, this.metadata))
                    .append(')').toString();
        } else {
            return sbuffer.append("PutReq(").append(key).append('=').append(_show_value(this.value, this.metadata))
                    .append(", handoff=(").append(Arrays.toString(handoff.toArray())).append("))").toString();
        }
    }
    
    
    
}