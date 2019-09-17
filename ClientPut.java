package dynamo;


public class ClientPut extends DynamoRequestMessage {

	String value;                // in class DynamoResponseMessage
	VectorClock  metadata = null;
	boolean _show_metadata = true;
	

	public ClientPut(Node from_node, Node to_node, String key, String value, VectorClock metadata){
        this(from_node, to_node, key, value, metadata, -1);
    }	
	
    public ClientPut(Node from_node, Node to_node, String key, String value, VectorClock metadata, int msg_id){
        super(from_node, to_node, key, msg_id);
        this.value = value;
        if(metadata == null)
        {
        	metadata = new VectorClock();
        }
        else {
        	this.metadata = metadata;
        }
        
    }

    public String toString(){
    	StringBuffer sbuffer = new StringBuffer();
    	String valueMetadata = _show_value(this.value, this.metadata);
        String s = sbuffer.append("ClientPut(").append(this.key).append('=').append(valueMetadata).append(")").toString();
        return s;
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

}