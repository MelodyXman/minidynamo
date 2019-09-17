package dynamo;

public class ResponseTriples {
    Node node;
    String value;
    VectorClock metadata;
    
    public ResponseTriples(Node node,  String value, VectorClock metadata)
    {
    	this.node = node;
    	this.value = value;
    	this.metadata = metadata;       	
    }
    
    //public String toString(){
    	//StringBuffer sbuffer = new StringBuffer();
        //String s = sbuffer.append("name=").append(name).append(", seqno=").append(seqno).toString();
        //return s;
    //}
}
