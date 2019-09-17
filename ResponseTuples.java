package dynamo;

public class ResponseTuples {
    String value;
    VectorClock metadata;
    
    public ResponseTuples(String value, VectorClock metadata)
    {
    	this.value = value;
    	this.metadata = metadata;       	
    }
    
    //public String toString(){
    	//StringBuffer sbuffer = new StringBuffer();
        //String s = sbuffer.append("name=").append(name).append(", seqno=").append(seqno).toString();
        //return s;
    //}
}
