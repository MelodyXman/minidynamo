package dynamo;

import java.util.*;


//Base class for Dynamo response messages; all include key and value (plus metadata)
class DynamoResponseMessage extends ResponseMessage {
    String key;
    ArrayList<String> value;                
    ArrayList<VectorClock>  metadata; 
    
    boolean _show_metadata = true;
    
    public DynamoResponseMessage(DynamoRequestMessage req, String value, VectorClock metadata){
        super(req);
        this.key = req.key;
        
        this.value = new ArrayList<String>();
        this.metadata = new ArrayList<VectorClock>();
        this.value.add(value);
        if(metadata != null) {
        	this.metadata.add(metadata);
        }
    }
    
    public DynamoResponseMessage(DynamoRequestMessage req, ArrayList<String> value, ArrayList<VectorClock> metadata){
        super(req);
        this.key = req.key;
        this.value = value;
        this.metadata = metadata;
    }
    
    public String _show_value(){
    	StringBuffer sbuffer = new StringBuffer();
    	
        if (_show_metadata) {
            try{
                return sbuffer.append(Arrays.toString(value.toArray())).append("@[").append(Arrays.toString(metadata.toArray())).append(']').toString();
            } catch (Exception e){
                return sbuffer.append(Arrays.toString(value.toArray())).append('@').append(metadata).toString();
            }
        } else{  
            return sbuffer.append(Arrays.toString(value.toArray())).toString();           
        }
    }   
    
    @Override
    public String toString(){
    	StringBuffer sbuffer = new StringBuffer();
        String s = sbuffer.append(this.getClass().getName()).append('(').append(this.key).append("=")
                .append( _show_value()).append(')').toString();
        return s;
    }


}