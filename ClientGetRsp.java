package dynamo;

import java.util.*;

public class ClientGetRsp extends DynamoResponseMessage {
	
    public ClientGetRsp(DynamoRequestMessage req, String value, VectorClock metadata){
        super(req, value, metadata);
    }
    
    public ClientGetRsp(DynamoRequestMessage req, ArrayList<String> value, ArrayList<VectorClock> metadata){
        super(req, value, metadata);
    }
}