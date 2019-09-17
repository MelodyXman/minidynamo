package dynamo;

import java.util.*;


public class ClientPutRsp extends DynamoResponseMessage {
	
    public ClientPutRsp(ClientPut reqeust) {
    	this(reqeust, reqeust.metadata);
    }
	
    public ClientPutRsp(ClientPut reqeust, VectorClock metadata) {
        super(reqeust, reqeust.value, metadata);
    }
}