package dynamo;

import java.util.*;

public class GetRsp extends DynamoResponseMessage{
	    public GetRsp(DynamoRequestMessage req, String value, VectorClock metadata){
	        super(req, value, metadata);
	    }
}