package dynamo;

//Base type for messages that are replies to existing messages
public class ResponseMessage extends Message{

    //String value;// in class DynamoResponseMessage
    //List<String> metadata;//in class DynamoResponseMessage
    Message response_to;

    public ResponseMessage(Message req){
    	super(req.to_node, req.from_node, req.msg_id);
        this.response_to = req;
    }
}
