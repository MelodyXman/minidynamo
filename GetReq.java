package dynamo;

public class GetReq extends DynamoRequestMessage{
    public GetReq(Node from_node, Node to_node, String key) {
        this(from_node, to_node, key, -1);
    }

    public GetReq(Node from_node, Node to_node, String key, int msg_id) {
        super(from_node, to_node, key, msg_id);
    }
}