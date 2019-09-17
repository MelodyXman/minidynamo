package dynamo;


//Base class for Dynamo request messages; all include the key for the data object in question
class DynamoRequestMessage extends Message {

    String key;
        
    public DynamoRequestMessage(Node from_node, Node to_node, String key) {
        this(from_node, to_node, key, -1);
    }

    public DynamoRequestMessage(Node from_node, Node to_node, String key, int msg_id) {
        super(from_node, to_node, msg_id);
        this.key = key;
    }

    //to be done
    @Override
    public String toString() {
        return this.getClass().getName() +"(key=" + this.key +")";
    }
}