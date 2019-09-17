package dynamo;

public class Message {
//Base type for messages between Nodes
     Node from_node = null;
     Node to_node = null;
     int msg_id = -1;
     
     Node intermediate_node = null;
     Message original_msg = null;

    public Message () {
    }

    public Message(Node from_node, Node to_node){
        this(from_node, to_node, -1);
    }

    public Message(Node from_node, Node to_node, int msg_id){
        this.from_node = from_node;
        this.to_node = to_node;
        this.msg_id = msg_id;
    }

        public String toString() {
        return this.getClass().getName();
    }
}