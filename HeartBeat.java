package dynamo;

public class HeartBeat extends Message {

    String from;
    String aboutNode;
    boolean aboutNode_status;
    int priority;
    public int version = 1;
    boolean status;

    public HeartBeat(){}
    public HeartBeat(Node from_node, Node to_node ,String about) {
        super(from_node, to_node);
        this.aboutNode_status = true;
        this.aboutNode = about;
        this.priority = 10;
        this.from = from_node.name;
        this.status = true;
    }

    public HeartBeat(Node from_node, Node to_node, int priority) {
        super(from_node, to_node);
        this.priority = priority;
    }
}
