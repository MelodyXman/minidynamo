package dynamo;

public class HeartMessage extends Message{

    boolean aboutNodeStatus;
    String aboutNode;
    public int heartmsg_id = 0;
    static int msg_id = 0;
    public int forwardtimes;


    public HeartMessage(Node from_node, Node to_node, String aboutNode, boolean status){
        super(from_node, to_node);
        this.aboutNode = aboutNode;
        this.aboutNodeStatus = status;
        msg_id++;
        heartmsg_id = msg_id;
        this.forwardtimes = 0;
    }
}
