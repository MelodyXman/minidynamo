package dynamo;


class Timer extends Message{
    //Internal message indicating a timer event at a node
    String reason;
    String callback;
    Message msg = null;
    int priority;

    
    public Timer(Node node, String reason, String callback, int priority)
    {
        super(node, node);
        this.reason = reason;
        this.callback = callback;
        this.priority = priority;
    }
}