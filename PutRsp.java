package dynamo;


public class PutRsp extends DynamoResponseMessage{
    public PutRsp(PutReq req){
        super(req, req.value, req.metadata);
    }
}