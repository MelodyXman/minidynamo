package dynamo;

import java.io.File;
import java.security.MessageDigest;
import java.util.*;

//Minimal Merkle Tree implementation


class MerkleTreeNode {
    String value;
    long min_key;
    long max_key;
    MerkleTreeNode parent;
    HashMap<Long, String> data;

    public MerkleTreeNode(){
        this.value = null;
        this.parent = null;
    }

    public MerkleTreeNode(MerkleTreeNode node){
        this.value = node.value;
        this.parent = node.parent;
        this.max_key = node.max_key;
        this.min_key = node.min_key;
        if(node.data != null)
            this.data = new HashMap<>(node.data);
    }

    public void recalc(){
    }
}





