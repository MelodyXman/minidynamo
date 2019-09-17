package dynamo;

import java.math.BigInteger;
import java.security.MessageDigest;

//Interior node in Merkle tree
class MerkleBranchNode extends MerkleTreeNode {
    MerkleTreeNode left;
    MerkleTreeNode right;

    public MerkleBranchNode(MerkleTreeNode left, MerkleTreeNode right) {
        super();
        this.left = new MerkleTreeNode(left);
        left.parent = this;
        if(right != null) {
            this.value = left.value.concat(right.value);
            this.max_key = (left.max_key > right.max_key) ? left.max_key : right.max_key;
            this.min_key = (left.min_key < right.min_key) ? left.min_key : right.min_key;
            this.right = new MerkleTreeNode(right);
            right.parent = this;
        } else {
            this.value = left.value;
            this.max_key = left.max_key;
            this.min_key = left.min_key;
            this.right = null;
        }

        recalc();
    }


    //Recalculate the Merkle value for this node, and all parent nodes
    public void recalc(){
        //Node value is hash of two children's hash values concatenated
        StringBuffer sb = new StringBuffer();
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] resultByte;
            if(this.right != null) {
                resultByte = md.digest((this.left.value.concat(this.right.value)).getBytes());
            }else {
                resultByte = md.digest((this.left.value).getBytes());
            }
           // System.out.println("node # "+ this.value + "\n\tleft" + this.left.value+ ",, right "+ this.right.value);

            //byte to hex:
            sb.delete(0, sb.length());
            for (int i = 0; i < resultByte.length; i++) {
                sb.append(Integer.toString((resultByte[i] & 0xff) + 0x100, 16).substring(1));
            }
            // hex to long:
            String s = sb.toString();
            BigInteger bi = new BigInteger(s, 16);
            this.value = bi.toString();

        } catch(java.security.NoSuchAlgorithmException e) {
        }

        if (this.parent != null) {
            try {
                this.parent.recalc();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public String toString(){
        return String.valueOf(value).substring(0,16);
    }
}
