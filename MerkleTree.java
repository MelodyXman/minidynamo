package dynamo;

import java.io.File;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.*;

//Build a Merkle tree of given depth covering keys in range [min_key, max_key)
public class MerkleTree {

    int depth;
    long value;
    long min_key;
    long max_key;

    int num_leaves;
    int leaf_size;

    ArrayList<ArrayList<MerkleTreeNode>> nodes;

    MerkleTreeNode root;
    long hashval;

    public MerkleTree(int depth , long min_key, long max_key){
        this.min_key = 0 ;
        this.max_key = (long)Math.pow(2,128) - 1 ;
        this.depth = 12;

        this.num_leaves = (int)Math.pow(2,depth);
        this.leaf_size = (int)((max_key - min_key) + num_leaves -1) /num_leaves;

        //nodes is an array of (depth+1) lists; each list is a layer of the tree
        nodes = new ArrayList<ArrayList<MerkleTreeNode>>();

        //layer 0 (bottom) of the tree is (2^depth) leaf nodes
        ArrayList<MerkleTreeNode> layer = new ArrayList<>();
        for(int i = 0 ; i < num_leaves; i++) {
            layer.add(new MerkleLeaf(this.min_key + i * leaf_size,
                        Math.min(min_key + (i + 1) * leaf_size, max_key), null));
        }
        nodes.add(layer);



        //Each layer >= 1 consists of interior nodes, and is half the size
        //of the layer below.  Each interior node is built from two nodes below it

        int level = 1;
        while (level <= depth) {
            ArrayList<MerkleBranchNode> branches = new ArrayList<MerkleBranchNode>();
            for(int i = 0; i < nodes.get(level - 1).size() / 2; i++) {
                MerkleBranchNode node = new MerkleBranchNode(nodes.get(level - 1).get(2 * i), nodes.get(level - 1).get(2 * i + 1));
                branches.add(node);
            }
            ArrayList<MerkleTreeNode> temp = new ArrayList<>(branches);
            nodes.add(temp);
        }
        this.root = nodes.get(nodes.size() - 1).get(0);

    }

    public MerkleTree(int depth , long min_key, long max_key, HashMap<String, String> initdata){
        this.depth = (int)Math.ceil(Math.log(initdata.size() / Math.log(2)));
        this.num_leaves = initdata.size();
        this.leaf_size = (int)((max_key - min_key) + num_leaves -1) /num_leaves;

        //nodes is an array of (depth+1) lists; each list is a layer of the tree
        nodes = new ArrayList<ArrayList<MerkleTreeNode>>();

        //layer 0 (bottom) of the tree is (2^depth) leaf nodes
        ArrayList<MerkleTreeNode> layer = new ArrayList<>();

        int leafno = 0;
        for(Map.Entry<String,String> entry : initdata.entrySet()) {
            HashMap<String, String> datavalue = new HashMap<>();
            datavalue.put(entry.getKey(), entry.getValue());

            MerkleLeaf leaf = new MerkleLeaf(this.min_key + leafno * leaf_size,
                        Math.min(min_key + (leafno+ 1) * leaf_size, max_key), datavalue);
            layer.add(leaf);
//            System.out.println("leaf "+ leafno +"  "+ leaf.min_key + " -- " + leaf.max_key + ": " + leaf.value);
            leafno++;
        }
        nodes.add(new ArrayList<>(layer));

        //Each layer >= 1 consists of interior nodes, and is half the size
        //of the layer below.  Each interior node is built from two nodes below it

        int level = 1;
        while (level <= this.depth) {
            ArrayList<MerkleBranchNode> branches = new ArrayList<MerkleBranchNode>();
            int size = nodes.get(level - 1).size() /2;
            for(int i = 0; i < size ; i++) {
                MerkleBranchNode node;
                    node = new MerkleBranchNode(nodes.get(level - 1).get(2 * i),
                             nodes.get(level - 1).get(2 * i + 1));
                branches.add(node);

               // System.out.println("node "+ node.min_key + " -- " + node.max_key + ": "+node.value );
              //  System.out.println("node.left "+ nodes.get(level - 1).get(2 * i).value + "\t\t\t" + "node.right " + nodes.get(level - 1).get(2 * i + 1).value);
            }
            ArrayList<MerkleTreeNode> temp = new ArrayList<>(branches);
            nodes.add(temp);
            level++;
        }

        this.root = nodes.get(nodes.size() - 1).get(0);

    }


    //PART keyhash
    public long keyhash(long key){
        long hashval = 0l;
        StringBuffer sb = new StringBuffer();
        String hexString = getmd5Hex(String.valueOf(key));
        //hexStirng to long
        BigInteger bi = new BigInteger(hexString, 16);
        hashval = bi.longValue();
        return hashval;
    }

    public String getmd5Hex(String key){
        StringBuffer sb = new StringBuffer();
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] resultByte = md.digest((key + "").getBytes());
            for (int i = 0; i < resultByte.length; i++) {
                sb.append(Integer.toString((resultByte[i] & 0xff) + 0x100, 16).substring(1));
            }
            return sb.toString();
        }catch (java.security.NoSuchAlgorithmException e){

        }
        return "";
    }

    //Return the index of the leaf node corresponding to the given key
    public int findleaf(long key) {
        hashval = keyhash(key);

        if(hashval < min_key || hashval >= max_key) {
            System.out.println(String.format("Key %s hashes to value outside range for this tree" ,key));
        }
        return (int)hashval / leaf_size;
    }

    public void setitem(long key, String value) {
        int leafindex = findleaf(key);
        nodes.get(0).get(leafindex).data.put(key,value);
        nodes.get(0).get(leafindex).recalc();
    }

    public void delitem(long key){
        int leafindex = findleaf(key);
        nodes.get(0).get(leafindex).data.remove(key);
        nodes.get(0).get(leafindex).recalc();
    }

    public String getitem(long key){
        int leafindex = findleaf(key);
        return nodes.get(0).get(leafindex).data.get(key);
    }

    public boolean contains(long key){
        int leafindex = findleaf(key);
        return nodes.get(0).get(leafindex).data.containsKey(key);
    }

    public Set<Long> keys(){
        Set<Long> results = new HashSet<>();
        for(int leafindex = 0; leafindex < num_leaves;leafindex++){
            Set<Long> sets = new HashSet<>(nodes.get(0).get(leafindex).data.keySet());
            results.addAll(sets);
        }
        return results;
    }

    public void iter() {
    //todo
    }
    public void iteritems(){
    //todo
    }

    public String toString(){
        StringBuffer sb = new StringBuffer();
        for(int level = depth; level >= 0 ; level--) {
            sb.append("level: " + level + "  " );
            ArrayList<MerkleTreeNode> list = nodes.get(level);
            for(MerkleTreeNode node : list){
                sb.append(node.toString() + " ");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

}


