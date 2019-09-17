package dynamo;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//Leaf node in Merkle tree, encompassing all keys in subrange [min_key, max_key)
class MerkleLeaf extends MerkleTreeNode {

    public MerkleLeaf(long min_key, long max_key) {
        //super();
        this.min_key = min_key;
        this.max_key = max_key;
        data = new HashMap<>();

        this.value = "";
    }


    public MerkleLeaf(long min_key, long max_key, HashMap<String,String> initdata) {
        super();
        this.min_key = min_key;
        this.max_key = max_key;


        //Copy in any keys whose hash falls in range for this node
        if(initdata != null){
            data = new HashMap<>();
            for(Map.Entry entry : initdata.entrySet()){
                data.put(Long.decode((String)entry.getKey()), (String)entry.getValue());
            }
        }

        String hexString = getmd5Hex(String.valueOf(data.entrySet()));
        //hexStirng to long
        BigInteger bi = new BigInteger(hexString, 16);
        this.value = bi.toString();

    }

    public String toString(){
        return String.format("[%s,%s)=>%s",this.min_key, this.max_key, this.value);
    }

    //Determine whether the given key falls within the subrange of this leaf node
    public boolean _inrange(long key) {

        long hashval = keyhash(key);
        return hashval >= min_key && hashval < max_key;
    }


    //Recalculate the Merkle value for this node, and all parent nodes
    public void recal(){
        this.value = getmd5Hex(data.entrySet().toString());
        this.parent.recalc();
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
}
