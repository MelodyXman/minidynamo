package dynamo;

import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Random;

public class MerkleTestCase {
    public static void main(String[] args) {

        testCompare();

        addingValue();

    }

    public static void addingValue() {
        HashMap<String, String> keyStore = new HashMap<>();
        int n = 0;
        while (n < 50) {
            keyStore.put(String.valueOf(n),"this is testdata " + n + "");
            n+=5;
        }
        MerkleTree x0 = new MerkleTree(3,0,20, keyStore);
        MerkleTree x1 = new MerkleTree(3,0,20, keyStore);
        MerkleTreeNode x1t = x1.root;

        System.out.println("\n\n now adding new data in tree x1");
        keyStore.put("10002","asfa");
        x1 = new MerkleTree(3,0,20, keyStore);
        x1t = x1.root;
        System.out.println("*** new tree 1 ***" + x1.getmd5Hex(x1t.value));
        System.out.println(x1.toString());
    }

    public static void testCompare(){
        HashMap<String, String> keyStore = new HashMap<>();
        int n = 0;
        while (n < 50) {
            keyStore.put(String.valueOf(n),"this is testdata " + n + "");
            n+=5;
        }
        MerkleTree x0 = new MerkleTree(3,0,20, keyStore);
        MerkleTree x1 = new MerkleTree(3,0,20, keyStore);



        HashMap<String, String> keyStore2 = new HashMap<>();
        int m = 0;
        while (m < 50) {

            Random rand = new Random();
            int r= 1 + rand.nextInt(100);
            keyStore2.put(r +"","this is testdata " + m + "");
            m += 10;
            //System.out.println("<"+r+", testdata"+m+">");
        }
        MerkleTree x2 = new MerkleTree(6,0,100, keyStore2);

        MerkleTreeNode x0t = x0.root;
        MerkleTreeNode x1t = x1.root;
        MerkleTreeNode x2t = x2.root;

        System.out.println(x0.toString());
        System.out.println("*** tree 0 root hash value ***" + x0.getmd5Hex(x0t.value));
        System.out.println("*** tree 1 root hash value ***" + x1.getmd5Hex(x1t.value));
        System.out.println("tree 0 & tree 1 exactly the same");
        System.out.println("tree 2 & tree 1 are different");
        System.out.println("*** tree 2 root hash value ***" + x2.getmd5Hex(x2t.value));
        System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");


    }

    public static String getmd5Hex(String key){
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
