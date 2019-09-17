package dynamo;

import java.util.ArrayList;
import java.util.Random;

/*
 * Test code for Dynamo
 */
public class SimpleTestCase {
	
	public static void main (String[] args) {
		
		testSimplePut();
		testSimpleGet();
		tearDown();

	}
	

	public static void sleep(int second)
	{
        try{
        	Thread.sleep(second*1000);
        }
   	    catch(InterruptedException e)
   	    {
   	    	System.out.println(e);
   	    }  		
	}
	
	public static void resetAll() {
		Framework.reset_all();
	}
	
	public static void setUp() {
		resetAll();
		DynamoNode2.reset();
		DynamoNode1.reset();
		DynamoNode.reset();
	}
	
	public static void tearDown() {
		resetAll();
	}
	
	public static void testSimplePut() {
		setUp();
		
		System.out.println("\n-----------------Start Put TestCase-----------------\n");
		
		//Enable threads
		Framework.enableThreads = true;
		
		DynamoNode dynamoNode1 = new DynamoNode();
		DynamoNode dynamoNode2 = new DynamoNode();
		DynamoNode dynamoNode3 = new DynamoNode();
		DynamoNode dynamoNode4 = new DynamoNode();
		DynamoNode dynamoNode5 = new DynamoNode();
		DynamoNode dynamoNode6 = new DynamoNode();
		ClientNode a = new ClientNode(DynamoNode.nodelist, "a");
		
		System.out.println("\n" + "Start threads");
		
		dynamoNode1.start();
		dynamoNode2.start();
		dynamoNode3.start();
		dynamoNode4.start();
		dynamoNode5.start();
		dynamoNode6.start();
		a.start();
		
		sleep(1);
		System.out.println("\n" + "Start Put");
		
        a.put("K1", (VectorClock)null, "1");
        
        sleep(8);
        System.out.println("\n" + "Terminate threads");
        
        dynamoNode1.interrupt();
        dynamoNode2.interrupt();
        dynamoNode3.interrupt();
        dynamoNode4.interrupt();
        dynamoNode5.interrupt();
        dynamoNode6.interrupt();
        a.interrupt();
        
        try {
            dynamoNode1.join();
            dynamoNode1.join();
            dynamoNode1.join();
            dynamoNode1.join();
            dynamoNode1.join();
            dynamoNode1.join();
            a.join();        	
        }
   	    catch(InterruptedException e)
   	    {
   	    	System.out.println(e);
   	    }  
        
        sleep(1);
	}
	
	
	public static void testSimpleGet() {
		setUp();
	
		System.out.println("\n-----------------Start Get TestCase-----------------\n");
		//Enable threads
		Framework.enableThreads = true;
		
		DynamoNode dynamoNode1 = new DynamoNode();
		DynamoNode dynamoNode2 = new DynamoNode();
		DynamoNode dynamoNode3 = new DynamoNode();
		DynamoNode dynamoNode4 = new DynamoNode();
		DynamoNode dynamoNode5 = new DynamoNode();
		DynamoNode dynamoNode6 = new DynamoNode();
		ClientNode a = new ClientNode(DynamoNode.nodelist, "a");
		
		System.out.println("\n" + "Start threads");
		
		dynamoNode1.start();
		dynamoNode2.start();
		dynamoNode3.start();
		dynamoNode4.start();
		dynamoNode5.start();
		dynamoNode6.start();
		a.start();
		
		sleep(1);
		
		System.out.println("\n" + "Start Put");		
        a.put("K1", (VectorClock)null, "1");
        sleep(8);
        
        System.out.println("\n" + "Start Get");
        a.get("K1");
        sleep(8);
        
        System.out.println("\n" + "Terminate threads");

        dynamoNode1.interrupt();
        dynamoNode2.interrupt();
        dynamoNode3.interrupt();
        dynamoNode4.interrupt();
        dynamoNode5.interrupt();
        dynamoNode6.interrupt();
        a.interrupt();
        
        try {
            dynamoNode1.join();
            dynamoNode1.join();
            dynamoNode1.join();
            dynamoNode1.join();
            dynamoNode1.join();
            dynamoNode1.join();
            a.join();        	
        }
   	    catch(InterruptedException e)
   	    {
   	    	System.out.println(e);
   	    }  
        
        sleep(1);        
	}

}