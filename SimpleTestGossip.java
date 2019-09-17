package dynamo;

public class SimpleTestGossip {

    public static void main(String[] args) {

//		testGossip();
        testGossip2();

//		gossipWithSeedNode();

    }

	public static void gossipWithSeedNode(){
        System.out.println("\n-----------------Start Gossip TestCase-----------------\n");
        System.out.println("\n" + "Gossip with seed node");

        for (int i = 0; i < 6; i++) {
			new DynamoNode1();
		}
	}

    public static void testGossip() {
        setUp();

        System.out.println("\n-----------------Start Gossip TestCase-----------------\n");
        //Enable threads
        Framework.enableThreads = true;
        System.out.println("\n" + "Start threads");

        //all nodes alive
        DynamoNode2 dynamoNode1 = new DynamoNode2();
        dynamoNode1.start();
        DynamoNode2 dynamoNode2 = new DynamoNode2();
        dynamoNode2.start();
        DynamoNode2 dynamoNode3 = new DynamoNode2();
        dynamoNode3.start();
        DynamoNode2 dynamoNode4 = new DynamoNode2();
        dynamoNode4.start();
        DynamoNode2 dynamoNode5 = new DynamoNode2();
        dynamoNode5.start();
        DynamoNode2 dynamoNode6 = new DynamoNode2();
        dynamoNode6.start();

//
//        dynamoNode1.healthy();
//        dynamoNode2.healthy();
//        dynamoNode3.healthy();
//        dynamoNode4.healthy();
//        dynamoNode5.healthy();
//        dynamoNode6.healthy();

        System.out.println("\n" + "Start with all alived");


    }


    public static void testGossip2() {
        setUp();

        System.out.println("\n-----------------Start Gossip TestCase-----------------\n");
        //Enable threads
        Framework.enableThreads = true;

        //with one failure node
        System.out.println("\n" + "Starts with failure node");
        DynamoNode2 dynamoNode1 = new DynamoNode2();
        DynamoNode2 dynamoNode2 = new DynamoNode2();
        DynamoNode2 dynamoNode3 = new DynamoNode2();
        DynamoNode2 dynamoNode4 = new DynamoNode2();
        DynamoNode2 dynamoNode5 = new DynamoNode2();
        DynamoNode2 dynamoNode6 = new DynamoNode2();

        System.out.println("\n" + "Start threads");

        dynamoNode1.healthstatus = false;
        dynamoNode1.start();

        dynamoNode2.start();
        dynamoNode3.start();
        dynamoNode4.start();
        dynamoNode5.start();
        dynamoNode6.start();




    }

    public static void sleep(int second) {
        try {
            Thread.sleep(second * 1000);
        } catch (InterruptedException e) {
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

}