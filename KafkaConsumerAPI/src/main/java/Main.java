public class Main {

    public static void main(String[] args) {
        String topicName="kafka-test-topic";

        // autoCommit is true, kafka automatically commits the line it reads with this consumer
        Boolean autoCommit=true;

        //offsetConfig -> earliest : consumer reads from the beginning of the topic
        //offsetConfig -> latest : consumer reads the topic from the last commit
        String offsetConfig = "latest";

        String headerType = "TeestHeaderEvent";

        //groupID -> The client created with the consumer api connects with this name. If the client is not closed, a client with the same name cannot be created.
        String groupID = "ConsumerTest";

        //ipAddress -> Broker ip address
        String ipAddress = "192.168.1.87:9092,192.168.168.1.88";

        ConsumerService consumerService = new ConsumerService();
        System.out.println(consumerService.run(topicName,autoCommit,offsetConfig,headerType,groupID,ipAddress));


    }
}
