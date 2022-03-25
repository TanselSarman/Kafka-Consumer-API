import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;

public class ConsumerService {
    private volatile boolean closing = false;

    private Consumer<String, String> kafkaConsumer;
    public String run(String topic, Boolean autoCommit, String offsetConfig,String headerTypeID, String groupID, String serverIP) {

        JSONArray jsonArray = new JSONArray();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverIP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetConfig);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        // System.out.println("KafkaConsumer(" + Thread.currentThread().getId() + ") is started.");
        int count = 0;
        try {
            while (!closing) {

                try {
                    // Poll on the Kafka consumer, waiting up to 10 secs if there's nothing to consume.
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(10));
                    if (records.isEmpty()) {
                        //logger.info("No messages consumed");
                        //System.out.println("No messages consumed ");


                        closing = true;
                        kafkaConsumer.wakeup();
                        kafkaConsumer.close();
                    } else {

                        Iterator lineiterator = records.iterator();

                        count = 0;
                        while (lineiterator.hasNext()) {

                            ConsumerRecord currentRecord = (ConsumerRecord) lineiterator.next();
                            RecordHeaders currentRecordHeaders = (RecordHeaders) currentRecord.headers();
                            Header matchingheader = currentRecordHeaders.lastHeader("__TypeId__");
                            String headerResult = new String(matchingheader.value(), StandardCharsets.UTF_8);

                            if (headerResult.equals(headerTypeID) && !headerResult.equals(null)) {

                                String message = currentRecord.value().toString();
                                if(message!=null){

                                    JSONObject jsonObject = new JSONObject(message);
                                    jsonArray.put(jsonObject);
                                }else{

                                    // System.out.println("Null error");
                                }
                            }else{

                                // System.out.println("Null error");
                            }
                            count++;
                        }
                        String message = String.valueOf(jsonArray);

                        return "200" + "|" +message;
                    }
                } catch (final WakeupException e) {
                    //  logger.warn("Consumer closing - caught exception: {}", e);
                    System.out.printf("Consumer closing - caught exception: {}", e, e);

                } catch (final KafkaException e) {
                    //   logger.error("Sleeping for 5s - Consumer has caught: {}", e, e);
                    System.out.printf("Sleeping for 5s - Consumer has caught: {}", e, e);
                    try {
                        Thread.sleep(50); // Longer sleep before retrying
                    } catch (InterruptedException e1) {
                        //      logger.warn("Consumer closing - caught exception: {}", e1);

                        System.out.printf("Consumer closing - caught exception: {}", e, e);
                    }
                }
            }
        } finally {

            kafkaConsumer.close(Duration.ofSeconds(20));
            String message = String.valueOf(jsonArray);
            if(jsonArray.isEmpty()){
                return "500|No messages consumed";
            }else{
                return "200" + "|" + message;
            }
            //  logger.warn("{} has shut down.");
            //System.out.printf("{} has shut down. asd", KafkaConsumerService.class);
            //shutdown();

        }
    }
    public void shutdown() {
        closing = true;
        kafkaConsumer.wakeup();
        kafkaConsumer.close();



    }
}