import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class SimpleConsumer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String TOPIC_NAME = "test1";
    private final static String BOOTSTRAP_SERVERS = "public.itseasy.site:10006";
    private final static String GROUP_ID = "test1-group";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(GROUP_ID_CONFIG, GROUP_ID);

        /**
         * 자동 커밋
         */
//        configs.put(ENABLE_AUTO_COMMIT_CONFIG, true);
//        configs.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, 60000);

        /**
         * 수동 커밋
         */
        configs.put(ENABLE_AUTO_COMMIT_CONFIG, false);


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);

        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        /**
         * 동기 커밋
         */
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
//
//            for (ConsumerRecord<String, String> record : records) {
//                logger.info("record:{}", record);
//            }
//
//            consumer.commitSync();
//        }

        /**
         * 비동기 커밋
         */
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("record:{}", record);
            }
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null)
                        System.err.println("Commit failed");
                    else
                        System.out.println("Commit succeeded");

                    if (exception != null)
                        logger.error("Commit failed for offsets {}", offsets, exception);
                }
            });
        }
    }
}
