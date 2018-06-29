import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.concurrent.TimeUnit;

public class MessageProducer {

    private Client client;
    private Producer<byte[]> producer;

    public MessageProducer(String topic) throws PulsarClientException {
        client = new Client();
        producer = createProducer(topic);
    }

    private Producer<byte[]> createProducer(String topic) throws PulsarClientException {
        return client.getPulsarClient().newProducer()
                .topic(topic)
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .sendTimeout(10, TimeUnit.SECONDS)
                .blockIfQueueFull(true)
                .create();
    }

    public void sendMessage(String message) {
        producer.sendAsync(message.getBytes()).thenAccept(msgId -> {
            System.out.printf("Message with ID %s successfully sent", msgId);
        });
    }

    // todo add exceptionally().
    public void close(Producer<byte[]> producer){
        producer.closeAsync()
                .thenRun(() -> System.out.println("Producer closed"));
    }

    public static void main(String[] args) throws PulsarClientException {
        MessageProducer producer = new MessageProducer("my-topic");
        producer.sendMessage("Hello World");
    }
}
