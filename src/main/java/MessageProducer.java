import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.concurrent.TimeUnit;

public class MessageProducer {

    private Client client;
    private Producer<byte[]> producer;

    public MessageProducer() throws PulsarClientException {
        client = new Client();
        producer = client.getPulsarClient().newProducer()
                .topic("my-topic")
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .sendTimeout(10, TimeUnit.SECONDS)
                .blockIfQueueFull(true)
                .create();
    }

    public void sendMessage(String message) throws PulsarClientException {
        producer.sendAsync(message.getBytes()).thenAccept(msgId -> {
            System.out.printf("Message with ID %s successfully sent", msgId);
        });
    }

    // todo add exceptionally().
    public void close(){
        producer.closeAsync()
                .thenRun(() -> System.out.println("Producer closed"));
    }

    public static void main(String[] args) throws PulsarClientException {
        MessageProducer mp = new MessageProducer();
        mp.sendMessage("Hello World");
    }
}
