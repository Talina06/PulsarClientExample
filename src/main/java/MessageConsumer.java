import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MessageConsumer {

    private Client client;
    private Consumer consumer;

    public MessageConsumer(String topic, String subscription) throws PulsarClientException {
        client = new Client();
        consumer = createConsumer(topic, subscription);
    }

    private Consumer createConsumer(String topic, String subscription) throws PulsarClientException {
        return client.getPulsarClient().newConsumer()
                .topic(topic)
                .subscriptionName(subscription)
                .ackTimeout(10, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();
    }

    public void receiveMessage() throws ExecutionException, InterruptedException, PulsarClientException {
        do {
            // Wait for a message
            CompletableFuture<Message> msg = consumer.receiveAsync();

            System.out.printf("Message received: %s", new String(msg.get().getData()));

            // Acknowledge the message so that it can be deleted by the message broker
            consumer.acknowledge(msg.get());
        } while (true);
    }


    public static void main(String[] args) throws PulsarClientException, ExecutionException, InterruptedException {
        MessageConsumer consumer = new MessageConsumer("my-topic", "my-subscription");
        consumer.receiveMessage();
    }
}
