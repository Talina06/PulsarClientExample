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

    public MessageConsumer() throws PulsarClientException {
        client = new Client();
        consumer = client.getPulsarClient().newConsumer()
                .topic("my-topic")
                .subscriptionName("my-subscription")
                .ackTimeout(10, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();
    }

    public void receiveMessage() throws PulsarClientException, ExecutionException, InterruptedException {
        do {
            // Wait for a message
            CompletableFuture<Message> msg = consumer.receiveAsync();

            System.out.printf("Message received: %s", new String(msg.get().getData()));

            // Acknowledge the message so that it can be deleted by the message broker
            consumer.acknowledge(msg.get());
        } while (true);
    }


    public static void main(String[] args) throws PulsarClientException, ExecutionException, InterruptedException {
        MessageConsumer mc = new MessageConsumer();
        mc.receiveMessage();
    }
}
