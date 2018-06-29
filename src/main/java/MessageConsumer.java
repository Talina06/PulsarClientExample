import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

public class MessageConsumer {

    private Client client;
    private Consumer consumer;

    public MessageConsumer() throws PulsarClientException {
        client = new Client();
        consumer = client.getPulsarClient().newConsumer()
                .topic("my-topic")
                .subscriptionName("my-subscription")
                .subscribe();
    }

    public void receiveMessage() throws PulsarClientException {
        do {
            // Wait for a message
            Message msg = consumer.receive();

            System.out.printf("Message received: %s", new String(msg.getData()));

            // Acknowledge the message so that it can be deleted by the message broker
            consumer.acknowledge(msg);
        } while (true);
    }


    public static void main(String[] args) throws PulsarClientException {
        MessageConsumer mc = new MessageConsumer();
        mc.receiveMessage();
    }
}
