import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

public class MessageProducer {

    private Client client;
    private Producer<byte[]> producer;

    public MessageProducer() throws PulsarClientException {
        client = new Client();
        producer = client.getPulsarClient().newProducer()
                .topic("my-topic")
                .create();
    }

    public void sendMessage(String message) throws PulsarClientException {
        producer.send(message.getBytes());
    }


    public static void main(String[] args) throws PulsarClientException {
        MessageProducer mp = new MessageProducer();
        mp.sendMessage("Hello World");
    }
}
