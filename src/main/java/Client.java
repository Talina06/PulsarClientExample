import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class Client {

    private PulsarClient client;

    public Client() throws PulsarClientException {
        client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
    }

    public PulsarClient getPulsarClient(){
        return client;
    }
}
