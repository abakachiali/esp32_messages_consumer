package esp32_messages_consumer;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import esp32_messages_consumer.service.DatasetsService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import static com.hivemq.client.mqtt.MqttGlobalPublishFilter.ALL;
import static java.nio.charset.StandardCharsets.UTF_8;

@SpringBootApplication
public class Esp32MessagesConsumerApplication {

	@Autowired
	static DatasetsService datasetsService;

	@Autowired
	private DatasetsService datasetsService0;

	@PostConstruct
	public void init() {
		Esp32MessagesConsumerApplication.datasetsService = datasetsService0;
	}

	public static void main(String[] args) {

		SpringApplication.run(Esp32MessagesConsumerApplication.class, args);

		final String host = "2e572361be2146ed90f79d54e975aacc.s1.eu.hivemq.cloud";
		final String username = "abakachiali";
		final String password = "DDSYny*_CVq6mHU";

		// create an MQTT client
		final Mqtt5BlockingClient client = MqttClient.builder()
				.useMqttVersion5()
				.serverHost(host)
				.serverPort(8883)
				.sslWithDefaultConfig()
				.buildBlocking();

		// connect to HiveMQ Cloud with TLS and username/pw
		client.connectWith()
				.simpleAuth()
				.username(username)
				.password(UTF_8.encode(password))
				.applySimpleAuth()
				.send();

		System.out.println("Connected successfully");

		// subscribe to the topic "my/test/topic"
		client.subscribeWith()
				.topicFilter("testtopic/1")
				.send();

		// set a callback that is called when a message is received (using the async API style)
		client.toAsync().publishes(ALL, publish -> {
			System.out.println("Received message: " +
					publish.getTopic() + " -> " +
					UTF_8.decode(publish.getPayload().get()));
			try {
				String result = datasetsService.processFileDataContent(UTF_8.decode(publish.getPayload().get()));
				System.out.println("Dataset processed: " + result);
			} catch (ParseException | IOException e) {
				e.printStackTrace();
			}

			// disconnect the client after a message was received
			//client.disconnect();
		});

		// publish a message to the topic "my/test/topic"
		/*
		client.publishWith()
				.topic("my/test/topic")
				.payload(UTF_8.encode("Hello"))
				.send();
		*/
	}

}
