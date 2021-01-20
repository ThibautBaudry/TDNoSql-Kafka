package analysis.WifiSpots;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.Properties;

import static config.KafkaConfig.RAW_TOPIC_NAME;
import static config.KafkaConfig.BOOTSTRAP_SERVERS;

public class producer {

    public static final String STREAM_APP_1_OUT = "wifiSpots-nbTerminals-update";
    private static final String WIFI_STREAM_APPLICATION = "wifiSpots-stream-application";
    private static final String STREAM_APP_1_INPUT = RAW_TOPIC_NAME;

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, WIFI_STREAM_APPLICATION);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS.get(0));
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        producer wifiSpotsApp = new producer();

        KafkaStreams streams = new KafkaStreams(wifiSpotsApp.createTopology(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        while (true) {
            streams.localThreadsMetadata().forEach(System.out::println);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private Topology createTopology() {

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> doubleSerde = Serdes.Long();
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stats = builder.stream(STREAM_APP_1_INPUT);
        KStream<String, Long> wifiSpots = stats
                .selectKey((key, jsonRecordString) -> extract_wifi_place(jsonRecordString))
                .map((key, value) -> new KeyValue<>(key, extract_nb_wifi_terminals(value)));

        wifiSpots.to(STREAM_APP_1_OUT, Produced.with(stringSerde, doubleSerde));

        return builder.build();
    }

    private String extract_wifi_place(String jsonRecordString) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(jsonRecordString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JsonNode fieldsMode = jsonNode.get("fields");

        JsonNode wifiPlaceName = fieldsMode.get("nom_site");

        return wifiPlaceName.asText();
    }

    private Long extract_nb_wifi_terminals(String jsonRecordString) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(jsonRecordString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JsonNode fieldsMode = jsonNode.get("fields");

        JsonNode nbWifiTerminals = fieldsMode.get("nombre_de_borne_wifi");

        return Long.parseLong(nbWifiTerminals.asText());
    }
}
