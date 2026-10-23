package fr.esgi.kafka.tempo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import fr.esgi.kafka.tempo.common.JsonSerdes;
import fr.esgi.kafka.tempo.model.ListeningEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class TempoIngestionTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String VALID = """
            {"event_id":"ev-1","user_id":"u-1","track_id":"trk-1",
             "artist":"Nova Kane","event_type":"COMPLETE","ms_played":31000,
             "country":"FR","timestamp":"2026-07-04T14:03:21.512Z"}
            """;

    @Test
    void productionTopologyRoutesBrokenJsonAndPreservesOriginalAndKey() throws Exception {
        try (var driver = driver(new TopologyProducer().buildTopology())) {
            String broken = "{\"event_id\": \"ev";
            input(driver).pipeInput("u-broken", broken);
            var dlq = dlq(driver);
            var record = dlq.readKeyValue();
            assertEquals("u-broken", record.key);
            var payload = MAPPER.readTree(record.value);
            assertEquals(broken, payload.get("originalMessage").textValue());
            assertTrue(payload.get("reason").textValue().contains("JSON invalide"));
            assertTrue(dlq.isEmpty());
        }
    }

    @Test
    void rejectsMissingNullAndWronglyTypedFields() throws Exception {
        try (var driver = driver(observableTopology())) {
            var input = input(driver);
            var dlq = dlq(driver);
            for (String field : List.of("event_id", "user_id", "track_id", "artist",
                    "event_type", "ms_played", "country", "timestamp")) {
                for (int variant = 0; variant < 3; variant++) {
                    ObjectNode event = validObject();
                    if (variant == 0) event.remove(field);
                    else if (variant == 1) event.putNull(field);
                    else event.put(field, true);
                    String raw = event.toString();
                    input.pipeInput("u-1", raw);
                    var rejected = MAPPER.readTree(dlq.readValue());
                    assertEquals(raw, rejected.get("originalMessage").textValue());
                    assertTrue(rejected.get("reason").textValue().contains(field), field);
                }
            }
            assertTrue(validOutput(driver).isEmpty());
            assertTrue(dlq.isEmpty());
        }
    }

    @Test
    void poisonPillsDoNotStopSubsequentValidMessages() throws Exception {
        try (var driver = driver(observableTopology())) {
            var input = input(driver);
            var dlq = dlq(driver);
            String[] brokenMessages = {null, "", "   ", "not JSON", "null", "[]", "42",
                    "\"text\"", "{", VALID + " {}", VALID + " trailing"};
            for (String broken : brokenMessages) {
                input.pipeInput("u-1", broken);
                input.pipeInput("u-1", VALID);
                var rejected = MAPPER.readTree(dlq.readValue());
                if (broken == null) assertTrue(rejected.get("originalMessage").isNull());
                else assertEquals(broken, rejected.get("originalMessage").textValue());
                assertFalse(rejected.get("reason").textValue().isBlank());
            }
            assertEquals(brokenMessages.length, validOutput(driver).readValuesToList().size());
            assertTrue(dlq.isEmpty());
        }
    }

    @Test
    void rejectsCoercionsOutOfRangeValuesAndUnknownEnums() throws Exception {
        try (var driver = driver(observableTopology())) {
            var input = input(driver);
            var dlq = dlq(driver);
            for (String duration : List.of("\"31000\"", "\"douze\"", "-1", "3.5", "2147483648")) {
                ObjectNode event = validObject();
                event.set("ms_played", MAPPER.readTree(duration));
                input.pipeInput("u-1", event.toString());
                assertTrue(MAPPER.readTree(dlq.readValue()).get("reason").textValue().contains("ms_played"));
            }
            for (var invalid : List.of(
                    new String[]{"event_type", "COMPLETEE"}, new String[]{"country", "XX"},
                    new String[]{"timestamp", "hier a 15h"},
                    new String[]{"timestamp", "2026-07-04T14:00:00"},
                    new String[]{"user_id", "   "})) {
                ObjectNode event = validObject();
                event.put(invalid[0], invalid[1]);
                input.pipeInput("u-1", event.toString());
                assertTrue(MAPPER.readTree(dlq.readValue()).get("reason").textValue().contains(invalid[0]));
            }
            assertTrue(validOutput(driver).isEmpty());
        }
    }

    @Test
    void validEventsKeepTheirDataKeyAndKafkaTimestampIncludingLateEvents() throws Exception {
        try (var driver = driver(observableTopology())) {
            var input = input(driver);
            var valid = validOutput(driver);
            Instant arrival = Instant.parse("2026-07-04T17:03:21.512Z");
            for (String type : List.of("START", "SKIP", "COMPLETE")) {
                ObjectNode event = validObject();
                event.put("event_type", type);
                event.put("ms_played", type.equals("START") ? 0 : 31000);
                event.put("extra_field", "accepted");
                input.pipeInput("u-1", event.toString(), arrival);
                var record = valid.readRecord();
                assertEquals("u-1", record.key());
                assertEquals(arrival.toEpochMilli(), record.timestamp());
                assertEquals(MAPPER.treeToValue(event.without("extra_field"), ListeningEvent.class),
                        record.value());
            }
            // Deux copies valides restent disponibles : dedoublonnage prevu en TMP-4.
            input.pipeInput((String) null, VALID);
            input.pipeInput((String) null, VALID);
            var duplicates = valid.readKeyValuesToList();
            assertEquals(2, duplicates.size());
            assertNull(duplicates.getFirst().key);
            assertEquals(duplicates.getFirst().value, duplicates.getLast().value);
            assertTrue(dlq(driver).isEmpty());
        }
    }

    private static ObjectNode validObject() throws Exception {
        return (ObjectNode) MAPPER.readTree(VALID);
    }

    private static Topology observableTopology() {
        var builder = new StreamsBuilder();
        new TopologyProducer().ingest(builder).to("test.valid",
                Produced.with(Serdes.String(), JsonSerdes.of(ListeningEvent.class)));
        return builder.build();
    }

    private static TopologyTestDriver driver(Topology topology) {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tempo-ingestion-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        return new TopologyTestDriver(topology, props);
    }

    private static TestInputTopic<String, String> input(TopologyTestDriver driver) {
        return driver.createInputTopic(Topics.LISTENING_EVENTS,
                new StringSerializer(), new StringSerializer());
    }

    private static TestOutputTopic<String, String> dlq(TopologyTestDriver driver) {
        return driver.createOutputTopic(Topics.DLQ,
                new StringDeserializer(), new StringDeserializer());
    }

    private static TestOutputTopic<String, ListeningEvent> validOutput(TopologyTestDriver driver) {
        return driver.createOutputTopic("test.valid", new StringDeserializer(),
                JsonSerdes.of(ListeningEvent.class).deserializer());
    }
}
