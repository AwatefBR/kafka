package fr.esgi.kafka.tempo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.esgi.kafka.tempo.processing.TempoSettings;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class TempoTopologyTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Instant BASE = Instant.parse("2026-07-04T12:00:00Z");
    @TempDir Path state;
    private TopologyTestDriver driver;
    private TestInputTopic<String, String> events;
    private TestInputTopic<String, String> tracks;
    private int sequence;

    @BeforeEach void start() {
        start(Duration.ofHours(1), Duration.ofHours(3), Duration.ofDays(7));
    }

    private void start(Duration fraudWindow, Duration grace, Duration retention) {
        if (driver != null) driver.close();
        var properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "tempo-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, state.toString());
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        var settings = new TempoSettings(grace, fraudWindow, retention, 10);
        driver = new TopologyTestDriver(new TopologyProducer(settings).buildTopology(), properties, BASE);
        events = driver.createInputTopic(Topics.LISTENING_EVENTS, new StringSerializer(), new StringSerializer());
        tracks = driver.createInputTopic(Topics.TRACKS, new StringSerializer(), new StringSerializer());
    }

    @AfterEach void close() { if (driver != null) driver.close(); }

    @Test void messageInvalideDoitPartirEnDlq() throws Exception {
        events.pipeInput("u-1", "{broken", BASE);
        JsonNode rejected = last(Topics.DLQ);
        assertEquals("{broken", rejected.get("originalMessage").asText());
        assertTrue(rejected.get("reason").asText().contains("JSON invalide"));
        assertTrue(output(Topics.ROYALTIES).isEmpty());
    }

    @Test void ecouteCompleteCrediteUneRoyaltie() throws Exception {
        send("COMPLETE", 31000, "trk-1", "u-1", BASE);
        JsonNode result = last(Topics.ROYALTIES);
        assertEquals("Artist", result.get("artist").asText());
        assertEquals(1, result.get("eligibleStreams").asLong());
        assertEquals(0, new BigDecimal("0.003").compareTo(result.get("amountEur").decimalValue()));
    }

    @Test void skipNeCrediteRien() throws Exception {
        send("SKIP", 12000, "trk-1", "u-1", BASE);
        send("SKIP", 45000, "trk-1", "u-1", BASE);
        send("COMPLETE", 29999, "trk-1", "u-1", BASE);
        send("START", 0, "trk-1", "u-1", BASE);
        assertTrue(output(Topics.ROYALTIES).isEmpty());
        send("COMPLETE", 30000, "trk-1", "u-1", BASE);
        assertEquals(1, last(Topics.ROYALTIES).get("eligibleStreams").asInt());
    }

    @Test void duplicatesDoNotPayOrIncreaseOtherCountersEvenWithDifferentKafkaKeys() throws Exception {
        String raw = event("ev-duplicate", "COMPLETE", 31000, "trk-1", "u-1", BASE);
        events.pipeInput("first-key", raw, BASE);
        events.pipeInput("other-key", raw, BASE.plusSeconds(1));
        assertEquals(1, last(Topics.ROYALTIES).get("eligibleStreams").asInt());
        assertEquals(1, last(Topics.SKIP_RATE).get("completes").asInt());
        assertEquals(1, last(Topics.TOP_BY_COUNTRY).get("tracks").get(0).get("listens").asInt());
    }

    @Test void royaltiesAccumulateExactlyAndAreSeparatedByArtist() throws Exception {
        for (int i = 0; i < 100; i++) send("COMPLETE", 30000, "trk-1", "u-1", BASE);
        assertEquals(0, new BigDecimal("0.300").compareTo(last(Topics.ROYALTIES).get("amountEur").decimalValue()));
        String other = event("ev-other", "COMPLETE", 30000, "trk-2", "u-2", BASE)
                .replace("\"Artist\"", "\"Other Artist\"");
        events.pipeInput("u-2", other, BASE);
        JsonNode result = last(Topics.ROYALTIES);
        assertEquals("Other Artist", result.get("artist").asText());
        assertEquals(1, result.get("eligibleStreams").asInt());
    }

    @Test void flopRequiresFiftyListensAndStrictlyMoreThanSeventyPercent() throws Exception {
        for (int i = 0; i < 3; i++) send("SKIP", 1000, "trk-small", "u-1", BASE);
        assertFalse(last(Topics.SKIP_RATE).get("flop").asBoolean());
        for (int i = 0; i < 35; i++) send("SKIP", 1000, "trk-1", "u-1", BASE);
        for (int i = 0; i < 15; i++) send("COMPLETE", 31000, "trk-1", "u-1", BASE);
        JsonNode exact = last(Topics.SKIP_RATE);
        assertEquals(0.7, exact.get("skipRate").asDouble(), 0.000001);
        assertFalse(exact.get("flop").asBoolean());
        send("SKIP", 1000, "trk-1", "u-1", BASE);
        assertTrue(last(Topics.SKIP_RATE).get("flop").asBoolean());
        send("START", 0, "trk-1", "u-1", BASE);
        assertTrue(output(Topics.SKIP_RATE).isEmpty());
    }

    @Test void skipWindowsUsePayloadTimeAndHaveTenMinuteBoundaries() throws Exception {
        send("SKIP", 1000, "trk-1", "u-1", BASE.plusSeconds(599));
        JsonNode first = last(Topics.SKIP_RATE);
        assertEquals(BASE.toString(), first.get("windowStart").asText());
        assertEquals(BASE.plusSeconds(600).toString(), first.get("windowEnd").asText());
        send("COMPLETE", 31000, "trk-1", "u-1", BASE.plusSeconds(600));
        JsonNode next = last(Topics.SKIP_RATE);
        assertEquals(BASE.plusSeconds(600).toString(), next.get("windowStart").asText());
        assertEquals(0, next.get("skips").asInt());
    }

    @Test void countryRankingEnrichesSortsAndKeepsOverlappingWindowsAcrossBoundary() throws Exception {
        catalog("trk-a", "Title A", "Artist A");
        catalog("trk-b", "Title B", "Artist B");
        send("COMPLETE", 31000, "trk-b", "u-1", BASE.plusSeconds(12 * 60));
        var windows = snapshots(Topics.TOP_BY_COUNTRY);
        assertEquals(3, windows.size());
        assertTrue(windows.containsKey("FR@" + BASE.plusSeconds(10 * 60).toEpochMilli()));
        send("SKIP", 1000, "trk-a", "u-1", BASE.plusSeconds(13 * 60));
        snapshots(Topics.TOP_BY_COUNTRY);
        send("SKIP", 1000, "trk-a", "u-1", BASE.plusSeconds(17 * 60));
        windows = snapshots(Topics.TOP_BY_COUNTRY);
        JsonNode ranking = windows.get("FR@" + BASE.plusSeconds(10 * 60).toEpochMilli());
        assertNotNull(ranking);
        assertEquals(2, ranking.get("tracks").size());
        assertEquals("trk-a", ranking.get("tracks").get(0).get("trackId").asText());
        assertEquals(2, ranking.get("tracks").get(0).get("listens").asInt());
        assertEquals("Title A", ranking.get("tracks").get(0).get("title").asText());
        assertEquals("Artist A", ranking.get("tracks").get(0).get("artist").asText());
        assertEquals(1, ranking.get("tracks").get(1).get("listens").asInt());
    }

    @Test void missingOrBrokenCatalogDoesNotDiscardListensAndCountriesAreIndependent() throws Exception {
        tracks.pipeInput("trk-1", "broken", BASE);
        send("COMPLETE", 31000, "trk-1", "u-1", BASE);
        JsonNode unknown = last(Topics.TOP_BY_COUNTRY).get("tracks").get(0);
        assertTrue(unknown.get("title").isNull());
        assertEquals("Artist", unknown.get("artist").asText());
        catalog("trk-1", "Available now", "Catalog artist");
        send("COMPLETE", 31000, "trk-1", "u-1", BASE);
        assertEquals("Available now", last(Topics.TOP_BY_COUNTRY).get("tracks").get(0).get("title").asText());
        String us = event("ev-us", "SKIP", 1000, "trk-1", "u-2", BASE).replace("\"FR\"", "\"US\"");
        events.pipeInput("u-2", us, BASE);
        JsonNode result = last(Topics.TOP_BY_COUNTRY);
        assertEquals("US", result.get("country").asText());
        assertEquals(1, result.get("tracks").get(0).get("listens").asInt());
    }

    @Test void topTenHasDeterministicTiesAndCatalogTombstonesAreSafe() throws Exception {
        for (int i = 11; i >= 0; i--) send("SKIP", 1000, "trk-%02d".formatted(i), "u-1", BASE);
        JsonNode ranking = last(Topics.TOP_BY_COUNTRY);
        assertEquals(10, ranking.get("tracks").size());
        assertEquals("trk-00", ranking.get("tracks").get(0).get("trackId").asText());
        catalog("trk-00", "Deleted", "Artist");
        tracks.pipeInput("trk-00", null, BASE);
        send("SKIP", 1000, "trk-00", "u-1", BASE);
        assertTrue(last(Topics.TOP_BY_COUNTRY).get("tracks").get(0).get("title").isNull());
    }

    @Test void fraudFiresOnceAtTwentyOneUniqueCompletesPerUserAndTrack() throws Exception {
        for (int i = 0; i < 20; i++) send("COMPLETE", 31000, "trk-1", "farm", BASE);
        assertTrue(output(Topics.ALERTS_FRAUD).isEmpty());
        send("SKIP", 31000, "trk-1", "farm", BASE);
        send("COMPLETE", 31000, "trk-other", "farm", BASE);
        send("COMPLETE", 31000, "trk-1", "other-user", BASE);
        assertTrue(output(Topics.ALERTS_FRAUD).isEmpty());
        // Identifiant distinct de ceux generes par send(), y compris pour les autres couples.
        String twentyFirst = event("fraud-threshold-event", "COMPLETE", 31000, "trk-1", "farm", BASE);
        events.pipeInput("farm", twentyFirst, BASE);
        JsonNode alert = last(Topics.ALERTS_FRAUD);
        assertEquals("farm", alert.get("userId").asText());
        assertEquals("trk-1", alert.get("trackId").asText());
        assertEquals(21, alert.get("completes").asInt());
        events.pipeInput("farm", twentyFirst, BASE);
        send("COMPLETE", 31000, "trk-1", "farm", BASE);
        assertTrue(output(Topics.ALERTS_FRAUD).isEmpty());
        send("COMPLETE", 31000, "trk-1", "farm", BASE.plusSeconds(3600));
        assertTrue(output(Topics.ALERTS_FRAUD).isEmpty());
    }

    @Test void fraudWindowCanBeFiveMinutesForDemo() throws Exception {
        start(Duration.ofMinutes(5), Duration.ofHours(3), Duration.ofDays(7));
        for (int i = 0; i < 21; i++) send("COMPLETE", 31000, "trk-1", "farm", BASE.plusSeconds(i * 5));
        JsonNode alert = last(Topics.ALERTS_FRAUD);
        assertEquals(BASE.plusSeconds(300).toString(), alert.get("windowEnd").asText());
        send("COMPLETE", 31000, "trk-1", "farm", BASE.plusSeconds(300));
        assertTrue(output(Topics.ALERTS_FRAUD).isEmpty());
    }

    @Test void lateEventsWithinGraceUpdateWindowsButOlderEventsOnlyPayRoyalties() throws Exception {
        send("COMPLETE", 31000, "trk-1", "u-1", BASE);
        // Un SKIP atteint l'agregation fenetree ; un START serait filtre en amont.
        send("SKIP", 1000, "clock-track", "clock-user", BASE.plusSeconds(2 * 3600));
        output(Topics.SKIP_RATE).readValuesToList();
        send("SKIP", 1000, "trk-1", "u-1", BASE.plusSeconds(60));
        JsonNode updated = last(Topics.SKIP_RATE);
        assertEquals(BASE.toString(), updated.get("windowStart").asText());
        assertEquals(1, updated.get("skips").asInt());
        send("SKIP", 1000, "clock-track", "clock-user", BASE.plusSeconds(5 * 3600));
        output(Topics.SKIP_RATE).readValuesToList();
        send("COMPLETE", 31000, "trk-1", "u-1", BASE.plusSeconds(120));
        assertTrue(output(Topics.SKIP_RATE).isEmpty());
        assertEquals(2, last(Topics.ROYALTIES).get("eligibleStreams").asInt());
        assertTrue(output(Topics.DLQ).isEmpty());
    }

    @Test void dedupRetentionExpiresUsingStreamTimeWithoutSleep() throws Exception {
        start(Duration.ofHours(1), Duration.ZERO, Duration.ofHours(1));
        String raw = event("ev-expire", "COMPLETE", 31000, "trk-1", "u-1", BASE);
        events.pipeInput("u-1", raw, BASE);
        events.pipeInput("u-1", raw, BASE.plusSeconds(1));
        assertEquals(1, last(Topics.ROYALTIES).get("eligibleStreams").asInt());
        send("START", 0, "trk-1", "u-1", BASE.plusSeconds(2 * 3600));
        events.pipeInput("u-1", raw, BASE.plusSeconds(2 * 3600));
        assertEquals(2, last(Topics.ROYALTIES).get("eligibleStreams").asInt());
    }

    @Test void hostileTimestampCannotCrashProcessing() throws Exception {
        for (String timestamp : List.of("1960-01-01T00:00:00Z", "+999999999-12-31T23:59:59Z")) {
            String raw = event("ev-bad-date", "COMPLETE", 31000, "trk-1", "u-1", BASE)
                    .replace(BASE.toString(), timestamp);
            events.pipeInput("u-1", raw, BASE);
            assertTrue(last(Topics.DLQ).get("reason").asText().contains("timestamp"));
        }
        send("COMPLETE", 31000, "trk-1", "u-1", BASE);
        assertEquals(1, last(Topics.ROYALTIES).get("eligibleStreams").asInt());
    }

    private void catalog(String track, String title, String artist) throws Exception {
        tracks.pipeInput(track, JSON.writeValueAsString(Map.of("track_id", track, "title", title,
                "artist", artist, "genre", "POP", "duration_ms", 180000)), BASE);
    }

    private void send(String type, int ms, String track, String user, Instant time) throws Exception {
        events.pipeInput(user, event("ev-" + sequence++, type, ms, track, user, time), time.plusSeconds(10));
    }

    private static String event(String id, String type, int ms, String track, String user, Instant time) throws Exception {
        return JSON.writeValueAsString(Map.of("event_id", id, "user_id", user, "track_id", track,
                "artist", "Artist", "event_type", type, "ms_played", ms, "country", "FR", "timestamp", time.toString()));
    }

    private TestOutputTopic<String, String> output(String name) {
        return driver.createOutputTopic(name, new StringDeserializer(), new StringDeserializer());
    }

    private JsonNode last(String name) throws Exception {
        var values = output(name).readValuesToList();
        assertFalse(values.isEmpty(), "Sortie attendue dans " + name);
        return JSON.readTree(values.getLast());
    }

    private Map<String, JsonNode> snapshots(String name) throws Exception {
        var result = new HashMap<String, JsonNode>();
        for (var pair : output(name).readKeyValuesToList()) result.put(pair.key, JSON.readTree(pair.value));
        return result;
    }
}
