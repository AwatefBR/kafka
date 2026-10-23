package fr.esgi.kafka.tempo.processing;

import fr.esgi.kafka.tempo.Topics;
import fr.esgi.kafka.tempo.TopologyProducer;
import fr.esgi.kafka.tempo.common.JsonSerdes;
import fr.esgi.kafka.tempo.model.ListeningEvent;
import fr.esgi.kafka.tempo.model.Track;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public final class Analytics {
    private Analytics() {}

    public record SkipCounts(long skips, long completes) {
        SkipCounts add(ListeningEvent event) {
            return new SkipCounts(skips + (event.eventType().equals("SKIP") ? 1 : 0),
                    completes + (event.eventType().equals("COMPLETE") ? 1 : 0));
        }
    }
    public record SkipRate(String trackId, String windowStart, String windowEnd,
                           long skips, long completes, double skipRate, boolean flop) {}
    public record Royalties(String artist, long eligibleStreams, BigDecimal amountEur) {}
    public record UserTrack(String userId, String trackId) {}
    public record Fraud(String userId, String trackId, String windowStart, String windowEnd, long completes) {}
    public record CountryTrack(String country, String trackId) {}
    public record Enriched(ListeningEvent event, String title, String artist) {}
    public record TopEntry(String trackId, String title, String artist, long listens) {}
    public record TopUpdate(String country, String windowStart, String windowEnd, TopEntry entry) {}
    public record RankingState(String country, String windowStart, String windowEnd, Map<String, TopEntry> entries) {}
    public record Ranking(String country, String windowStart, String windowEnd, List<TopEntry> tracks) {}

    public static void attach(StreamsBuilder builder, KStream<String, ListeningEvent> valid, TempoSettings settings) {
        builder.addStateStore(Stores.windowStoreBuilder(
                Stores.persistentWindowStore(UniqueEvents.STORE, settings.dedupRetention(), Duration.ofMillis(1), false),
                Serdes.String(), Serdes.Boolean()));

        KStream<String, ListeningEvent> unique = valid
                .processValues(EventTime::new, Named.as("event-time"))
                .selectKey((key, event) -> event.eventId())
                .repartition(Repartitioned.<String, ListeningEvent>as("by-event-id")
                        .withKeySerde(Serdes.String()).withValueSerde(JsonSerdes.of(ListeningEvent.class)))
                .processValues(() -> new UniqueEvents(settings.dedupRetention()),
                        Named.as("deduplicate"), UniqueEvents.STORE);

        KStream<String, ListeningEvent> listens = unique.filter((key, event) -> !event.eventType().equals("START"));
        skipRates(listens, settings);
        topByCountry(builder, listens, settings);
        royalties(unique);
        fraud(unique, settings);
    }

    private static void skipRates(KStream<String, ListeningEvent> listens, TempoSettings settings) {
        listens.groupBy((key, event) -> event.trackId(),
                        Grouped.with(Serdes.String(), JsonSerdes.of(ListeningEvent.class)))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(10), settings.grace()))
                .aggregate(() -> new SkipCounts(0, 0), (key, event, count) -> count.add(event),
                        Materialized.<String, SkipCounts, org.apache.kafka.streams.state.WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("skip-counts")
                                .withKeySerde(Serdes.String()).withValueSerde(JsonSerdes.of(SkipCounts.class)))
                .toStream().map((key, count) -> {
                    long total = count.skips() + count.completes();
                    var output = new SkipRate(key.key(), iso(key.window().start()), iso(key.window().end()),
                            count.skips(), count.completes(), (double) count.skips() / total,
                            total >= 50 && count.skips() * 10 > total * 7);
                    return KeyValue.pair(key.key() + "@" + key.window().start(), JsonSerdes.toJson(output));
                }).to(Topics.SKIP_RATE, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static void royalties(KStream<String, ListeningEvent> events) {
        events.filter((key, event) -> event.eventType().equals("COMPLETE")
                        && event.msPlayed() >= TopologyProducer.MIN_MS_FOR_ROYALTY)
                .groupBy((key, event) -> event.artist(),
                        Grouped.with(Serdes.String(), JsonSerdes.of(ListeningEvent.class)))
                .aggregate(() -> 0L, (artist, event, count) -> count + 1,
                        Materialized.<String, Long, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("royalty-counts")
                                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
                .toStream().mapValues((artist, count) -> JsonSerdes.toJson(new Royalties(artist, count,
                        BigDecimal.valueOf(TopologyProducer.ROYALTY_PER_STREAM).multiply(BigDecimal.valueOf(count)))))
                .to(Topics.ROYALTIES, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static void fraud(KStream<String, ListeningEvent> events, TempoSettings settings) {
        events.filter((key, event) -> event.eventType().equals("COMPLETE"))
                .groupBy((key, event) -> new UserTrack(event.userId(), event.trackId()),
                        Grouped.with(JsonSerdes.of(UserTrack.class), JsonSerdes.of(ListeningEvent.class)))
                .windowedBy(TimeWindows.ofSizeAndGrace(settings.fraudWindow(), settings.grace()))
                .count(Materialized.<UserTrack, Long, org.apache.kafka.streams.state.WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("fraud-counts")
                        .withKeySerde(JsonSerdes.of(UserTrack.class)).withValueSerde(Serdes.Long()).withCachingDisabled())
                .toStream().filter((key, count) -> count == 21L)
                .map((key, count) -> KeyValue.pair(JsonSerdes.toJson(key.key()) + "@" + key.window().start(),
                        JsonSerdes.toJson(new Fraud(key.key().userId(), key.key().trackId(),
                                iso(key.window().start()), iso(key.window().end()), count))))
                .to(Topics.ALERTS_FRAUD, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static void topByCountry(StreamsBuilder builder, KStream<String, ListeningEvent> listens, TempoSettings settings) {
        GlobalKTable<String, String> tracks = builder.globalTable(Topics.TRACKS,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as("track-catalog"));

        listens.leftJoin(tracks, (key, event) -> event.trackId(), (event, rawTrack) -> {
                    Track track = JsonSerdes.parseOrNull(rawTrack, Track.class);
                    return new Enriched(event, track == null ? null : track.title(),
                            track == null || track.artist() == null ? event.artist() : track.artist());
                })
                .groupBy((key, value) -> new CountryTrack(value.event().country(), value.event().trackId()),
                        Grouped.with(JsonSerdes.of(CountryTrack.class), JsonSerdes.of(Enriched.class)))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(15), settings.grace()).advanceBy(Duration.ofMinutes(5)))
                .aggregate(() -> new TopEntry(null, null, null, 0),
                        (key, value, previous) -> new TopEntry(key.trackId(), value.title(), value.artist(), previous.listens() + 1),
                        Materialized.<CountryTrack, TopEntry, org.apache.kafka.streams.state.WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("country-track-counts")
                                .withKeySerde(JsonSerdes.of(CountryTrack.class)).withValueSerde(JsonSerdes.of(TopEntry.class)))
                .toStream().map((key, entry) -> KeyValue.pair(key.key().country() + "@" + key.window().start(),
                        new TopUpdate(key.key().country(), iso(key.window().start()), iso(key.window().end()), entry)))
                .processValues(RankingTime::new, Named.as("ranking-window-time"))
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.of(TopUpdate.class)))
                // Bucket 5 min a l'instant de debut de la fenetre 15 min ; fermeture identique.
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), settings.grace().plusMinutes(10)))
                .aggregate(() -> new RankingState(null, null, null, Map.of()), (key, update, previous) -> {
                    var entries = new HashMap<>(previous.entries());
                    entries.put(update.entry().trackId(), update.entry());
                    return new RankingState(update.country(), update.windowStart(), update.windowEnd(), entries);
                }, Materialized.<String, RankingState, org.apache.kafka.streams.state.WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("country-rankings")
                        .withKeySerde(Serdes.String()).withValueSerde(JsonSerdes.of(RankingState.class)))
                .toStream().map((key, state) -> {
                    var sorted = state.entries().values().stream()
                            .sorted(Comparator.comparingLong(TopEntry::listens).reversed().thenComparing(TopEntry::trackId))
                            .limit(settings.topLimit()).toList();
                    return KeyValue.pair(key.key(), JsonSerdes.toJson(new Ranking(state.country(),
                            state.windowStart(), state.windowEnd(), sorted)));
                }).to(Topics.TOP_BY_COUNTRY, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static String iso(long millis) {
        return Instant.ofEpochMilli(millis).toString();
    }
}
