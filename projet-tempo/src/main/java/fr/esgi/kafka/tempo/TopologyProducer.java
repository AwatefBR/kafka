package fr.esgi.kafka.tempo;

import fr.esgi.kafka.tempo.common.JsonSerdes;
import fr.esgi.kafka.tempo.ingestion.EventValidator;
import fr.esgi.kafka.tempo.model.ListeningEvent;
import fr.esgi.kafka.tempo.processing.Analytics;
import fr.esgi.kafka.tempo.processing.TempoSettings;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Construisez ici la topologie du projet TEMPO.
 * Quarkus detecte ce @Produces Topology et demarre Kafka Streams
 * automatiquement (extension quarkus-kafka-streams).
 * Chaque ticket du backlog (README) correspond a un bloc ci-dessous.
 */
@ApplicationScoped
public class TopologyProducer {

    private final TempoSettings settings;

    public TopologyProducer() {
        this(TempoSettings.fromConfig());
    }

    public TopologyProducer(TempoSettings settings) {
        this.settings = settings;
    }

    /** Une ecoute complete (>= 30 s) rapporte 0.003 EUR a l'artiste. */
    public static final double ROYALTY_PER_STREAM = 0.003;
    public static final int MIN_MS_FOR_ROYALTY = 30000;

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, ListeningEvent> validEvents = ingest(builder);

        Analytics.attach(builder, validEvents, settings);

        return builder.build();
    }

    /**
     * TMP-1 : une seule validation par message, puis deux branches exclusives.
     * Le flux retourne sera l'entree des traitements TMP-2 a TMP-5.
     */
    public KStream<String, ListeningEvent> ingest(StreamsBuilder builder) {
        KStream<String, String> rawEvents = builder.stream(
                Topics.LISTENING_EVENTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        var branches = rawEvents.mapValues(EventValidator::validate)
                .split(Named.as("validation-"))
                .branch((key, result) -> !result.isValid(), Branched.as("invalid"))
                .defaultBranch(Branched.as("valid"));

        branches.get("validation-invalid")
                .mapValues(result -> JsonSerdes.toJson(
                        new DeadLetter(result.originalMessage(), result.reason())))
                .to(Topics.DLQ, Produced.with(Serdes.String(), Serdes.String()));

        return branches.get("validation-valid").mapValues(EventValidator.Result::event);
    }

    public record DeadLetter(String originalMessage, String reason) {
    }
}
