package fr.esgi.kafka.tempo.processing;

import fr.esgi.kafka.tempo.model.ListeningEvent;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.WindowStore;
import java.time.Duration;
import java.time.Instant;

/** Le repartitionnement par event_id en amont rend le controle commun a tous les producteurs. */
public final class UniqueEvents implements FixedKeyProcessor<String, ListeningEvent, ListeningEvent> {
    public static final String STORE = "seen-event-ids";
    private final Duration retention;
    private FixedKeyProcessorContext<String, ListeningEvent> context;
    private WindowStore<String, Boolean> seen;

    public UniqueEvents(Duration retention) {
        this.retention = retention;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, ListeningEvent> context) {
        this.context = context;
        this.seen = context.getStateStore(STORE);
    }

    @Override
    public void process(FixedKeyRecord<String, ListeningEvent> record) {
        long now = Math.max(record.timestamp(), context.currentStreamTimeMs());
        long start = Math.max(0, now - retention.toMillis());
        try (var matches = seen.fetch(record.key(), Instant.ofEpochMilli(start), Instant.ofEpochMilli(now))) {
            if (matches.hasNext()) return;
        }
        // Stocker a l'instant du watermark permet aussi de dedoublonner les retardataires.
        // L'expiration et le nettoyage disque sont geres par le WindowStore Kafka.
        seen.put(record.key(), true, now);
        context.forward(record);
    }
}
