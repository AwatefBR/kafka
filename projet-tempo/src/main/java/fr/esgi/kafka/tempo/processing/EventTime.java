package fr.esgi.kafka.tempo.processing;

import fr.esgi.kafka.tempo.model.ListeningEvent;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import java.time.Instant;

/** Applique le temps metier seulement APRES validation, pour proteger la DLQ. */
public final class EventTime implements FixedKeyProcessor<String, ListeningEvent, ListeningEvent> {
    private FixedKeyProcessorContext<String, ListeningEvent> context;

    @Override
    public void init(FixedKeyProcessorContext<String, ListeningEvent> context) {
        this.context = context;
    }

    @Override
    public void process(FixedKeyRecord<String, ListeningEvent> record) {
        context.forward(record.withTimestamp(Instant.parse(record.value().timestamp()).toEpochMilli()));
    }
}
