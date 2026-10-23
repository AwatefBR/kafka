package fr.esgi.kafka.tempo.processing;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import java.time.Instant;

/** Toutes les mises a jour d'une meme fenetre hopping doivent rejoindre le meme etat. */
public final class RankingTime implements FixedKeyProcessor<String, Analytics.TopUpdate, Analytics.TopUpdate> {
    private FixedKeyProcessorContext<String, Analytics.TopUpdate> context;

    @Override
    public void init(FixedKeyProcessorContext<String, Analytics.TopUpdate> context) {
        this.context = context;
    }

    @Override
    public void process(FixedKeyRecord<String, Analytics.TopUpdate> record) {
        context.forward(record.withTimestamp(Instant.parse(record.value().windowStart()).toEpochMilli()));
    }
}
