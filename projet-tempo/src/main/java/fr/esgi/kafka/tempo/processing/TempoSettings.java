package fr.esgi.kafka.tempo.processing;

import org.eclipse.microprofile.config.ConfigProvider;
import java.time.Duration;

public record TempoSettings(Duration grace, Duration fraudWindow, Duration dedupRetention, int topLimit) {
    public TempoSettings {
        if (grace.isNegative() || fraudWindow.isNegative() || fraudWindow.isZero()
                || dedupRetention.compareTo(grace.plusMinutes(15)) <= 0 || topLimit < 1) {
            throw new IllegalArgumentException("Fenetre positive, grace >= 0, retention > grace + 15 min et topLimit >= 1 requis");
        }
    }

    public static TempoSettings fromConfig() {
        var config = ConfigProvider.getConfig();
        return new TempoSettings(
                Duration.ofMinutes(config.getOptionalValue("tempo.grace.minutes", Long.class).orElse(180L)),
                Duration.ofMinutes(config.getOptionalValue("tempo.fraud.window.minutes", Long.class).orElse(60L)),
                Duration.ofHours(config.getOptionalValue("tempo.dedup.retention.hours", Long.class).orElse(168L)),
                config.getOptionalValue("tempo.top.limit", Integer.class).orElse(10));
    }
}
