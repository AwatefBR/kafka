package fr.esgi.kafka.tempo.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.esgi.kafka.tempo.model.ListeningEvent;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;

/** Validation stricte avant toute conversion vers le modele Java. */
public final class EventValidator {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final Set<String> EVENT_TYPES = Set.of("START", "SKIP", "COMPLETE");
    private static final Set<String> COUNTRIES = Set.of("FR", "US", "DE", "BR", "JP", "GB");
    private static final String[] TEXT_FIELDS = {
            "event_id", "user_id", "track_id", "artist", "event_type", "country", "timestamp"
    };

    private EventValidator() {
    }

    /** Le resultat porte toujours le contenu original, meme null ou illisible. */
    public record Result(String originalMessage, ListeningEvent event, String reason) {
        public boolean isValid() {
            return event != null;
        }
    }

    public static Result validate(String raw) {
        if (raw == null || raw.isBlank()) {
            return invalid(raw, "Message vide ou null");
        }

        final JsonNode json;
        try {
            json = MAPPER.readTree(raw);
        } catch (JsonProcessingException e) {
            return invalid(raw, "JSON invalide : " + e.getOriginalMessage());
        }
        if (json == null || !json.isObject()) {
            return invalid(raw, "La racine JSON doit etre un objet");
        }

        for (String field : TEXT_FIELDS) {
            JsonNode value = json.get(field);
            if (value == null || value.isNull()) {
                return invalid(raw, "Champ requis absent ou null : " + field);
            }
            if (!value.isTextual() || value.textValue().isBlank()) {
                return invalid(raw, "Le champ " + field + " doit etre une chaine non vide");
            }
        }

        JsonNode duration = json.get("ms_played");
        if (duration == null || duration.isNull()) {
            return invalid(raw, "Champ requis absent ou null : ms_played");
        }
        if (!duration.isIntegralNumber() || !duration.canConvertToInt() || duration.intValue() < 0) {
            return invalid(raw, "ms_played doit etre un entier entre 0 et 2147483647");
        }
        if (!EVENT_TYPES.contains(json.get("event_type").textValue())) {
            return invalid(raw, "event_type inconnu : attendu START, SKIP ou COMPLETE");
        }
        if (!COUNTRIES.contains(json.get("country").textValue())) {
            return invalid(raw, "country inconnu : attendu FR, US, DE, BR, JP ou GB");
        }
        try {
            // On verifie le format, sans rejeter les evenements en retard.
            long millis = Instant.parse(json.get("timestamp").textValue()).toEpochMilli();
            if (millis < 0) {
                return invalid(raw, "timestamp doit etre posterieur ou egal a l'epoch Kafka (1970)");
            }
        } catch (DateTimeParseException | ArithmeticException e) {
            return invalid(raw, "timestamp doit etre une date ISO-8601 avec fuseau, representable en millisecondes Kafka");
        }

        ListeningEvent event = new ListeningEvent(
                json.get("event_id").textValue(), json.get("user_id").textValue(),
                json.get("track_id").textValue(), json.get("artist").textValue(),
                json.get("event_type").textValue(), duration.intValue(),
                json.get("country").textValue(), json.get("timestamp").textValue());
        return new Result(raw, event, null);
    }

    private static Result invalid(String raw, String reason) {
        return new Result(raw, null, reason);
    }
}
