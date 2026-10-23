package fr.esgi.kafka.tempo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Evenement d'ecoute (topic tempo.listening.events, cle = user_id).
 * event_type : START | SKIP | COMPLETE.
 * ms_played : millisecondes ecoutees (0 sur les START).
 */
public record ListeningEvent(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("user_id") String userId,
        @JsonProperty("track_id") String trackId,
        @JsonProperty("artist") String artist,
        @JsonProperty("event_type") String eventType,
        @JsonProperty("ms_played") Integer msPlayed,
        @JsonProperty("country") String country,
        @JsonProperty("timestamp") String timestamp) {
}
