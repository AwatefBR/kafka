package fr.esgi.kafka.tempo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Fiche titre (topic compacte tempo.tracks, cle = track_id). */
public record Track(
        @JsonProperty("track_id") String trackId,
        @JsonProperty("title") String title,
        @JsonProperty("artist") String artist,
        @JsonProperty("genre") String genre,
        @JsonProperty("duration_ms") Integer durationMs) {
}
