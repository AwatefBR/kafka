package fr.esgi.kafka.tempo;

import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Noms des topics du projet.
 * Entrees fournies par l'enseignant (lecture seule) ; sorties prefixees
 * par votre code groupe (variable d'environnement GROUPE, ex: grp07).
 */
public final class Topics {

    private Topics() {
    }

    // --- Entrees ---
    public static final String LISTENING_EVENTS = "tempo.listening.events";
    public static final String TRACKS = "tempo.tracks";

    /** Code groupe, obligatoire sur le cluster partage. */
    public static final String GROUPE = ConfigProvider.getConfig()
            .getOptionalValue("GROUPE", String.class)
            .orElse("grp00");

    public static String out(String suffix) {
        return GROUPE + "." + suffix;
    }

    // --- Sorties 
    public static final String DLQ = out("tempo.dlq");
    public static final String SKIP_RATE = out("tempo.skiprate");
    public static final String TOP_BY_COUNTRY = out("tempo.top.by-country");
    public static final String ROYALTIES = out("tempo.royalties");
    public static final String ALERTS_FRAUD = out("tempo.alerts.fraud");
}
