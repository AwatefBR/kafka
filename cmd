# ══════════════ 0. INSTALLATION (une seule fois) ══════════════
pyenv global 3.12.7
cd "/Users/prodiges3/Desktop/awatef kafka/generateurs-v3"
python -m pip install -r requirements.txt

# ══════════════ 1. CLUSTER KAFKA — Terminal 1 ══════════════
cd "/Users/prodiges3/Desktop/awatef kafka/projet-tempo"
docker compose up -d
docker ps                                   # vérifier : kafka-1, kafka-2, kafka-3, kafbat-ui
# Kafbat UI → http://localhost:8080

# ══════════════ 2. GÉNÉRATEUR — Terminal 2 ══════════════
cd "/Users/prodiges3/Desktop/awatef kafka/generateurs-v3"
python generator.py --project tempo --create-topics --replication-factor 3   # 1re fois
python generator.py --project tempo                                          # fois suivantes
python generator.py --project tempo --dirty-rate 0 --late-rate 0 --dup-rate 0   # flux propre (debug)
python generator.py --project tempo --dry-run --limit 30                     # aperçu sans Kafka

# ══════════════ 3. APPLICATION KAFKA STREAMS — Terminal 3 ══════════════
cd "/Users/prodiges3/Desktop/awatef kafka/projet-tempo"
GROUPE=grp07 TEMPO_FRAUD_WINDOW_MINUTES=5 mvn quarkus:dev
# attendre : "State transition from REBALANCING to RUNNING"

# ══════════════ 4. LIRE LES RÉSULTATS — Terminal 4 ══════════════
cd "/Users/prodiges3/Desktop/awatef kafka/projet-tempo"
# TMP-1 DLQ
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic grp07.tempo.dlq --from-beginning --isolation-level read_committed --property print.key=true
# TMP-2 Skip rate
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic grp07.tempo.skiprate --from-beginning --isolation-level read_committed --property print.key=true
# TMP-3 Top par pays
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic grp07.tempo.top.by-country --from-beginning --isolation-level read_committed --property print.key=true
# TMP-4 Royalties
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic grp07.tempo.royalties --from-beginning --isolation-level read_committed --property print.key=true
# TMP-5 Fraude
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic grp07.tempo.alerts.fraud --from-beginning --isolation-level read_committed --property print.key=true
# Ne voir que les flops (TMP-2) :
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic grp07.tempo.skiprate --from-beginning --isolation-level read_committed | grep '"flop":true'
# Lister tous les topics :
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --list

# ══════════════ 5. TESTS & BUILD (TMP-6) ══════════════
cd "/Users/prodiges3/Desktop/awatef kafka/projet-tempo"
mvn test
mvn package
GROUPE=grp07 TEMPO_FRAUD_WINDOW_MINUTES=5 java -jar target/quarkus-app/quarkus-run.jar   # alternative à quarkus:dev

# ══════════════ 6. SIMULER 3 H DE FLUX EN QUELQUES SECONDES ══════════════
cd "/Users/prodiges3/Desktop/awatef kafka/generateurs-v3"
python generator.py --project tempo --to-dir /tmp/tempo-data --minutes 180
python rejouer.py --dossier /tmp/tempo-data --project tempo --create-topics --replication-factor 3 --bootstrap localhost:29092

# ══════════════ 7. CLUSTER PARTAGÉ DE L'ÉCOLE (rendu) ══════════════
# le générateur y tourne déjà → lancer seulement l'appli
cd "/Users/prodiges3/Desktop/awatef kafka/projet-tempo"
GROUPE=grp07 KAFKA_BOOTSTRAP=<serveur>:9092 mvn quarkus:dev

# ══════════════ 8. REPARTIR DE ZÉRO (recalculer tous les résultats) ══════════════
# arrêter l'appli (Ctrl+C), puis :
cd "/Users/prodiges3/Desktop/awatef kafka/projet-tempo"
rm -rf .tempo-state
docker compose down          # supprime aussi tous les topics et messages
docker compose up -d         # puis reprendre aux étapes 2 et 3

# ══════════════ 9. TOUT ARRÊTER ══════════════
# Ctrl+C dans le terminal 2 (générateur) et le terminal 3 (appli)
cd "/Users/prodiges3/Desktop/awatef kafka/projet-tempo"
docker compose down
