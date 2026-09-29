# Taxi Streaming Lakehouse

Ce projet est une preuve de concept (PoC) d'une architecture Big Data en temps reel (Streaming Lakehouse). Il simule l'ingestion de trajets de taxis en continu, leur transit via un bus de messages, et leur stockage persistant dans un format ouvert oriente analytique, permettant des requetes SQL a la volee.

## Architecture

Le pipeline de donnees suit une architecture "Medallion" (couche Bronze) de bout en bout :

1. Producteur (Python) : Genere aleatoirement des donnees de trajets de taxi (ID, vendor, horodatage, passagers, distance, montant) au format JSON.
2. Message Broker (Apache Kafka) : Recoit et met en file d'attente les evenements en temps reel via un topic dedie (taxi-trips-stream).
3. Processing (Apache Spark) : Spark Structured Streaming consomme les messages Kafka en continu.
4. Stockage (Apache Iceberg) : Les donnees sont ecrites dans un catalogue Iceberg local (format Parquet), garantissant des transactions ACID et l'evolution du schema.
5. Analytique (Spark SQL) : Interrogation de la table Iceberg en temps reel pour extraire des KPIs (revenus, distances moyennes).

## Stack Technique

* Langage : Python 3, Java 17
* Infrastructure : Docker Compose (Kafka en mode KRaft)
* Data Streaming : Apache Kafka 3.8.0, PySpark 3.5.3
* Data Lakehouse : Apache Iceberg 1.4.3



Guide d'execution
1. Environnement

Creez et activez un environnement virtuel Python pour isoler les dependances :
Bash

python3 -m venv venv
source venv/bin/activate
pip install kafka-python pyspark

2. Demarrer l'infrastructure Kafka

Lancez le broker Kafka en arriere-plan et creez le topic de streaming :
Bash

docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic taxi-trips-stream \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

3. Lancer le Simulateur (Producer)

Dans un premier terminal, executez le producteur pour generer le flux de donnees :
Bash

python producer.py

(Laissez ce terminal ouvert)
4. Ingerer les donnees vers Apache Iceberg (Writer)

Dans un deuxieme terminal, lancez le job Spark Streaming pour ecrire dans le dossier warehouse :
Bash

export PYSPARK_PYTHON=./venv/bin/python
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 lakehouse_writer.py

(Laissez ce terminal ouvert)
5. Requeter le Lakehouse en temps reel (Reader)

Dans un troisieme terminal, lancez des requetes analytiques sur la donnee fraichement ingeree :
Bash

export PYSPARK_PYTHON=./venv/bin/python
spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 lakehouse_reader.py

6. Nettoyage

Pour tout arreter proprement une fois les tests termines :
Bash

docker compose down