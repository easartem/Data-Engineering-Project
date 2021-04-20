@echo Start Kafka for Peaceland Project


@echo Kafka Initialization
start cmd.exe /k "echo Run Zookeeper & cd C:\Apache\apache-zookeeper-3.6.1-bin\bin & zkserver"
timeout /t 20 /nobreak > NUL
start cmd.exe /k "echo Run Kafka server & cd C:\Apache\kafka_2.13-2.7.0\bin\windows & kafka-server-start.bat C:\Apache\kafka_2.13-2.7.0\config\server.properties"
timeout /t 5 /nobreak > NUL
start cmd.exe /c "echo Create Kafka Topics : Peaceland-STORAGE,Peaceland-TEST,Peaceland-ALERT & cd C:\Apache\kafka_2.13-2.7.0\bin\windows & kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Peaceland-STORAGE & kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Peaceland-TEST & kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Peaceland-ALERT"
@echo End of Kafka Initialization

@echo Connection to MongoDB Atlas
start cmd.exe /k "cd C:\Apache\kafka_2.13-2.7.0\bin\windows & connect-standalone.bat C:\Apache\kafka_2.13-2.7.0\config\connect-standalone.properties C:\Apache\kafka_2.13-2.7.0\config\MongoSinkConnector.properties"
@echo End of Connection to MongoDB Atlas
