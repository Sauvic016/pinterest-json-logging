export const databaseUrl = process.env.DATABASE_URL ?? 'postgresql://logging:local-logging-only@127.0.0.1:5433/logs';
export const kafkaBrokers = (process.env.KAFKA_BROKERS ?? '127.0.0.1:9092').split(',');
export const kafkaTopic = 'client-events';
export const kafkaDeadLetterTopic = 'client-events-dlq';
export const opensearchUrl = process.env.OPENSEARCH_URL ?? 'http://127.0.0.1:9200';
