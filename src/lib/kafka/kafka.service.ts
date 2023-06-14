import { Injectable, OnModuleInit } from '@nestjs/common';
import {
  Kafka,
  Consumer,
  Producer,
  EachMessagePayload,
  ProducerRecord,
  ConsumerSubscribeTopics,
} from 'kafkajs';

@Injectable()
export class KafkaService implements OnModuleInit {
  private kafka: Kafka;
  private consumer: Consumer;
  private producer: Producer;

  async onModuleInit() {
    await this.initializeKafka();
    await this.initializeConsumer();
    await this.initializeProducer();
  }

  private async initializeKafka() {
    this.kafka = new Kafka({
      brokers: ['localhost:8097', 'localhost:8098', 'localhost:8099'],
    });
  }

  private async initializeConsumer() {
    this.consumer = this.kafka.consumer({ groupId: 'kafka-consumer' });

    await this.consumer.connect();
    console.log('Kafka consumer is ready');
  }

  private async initializeProducer() {
    this.producer = this.kafka.producer();

    await this.producer.connect();
    console.log('Kafka producer is ready');
  }

  public async subscribe(
    topic: ConsumerSubscribeTopics,
    callback: (payload: EachMessagePayload) => Promise<void>,
  ) {
    if (this.consumer === undefined) {
      await this.initializeKafka();
      await this.initializeConsumer();
    }

    await this.consumer.connect();

    await this.consumer.subscribe(topic);

    await this.consumer.run({
      eachMessage: async (payload) => {
        await callback(payload);
      },
    });

    console.log('Subscribe');
  }

  public async sendMessage(record: ProducerRecord) {
    await this.producer.send(record);

    console.log('Message sent');
  }
}
