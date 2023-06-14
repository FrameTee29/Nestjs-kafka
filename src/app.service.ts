import { Injectable, OnModuleInit } from '@nestjs/common';
import { KafkaService } from './lib/kafka/kafka.service';

@Injectable()
export class AppService implements OnModuleInit {
  constructor(private readonly kafkaService: KafkaService) {}

  async onModuleInit() {
    this.setUpConsumer();
  }

  private async setUpConsumer() {
    await this.kafkaService.subscribe(
      { topics: ['test-kafka'] },
      async ({ message }) => {
        console.log('App Service =>', JSON.parse(message.value.toString()));
      },
    );
  }

  getHello(): string {
    this.kafkaService.sendMessage({
      topic: 'test-kafka',
      messages: [
        {
          value: JSON.stringify({
            account_type: '2',
            amount: new Date(),
            payment_type: 'date',
          }),
        },
      ],
    });
    return 'Hello World!';
  }
}
