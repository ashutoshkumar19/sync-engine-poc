import '../../common/suppress-warnings';
import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { SyncConsumerModule } from './sync.module';

async function bootstrap() {
  const logger = new Logger('SyncConsumer');
  const app = await NestFactory.create(SyncConsumerModule);

  const config = new DocumentBuilder()
    .setTitle('Sync Consumer Service')
    .setDescription('Dispatches staged records to destination systems with rate limiting and circuit breakers')
    .setVersion('1.0')
    .build();
  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup('docs', app, document);

  const port = process.env.PORT || 6001;
  await app.listen(port);
  logger.log(`Sync Consumer Service running on port ${port}`);
  logger.log(`Health endpoint: http://localhost:${port}/health`);
}

bootstrap();
