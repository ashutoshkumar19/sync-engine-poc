import '../../common/suppress-warnings';
import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { NormalizationConsumerModule } from './normalization.module';

async function bootstrap() {
  const logger = new Logger('NormalizationConsumer');
  const app = await NestFactory.create(NormalizationConsumerModule);

  const config = new DocumentBuilder()
    .setTitle('Normalization Consumer Service')
    .setDescription('Ingests raw CDC & Webhooks, normalizes to canonical sync events')
    .setVersion('1.0')
    .build();
  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup('docs', app, document);

  const port = process.env.PORT || 4001;
  await app.listen(port);
  logger.log(`Normalization Consumer Service running on port ${port}`);
  logger.log(`Health endpoint: http://localhost:${port}/health`);
}

bootstrap();
