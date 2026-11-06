import '../../common/suppress-warnings';
import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { SchemaMappingConsumerModule } from './schema-mapping.module';

async function bootstrap() {
  const logger = new Logger('SchemaMappingConsumer');
  const app = await NestFactory.create(SchemaMappingConsumerModule);

  const config = new DocumentBuilder()
    .setTitle('Schema Mapping Consumer Service')
    .setDescription('Consumes canonical sync events, resolves field conflicts, detects echoes, and stages events')
    .setVersion('1.0')
    .build();
  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup('docs', app, document);

  const port = process.env.PORT || 5001;
  await app.listen(port);
  logger.log(`Schema Mapping Consumer Service running on port ${port}`);
  logger.log(`Health endpoint: http://localhost:${port}/health`);
}

bootstrap();
