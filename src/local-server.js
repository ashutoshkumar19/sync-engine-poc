/**
 * HTTP Server Runner
 * Exposes the DemoController and APIs over HTTP for end-to-end API demonstration
 */
const http = require('http');
const url = require('url');
const fs = require('fs');
const path = require('path');
const babel = require('@babel/core');

// 1. Shims for NestJS
const Module = require('module');
const originalRequire = Module.prototype.require;

Module.prototype.require = function (id) {
  if (id === '@nestjs/common') {
    return {
      Injectable: () => (t) => t,
      Module: () => (t) => t,
      Controller: () => (t) => t,
      Get: () => (t, k, d) => d,
      Post: () => (t, k, d) => d,
      Body: () => () => {},
      Param: () => () => {},
      Query: () => () => {},
      HttpCode: () => (t, k, d) => d,
      HttpStatus: { ACCEPTED: 202, OK: 200, BAD_REQUEST: 400 },
      Logger: class {
        log(msg) { console.log(`[LOG] ${msg}`); }
        warn(msg) { console.warn(`[WARN] ${msg}`); }
        error(msg) { console.error(`[ERROR] ${msg}`); }
        debug(msg) { console.debug(`[DEBUG] ${msg}`); }
      },
      ValidationPipe: class {},
    };
  }

  if (id === '@nestjs/core') {
    return {
      NestFactory: {
        create: async () => ({
          useGlobalPipes: () => {},
          listen: async () => {},
        }),
      },
    };
  }

  if (id === 'kafkajs') {
    return {
      Kafka: class {
        producer() {
          return {
            connect: async () => {},
            disconnect: async () => {},
            send: async () => {},
          };
        }
        consumer() {
          return {
            connect: async () => {},
            subscribe: async () => {},
            run: async () => {},
          };
        }
      },
    };
  }

  if (id === 'ioredis') {
    return class MockRedis {
      connect() {
        return Promise.reject(new Error('Live Redis unavailable'));
      }
    };
  }

  if (id === 'pg') {
    return {
      Pool: class MockPool {
        connect() {
          return Promise.reject(new Error('Live PostgreSQL unavailable'));
        }
      },
    };
  }

  return originalRequire.apply(this, arguments);
};

// 2. TypeScript compilation hook
require.extensions['.ts'] = function (module, filename) {
  let content = fs.readFileSync(filename, 'utf8');
  content = content.replace(/@\w+\([^)]*\)\s*([a-zA-Z0-9_$]+)/g, '$1');

  const transformed = babel.transformSync(content, {
    presets: [
      '/usr/share/nodejs/@babel/preset-typescript',
      ['/usr/share/nodejs/@babel/preset-env', { targets: { node: 'current' } }],
    ],
    plugins: [
      ['/usr/share/nodejs/@babel/plugin-proposal-decorators', { legacy: true }],
      ['/usr/share/nodejs/@babel/plugin-proposal-class-properties', { loose: true }],
    ],
    filename,
  });
  return module._compile(transformed.code, filename);
};

// 3. Instantiate Architecture Components
const { KafkaService } = require('./infra/kafka.service.ts');
const { DatabaseService } = require('./infra/database.service.ts');
const { RedisService } = require('./infra/redis.service.ts');
const { CircuitBreakerService } = require('./infra/circuit-breaker.service.ts');
const { SystemAAdapter } = require('./adapters/system-a.adapter.ts');
const { SystemBAdapter } = require('./adapters/system-b.adapter.ts');
const { EchoDetectorService } = require('./engine/echo-detector.service.ts');
const { ConflictResolverService } = require('./engine/conflict-resolver.service.ts');
const { OutboundValidatorService } = require('./engine/outbound-validator.service.ts');
const { NormalizationConsumer } = require('./consumers/normalization.consumer.ts');
const { SchemaMappingConsumer } = require('./consumers/schema-mapping.consumer.ts');
const { SyncConsumer } = require('./consumers/sync.consumer.ts');
const { JobSchedulerService } = require('./scheduler/job-scheduler.service.ts');
const { DemoController } = require('./controllers/demo.controller.ts');

async function createServer() {
  const kafkaService = new KafkaService();
  const dbService = new DatabaseService();
  const redisService = new RedisService();
  const circuitBreaker = new CircuitBreakerService();

  const systemAAdapter = new SystemAAdapter(dbService);
  const systemBAdapter = new SystemBAdapter(redisService, circuitBreaker, dbService);

  const echoDetector = new EchoDetectorService(dbService);
  const conflictResolver = new ConflictResolverService(dbService);
  const outboundValidator = new OutboundValidatorService(dbService);

  const normalizationConsumer = new NormalizationConsumer(kafkaService, systemAAdapter, systemBAdapter);
  const schemaMappingConsumer = new SchemaMappingConsumer(
    kafkaService,
    echoDetector,
    conflictResolver,
    dbService,
    systemAAdapter,
    systemBAdapter,
  );
  const syncConsumer = new SyncConsumer(
    kafkaService,
    dbService,
    outboundValidator,
    systemAAdapter,
    systemBAdapter,
  );
  const scheduler = new JobSchedulerService(kafkaService);

  // Initialize consumers
  await normalizationConsumer.onModuleInit();
  await schemaMappingConsumer.onModuleInit();
  await syncConsumer.onModuleInit();

  const controller = new DemoController(
    kafkaService,
    dbService,
    redisService,
    circuitBreaker,
    scheduler,
    systemAAdapter,
    systemBAdapter,
    normalizationConsumer,
    schemaMappingConsumer,
    syncConsumer,
  );

  const server = http.createServer(async (req, res) => {
    const parsedUrl = url.parse(req.url, true);
    const pathname = parsedUrl.pathname;
    const method = req.method;

    const readBody = () =>
      new Promise((resolve) => {
        let body = '';
        req.on('data', (chunk) => (body += chunk));
        req.on('end', () => {
          try {
            resolve(body ? JSON.parse(body) : {});
          } catch {
            resolve({});
          }
        });
      });

    const sendJson = (statusCode, data) => {
      res.writeHead(statusCode, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data, null, 2));
    };

    try {
      // 1. POST /demo/emit/system-a
      if (pathname === '/demo/emit/system-a' && method === 'POST') {
        const body = await readBody();
        const result = await controller.emitSystemA(body);
        return sendJson(202, result);
      }

      // 2. POST /demo/emit/system-b
      if (pathname === '/demo/emit/system-b' && method === 'POST') {
        const body = await readBody();
        const result = await controller.emitSystemB(body);
        return sendJson(202, result);
      }

      // 3. POST /demo/emit/both
      if (pathname === '/demo/emit/both' && method === 'POST') {
        const body = await readBody();
        const result = await controller.emitBoth(body);
        return sendJson(202, result);
      }

      // 4. POST /demo/scheduler/start
      if (pathname === '/demo/scheduler/start' && method === 'POST') {
        const body = await readBody();
        const result = controller.startScheduler(body);
        return sendJson(200, result);
      }

      // 5. POST /demo/scheduler/stop
      if (pathname === '/demo/scheduler/stop' && method === 'POST') {
        const result = controller.stopScheduler();
        return sendJson(200, result);
      }

      // 6. POST /demo/scheduler/trigger
      if (pathname === '/demo/scheduler/trigger' && method === 'POST') {
        const result = await controller.triggerScheduler(parsedUrl.query.pipeline_id);
        return sendJson(200, result);
      }

      // 7. GET /demo/sync-events
      if (pathname === '/demo/sync-events' && method === 'GET') {
        const result = await controller.getSyncEvents(
          parsedUrl.query.pipeline_id,
          parsedUrl.query.status,
          parsedUrl.query.record_id,
        );
        return sendJson(200, result);
      }

      // 8. GET /demo/sync-metadata/:record_id
      if (pathname.startsWith('/demo/sync-metadata/') && method === 'GET') {
        const recordId = pathname.replace('/demo/sync-metadata/', '');
        const result = await controller.getSyncMetadata(
          recordId,
          parsedUrl.query.tenant_id,
          parsedUrl.query.pipeline_id,
        );
        return sendJson(200, result);
      }

      // 9. GET /demo/records/system-a/:id
      if (pathname.startsWith('/demo/records/system-a/') && method === 'GET') {
        const id = pathname.replace('/demo/records/system-a/', '');
        const result = await controller.getSystemARecord(id);
        return sendJson(200, result);
      }

      // 10. GET /demo/records/system-b/:id
      if (pathname.startsWith('/demo/records/system-b/') && method === 'GET') {
        const id = pathname.replace('/demo/records/system-b/', '');
        const result = await controller.getSystemBRecord(id);
        return sendJson(200, result);
      }

      // 11. GET /demo/status
      if (pathname === '/demo/status' && method === 'GET') {
        const result = await controller.getStatus();
        return sendJson(200, result);
      }

      // 12. POST /demo/scenarios/:name
      if (pathname.startsWith('/demo/scenarios/') && method === 'POST') {
        const scenarioName = pathname.replace('/demo/scenarios/', '');
        const result = await controller.runScenario(scenarioName);
        return sendJson(200, result);
      }

      sendJson(404, { error: 'Not Found', pathname });
    } catch (err) {
      sendJson(500, { error: err.message, stack: err.stack });
    }
  });

  const port = process.env.PORT || 3000;
  server.listen(port, () => {
    console.log(`Server listening on port ${port}`);
  });
}

createServer().catch(console.error);
