/**
 * Standalone Test Runner & Verification Engine
 * Runs all unit tests and flow tests without requiring external network access
 */
const fs = require('fs');
const path = require('path');
const babel = require('@babel/core');

// 1. Jest-compatible test registry supporting nested suites, async beforeEach/afterEach
const rootSuite = {
  name: '',
  beforeHooks: [],
  afterHooks: [],
  tests: [],
  suites: [],
  parent: null,
};

let activeSuite = rootSuite;

global.describe = (name, fn) => {
  const newSuite = {
    name,
    beforeHooks: [],
    afterHooks: [],
    tests: [],
    suites: [],
    parent: activeSuite,
  };
  activeSuite.suites.push(newSuite);
  const prevSuite = activeSuite;
  activeSuite = newSuite;

  try {
    fn();
  } catch (err) {
    console.error(`Error loading suite ${name}:`, err);
  } finally {
    activeSuite = prevSuite;
  }
};

global.beforeEach = (fn) => {
  activeSuite.beforeHooks.push(fn);
};

global.afterEach = (fn) => {
  activeSuite.afterHooks.push(fn);
};

global.it = (name, fn) => {
  activeSuite.tests.push({ name, fn });
};

global.expect = (actual) => ({
  toBe: (expected) => {
    if (actual !== expected) {
      throw new Error(`Expected ${JSON.stringify(expected)}, received ${JSON.stringify(actual)}`);
    }
  },
  toEqual: (expected) => {
    const actStr = JSON.stringify(actual);
    const expStr = JSON.stringify(expected);
    if (actStr !== expStr) {
      throw new Error(`Expected ${expStr}, received ${actStr}`);
    }
  },
  toBeDefined: () => {
    if (actual === undefined || actual === null) {
      throw new Error(`Expected value to be defined, received ${actual}`);
    }
  },
  toBeUndefined: () => {
    if (actual !== undefined) {
      throw new Error(`Expected undefined, received ${JSON.stringify(actual)}`);
    }
  },
  toContain: (item) => {
    if (!Array.isArray(actual) && typeof actual !== 'string') {
      throw new Error(`Expected collection, received ${typeof actual}`);
    }
    if (!actual.includes(item)) {
      throw new Error(`Expected collection to contain ${JSON.stringify(item)}, received ${JSON.stringify(actual)}`);
    }
  },
  toBeGreaterThan: (val) => {
    if (!(actual > val)) {
      throw new Error(`Expected ${actual} > ${val}`);
    }
  },
  toBeGreaterThanOrEqual: (val) => {
    if (!(actual >= val)) {
      throw new Error(`Expected ${actual} >= ${val}`);
    }
  },
});

global.jest = {
  spyOn: (obj, method) => {
    const original = obj[method];
    let customImpl = original;
    const mock = function (...args) {
      return customImpl.apply(this, args);
    };
    mock.mockImplementation = (fn) => {
      customImpl = fn;
      return mock;
    };
    obj[method] = mock;
    return mock;
  },
};

// 2. Mock missing npm packages for in-VM test execution
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
        log() {}
        warn() {}
        error() {}
        debug() {}
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
        return Promise.reject(new Error('Live Redis unavailable in test mode'));
      }
    };
  }

  if (id === 'pg') {
    return {
      Pool: class MockPool {
        connect() {
          return Promise.reject(new Error('Live PostgreSQL unavailable in test mode'));
        }
      },
    };
  }

  return originalRequire.apply(this, arguments);
};

// 3. Setup on-the-fly TypeScript transpiler via Babel
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

// Collect ancestry chain for hooks
function getAncestry(suite) {
  const chain = [];
  let curr = suite;
  while (curr) {
    chain.unshift(curr);
    curr = curr.parent;
  }
  return chain;
}

// 4. Test Suite Execution
async function executeSuite(suite, prefix = '') {
  let passed = 0;
  let failed = 0;
  const suiteName = prefix ? (suite.name ? `${prefix} > ${suite.name}` : prefix) : suite.name;

  const ancestry = getAncestry(suite);
  const beforeHooks = ancestry.flatMap((s) => s.beforeHooks);
  const afterHooks = ancestry.flatMap((s) => s.afterHooks).reverse();

  for (const test of suite.tests) {
    const testFullName = suiteName ? `${suiteName} > ${test.name}` : test.name;

    try {
      for (const hook of beforeHooks) {
        await hook();
      }

      await test.fn();

      for (const hook of afterHooks) {
        await hook();
      }

      passed++;
      console.log(`  ✓ ${testFullName}`);
    } catch (err) {
      failed++;
      console.error(`  ✗ ${testFullName}`);
      console.error(`    ${err.message}`);
    }
  }

  for (const childSuite of suite.suites) {
    const res = await executeSuite(childSuite, suiteName);
    passed += res.passed;
    failed += res.failed;
  }

  return { passed, failed };
}

async function runAllTests() {
  console.log('====================================================');
  console.log('Bi-Directional Record Synchronization Platform');
  console.log('Running Complete Unit & End-to-End Test Suite');
  console.log('====================================================\n');

  const testDir = path.join(__dirname);
  const testFiles = fs.readdirSync(testDir).filter((f) => f.endsWith('.spec.ts'));

  for (const file of testFiles) {
    console.log(`▶ Loading Test Suite: ${file}`);
    require(path.join(testDir, file));
  }

  console.log('\n▶ Executing Registered Test Suites...\n');
  const results = await executeSuite(rootSuite);

  console.log('\n====================================================');
  console.log(`Test Results Summary:`);
  console.log(`  Total:  ${results.passed + results.failed}`);
  console.log(`  Passed: ${results.passed}`);
  console.log(`  Failed: ${results.failed}`);
  console.log('====================================================');

  if (results.failed > 0) {
    process.exit(1);
  }
}

runAllTests().catch((err) => {
  console.error('Fatal test runner error:', err);
  process.exit(1);
});
