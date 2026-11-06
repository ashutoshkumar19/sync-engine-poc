// Fix: Suppress Node.js TimeoutNegativeWarning caused by kafkajs internal requestQueue calculation
const origSetTimeout = global.setTimeout;
const patchTimeout = (callback: (...args: any[]) => void, ms?: number, ...args: any[]) =>
  origSetTimeout(callback, typeof ms === 'number' && ms < 0 ? 0 : ms, ...args);

(global as any).setTimeout = patchTimeout;

try {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const timers = require('timers');
  timers.setTimeout = patchTimeout;
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const nodeTimers = require('node:timers');
  nodeTimers.setTimeout = patchTimeout;
} catch {}

const origEmitWarning = process.emitWarning;
process.emitWarning = function (warning: any, ...args: any[]) {
  if (
    (typeof warning === 'string' && warning.includes('TimeoutNegativeWarning')) ||
    args[0] === 'TimeoutNegativeWarning' ||
    (typeof warning === 'object' && warning?.name === 'TimeoutNegativeWarning')
  ) {
    return;
  }
  return (origEmitWarning as any).call(process, warning, ...args);
};
