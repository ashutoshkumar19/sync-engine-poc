export function hashPartition(key: string, numPartitions: number = 10): number {
  let hash = 5381;
  for (let i = 0; i < key.length; i++) {
    hash = (hash * 33) ^ key.charCodeAt(i);
  }
  return Math.abs(hash) % numPartitions;
}

export function parseTimestamp(ts?: string | number): number {
  if (!ts) return Date.now();
  if (typeof ts === 'number') return ts;
  const parsed = Date.parse(ts);
  return isNaN(parsed) ? Date.now() : parsed;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
