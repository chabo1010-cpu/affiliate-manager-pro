import Database from 'better-sqlite3';
import { isMainThread, parentPort, workerData } from 'node:worker_threads';

const [cliDbPath, cliHoldMsRaw] = process.argv.slice(2);
const dbPath = workerData?.dbPath || cliDbPath;
const holdMs = Number.parseInt(String(workerData?.holdMs ?? cliHoldMsRaw ?? '200'), 10) || 200;

if (!dbPath) {
  process.exit(1);
}

const db = new Database(dbPath);

try {
  db.pragma('busy_timeout = 50');
  db.exec('BEGIN EXCLUSIVE');
  if (isMainThread) {
    process.stdout.write('LOCK_READY\n');
  } else {
    parentPort?.postMessage('LOCK_READY');
  }

  setTimeout(() => {
    try {
      db.exec('COMMIT');
    } catch {
      try {
        db.exec('ROLLBACK');
      } catch {
        // no-op
      }
    }

    try {
      db.close();
    } catch {
      // no-op
    }

    if (isMainThread) {
      process.exit(0);
    } else {
      parentPort?.postMessage('LOCK_RELEASED');
    }
  }, holdMs);
} catch {
  try {
    db.close();
  } catch {
    // no-op
  }

  process.exit(1);
}
