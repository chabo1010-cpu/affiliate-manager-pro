import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import Database from 'better-sqlite3';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dataDir = path.join(__dirname, 'data');
const dbPath = path.join(dataDir, 'deals.db');
console.log('DB FILE PATH', dbPath);

if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const db = new Database(dbPath);

db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS deals_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asin TEXT,
    url TEXT NOT NULL,
    normalizedUrl TEXT NOT NULL,
    title TEXT,
    price TEXT,
    oldPrice TEXT,
    sellerType TEXT NOT NULL DEFAULT 'FBM',
    postedAt TEXT NOT NULL,
    channel TEXT,
    couponCode TEXT
  );

  CREATE INDEX IF NOT EXISTS idx_deals_history_asin ON deals_history (asin);
  CREATE INDEX IF NOT EXISTS idx_deals_history_normalizedUrl ON deals_history (normalizedUrl);
  CREATE INDEX IF NOT EXISTS idx_deals_history_postedAt ON deals_history (postedAt DESC);

  CREATE TABLE IF NOT EXISTS settings (
    repostCooldownEnabled INTEGER NOT NULL DEFAULT 1,
    repostCooldownHours INTEGER NOT NULL DEFAULT 12,
    telegramCopyButtonText TEXT NOT NULL DEFAULT '📋 Zum Kopieren hier klicken'
  );

  CREATE TABLE IF NOT EXISTS app_settings (
    id INTEGER PRIMARY KEY,
    repostCooldownEnabled INTEGER NOT NULL DEFAULT 1,
    repostCooldownHours INTEGER NOT NULL DEFAULT 12,
    telegramCopyButtonText TEXT NOT NULL DEFAULT '📋 Zum Kopieren hier klicken'
  );
`);

const settingsColumns = db.prepare(`PRAGMA table_info(settings)`).all();
const hasCooldownEnabledColumn = settingsColumns.some((column) => column.name === 'repostCooldownEnabled');
if (!hasCooldownEnabledColumn) {
  db.exec(`ALTER TABLE settings ADD COLUMN repostCooldownEnabled INTEGER NOT NULL DEFAULT 1`);
}
const hasTelegramCopyButtonTextColumn = settingsColumns.some((column) => column.name === 'telegramCopyButtonText');
if (!hasTelegramCopyButtonTextColumn) {
  db.exec(`ALTER TABLE settings ADD COLUMN telegramCopyButtonText TEXT NOT NULL DEFAULT '📋 Zum Kopieren hier klicken'`);
}

const row = db.prepare(`SELECT COUNT(*) AS count FROM settings`).get();
if (!row?.count) {
  db.prepare(
    `INSERT INTO settings (repostCooldownEnabled, repostCooldownHours, telegramCopyButtonText) VALUES (1, 12, '📋 Zum Kopieren hier klicken')`
  ).run();
} else {
  db.prepare(`DELETE FROM settings WHERE rowid NOT IN (SELECT MIN(rowid) FROM settings)`).run();
  db.prepare(
    `
      UPDATE settings
      SET repostCooldownEnabled = COALESCE(repostCooldownEnabled, 1),
          repostCooldownHours = COALESCE(repostCooldownHours, 12),
          telegramCopyButtonText = COALESCE(NULLIF(TRIM(telegramCopyButtonText), ''), '📋 Zum Kopieren hier klicken')
    `
  ).run();
}

const appSettingsColumns = db.prepare(`PRAGMA table_info(app_settings)`).all();
const hasAppTelegramCopyButtonTextColumn = appSettingsColumns.some((column) => column.name === 'telegramCopyButtonText');
if (!hasAppTelegramCopyButtonTextColumn) {
  db.exec(`ALTER TABLE app_settings ADD COLUMN telegramCopyButtonText TEXT NOT NULL DEFAULT '📋 Zum Kopieren hier klicken'`);
}

const appSettingsRow = db.prepare(`SELECT COUNT(*) AS count FROM app_settings`).get();
if (!appSettingsRow?.count) {
  const legacySettings = db
    .prepare(`SELECT repostCooldownEnabled, repostCooldownHours FROM settings LIMIT 1`)
    .get();

  db.prepare(
    `
      INSERT INTO app_settings (id, repostCooldownEnabled, repostCooldownHours, telegramCopyButtonText)
      VALUES (1, ?, ?, ?)
    `
  ).run(
    legacySettings?.repostCooldownEnabled ?? 1,
    legacySettings?.repostCooldownHours ?? 12,
    legacySettings?.telegramCopyButtonText ?? '📋 Zum Kopieren hier klicken'
  );
} else {
  db.prepare(`DELETE FROM app_settings WHERE id != 1`).run();
  db.prepare(
    `
      UPDATE app_settings
      SET repostCooldownEnabled = COALESCE(repostCooldownEnabled, 1),
          repostCooldownHours = COALESCE(repostCooldownHours, 12),
          telegramCopyButtonText = COALESCE(NULLIF(TRIM(telegramCopyButtonText), ''), '📋 Zum Kopieren hier klicken')
      WHERE id = 1
    `
  ).run();
}

export function getDb() {
  return db;
}
