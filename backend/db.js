import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import Database from 'better-sqlite3';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dataDir = path.join(__dirname, 'data');
const dbPath = path.join(dataDir, 'deals.db');
const DEFAULT_TELEGRAM_COPY_BUTTON_TEXT = '📋 Zum Kopieren hier klicken';

console.log('DB FILE PATH', dbPath);

if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const db = new Database(dbPath);

db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

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

  CREATE TABLE IF NOT EXISTS pricing_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    is_active INTEGER NOT NULL DEFAULT 1,
    keepa_required INTEGER NOT NULL DEFAULT 0,
    idealo_required INTEGER NOT NULL DEFAULT 0,
    autopost_above_score REAL NOT NULL DEFAULT 85,
    manual_review_below_score REAL NOT NULL DEFAULT 45,
    allow_amazon INTEGER NOT NULL DEFAULT 1,
    min_discount_amazon REAL NOT NULL DEFAULT 15,
    min_score_amazon REAL NOT NULL DEFAULT 70,
    sampling_amazon REAL NOT NULL DEFAULT 100,
    max_price_gap_idealo_amazon REAL,
    allow_fba INTEGER NOT NULL DEFAULT 1,
    min_discount_fba REAL NOT NULL DEFAULT 20,
    min_score_fba REAL NOT NULL DEFAULT 75,
    sampling_fba REAL NOT NULL DEFAULT 60,
    max_price_gap_idealo_fba REAL,
    allow_fbm INTEGER NOT NULL DEFAULT 1,
    min_discount_fbm REAL NOT NULL DEFAULT 40,
    min_score_fbm REAL NOT NULL DEFAULT 82,
    sampling_fbm REAL NOT NULL DEFAULT 20,
    max_price_gap_idealo_fbm REAL,
    fbm_requires_manual_review INTEGER NOT NULL DEFAULT 1,
    min_seller_rating_fbm REAL,
    fake_drop_filter_enabled INTEGER NOT NULL DEFAULT 0,
    coupon_only_penalty REAL NOT NULL DEFAULT 0,
    variant_switch_penalty REAL NOT NULL DEFAULT 0,
    marketplace_switch_penalty REAL NOT NULL DEFAULT 0,
    manual_blacklist_keywords TEXT NOT NULL DEFAULT '[]',
    manual_whitelist_brands TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS sampling_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    is_active INTEGER NOT NULL DEFAULT 1,
    default_sampling REAL NOT NULL DEFAULT 100,
    sampling_amazon REAL NOT NULL DEFAULT 100,
    sampling_fba REAL NOT NULL DEFAULT 100,
    sampling_fbm REAL NOT NULL DEFAULT 100,
    daily_limit INTEGER,
    min_score REAL,
    min_discount REAL,
    notes TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    platform TEXT NOT NULL CHECK(platform IN ('telegram', 'whatsapp')),
    source_type TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    priority INTEGER NOT NULL DEFAULT 100,
    pricing_rule_id INTEGER NOT NULL,
    sampling_rule_id INTEGER,
    last_import_at TEXT,
    success_rate REAL,
    notes TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (pricing_rule_id) REFERENCES pricing_rules(id),
    FOREIGN KEY (sampling_rule_id) REFERENCES sampling_rules(id)
  );

  CREATE TABLE IF NOT EXISTS imported_deals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL,
    asin TEXT,
    original_url TEXT NOT NULL,
    normalized_url TEXT NOT NULL,
    title TEXT,
    current_price REAL,
    old_price REAL,
    seller_type TEXT NOT NULL DEFAULT 'FBM',
    detected_discount REAL,
    score REAL,
    keepa_result_json TEXT,
    comparison_result_json TEXT,
    status TEXT NOT NULL DEFAULT 'imported',
    review_reason TEXT,
    decision_reason TEXT,
    posted_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (source_id) REFERENCES sources(id)
  );

  CREATE TABLE IF NOT EXISTS copybot_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    level TEXT NOT NULL DEFAULT 'info',
    event_type TEXT NOT NULL,
    source_id INTEGER,
    imported_deal_id INTEGER,
    message TEXT NOT NULL,
    payload_json TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (source_id) REFERENCES sources(id),
    FOREIGN KEY (imported_deal_id) REFERENCES imported_deals(id)
  );

  CREATE TABLE IF NOT EXISTS generator_posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    product_link TEXT,
    telegram_text TEXT,
    whatsapp_text TEXT,
    facebook_text TEXT,
    generated_image_path TEXT,
    uploaded_image_path TEXT,
    telegram_image_source TEXT NOT NULL DEFAULT 'standard',
    whatsapp_image_source TEXT NOT NULL DEFAULT 'standard',
    facebook_image_source TEXT NOT NULL DEFAULT 'link_preview',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS publishing_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,
    source_id INTEGER,
    status TEXT NOT NULL DEFAULT 'queued',
    payload_json TEXT NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS publishing_targets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER NOT NULL,
    channel_type TEXT NOT NULL,
    is_enabled INTEGER NOT NULL DEFAULT 1,
    image_source TEXT NOT NULL DEFAULT 'none',
    status TEXT NOT NULL DEFAULT 'pending',
    posted_at TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (queue_id) REFERENCES publishing_queue(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS publishing_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER,
    target_id INTEGER,
    worker_type TEXT,
    level TEXT NOT NULL DEFAULT 'info',
    event_type TEXT NOT NULL,
    message TEXT NOT NULL,
    payload_json TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (queue_id) REFERENCES publishing_queue(id),
    FOREIGN KEY (target_id) REFERENCES publishing_targets(id)
  );

  CREATE TABLE IF NOT EXISTS settings (
    repostCooldownEnabled INTEGER NOT NULL DEFAULT 1,
    repostCooldownHours INTEGER NOT NULL DEFAULT 12,
    telegramCopyButtonText TEXT NOT NULL DEFAULT '${DEFAULT_TELEGRAM_COPY_BUTTON_TEXT}'
  );

  CREATE TABLE IF NOT EXISTS app_settings (
    id INTEGER PRIMARY KEY,
    repostCooldownEnabled INTEGER NOT NULL DEFAULT 1,
    repostCooldownHours INTEGER NOT NULL DEFAULT 12,
    telegramCopyButtonText TEXT NOT NULL DEFAULT '${DEFAULT_TELEGRAM_COPY_BUTTON_TEXT}',
    copybotEnabled INTEGER NOT NULL DEFAULT 0,
    facebookEnabled INTEGER NOT NULL DEFAULT 0,
    facebookSessionMode TEXT NOT NULL DEFAULT 'persistent',
    facebookDefaultRetryLimit INTEGER NOT NULL DEFAULT 3,
    facebookDefaultTarget TEXT
  );
`);

function ensureColumn(tableName, columnName, definition) {
  const columns = db.prepare(`PRAGMA table_info(${tableName})`).all();
  if (!columns.some((column) => column.name === columnName)) {
    db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${definition}`);
  }
}

ensureColumn('settings', 'repostCooldownEnabled', `repostCooldownEnabled INTEGER NOT NULL DEFAULT 1`);
ensureColumn(
  'settings',
  'telegramCopyButtonText',
  `telegramCopyButtonText TEXT NOT NULL DEFAULT '${DEFAULT_TELEGRAM_COPY_BUTTON_TEXT}'`
);

ensureColumn(
  'app_settings',
  'telegramCopyButtonText',
  `telegramCopyButtonText TEXT NOT NULL DEFAULT '${DEFAULT_TELEGRAM_COPY_BUTTON_TEXT}'`
);
ensureColumn('app_settings', 'copybotEnabled', `copybotEnabled INTEGER NOT NULL DEFAULT 0`);
ensureColumn('app_settings', 'facebookEnabled', `facebookEnabled INTEGER NOT NULL DEFAULT 0`);
ensureColumn('app_settings', 'facebookSessionMode', `facebookSessionMode TEXT NOT NULL DEFAULT 'persistent'`);
ensureColumn(
  'app_settings',
  'facebookDefaultRetryLimit',
  `facebookDefaultRetryLimit INTEGER NOT NULL DEFAULT 3`
);
ensureColumn('app_settings', 'facebookDefaultTarget', `facebookDefaultTarget TEXT`);

ensureColumn('deals_history', 'originalUrl', 'originalUrl TEXT');
ensureColumn('deals_history', 'productTitle', 'productTitle TEXT');
ensureColumn('deals_history', 'currentPrice', 'currentPrice TEXT');
ensureColumn('deals_history', 'oldPrice', 'oldPrice TEXT');
ensureColumn('deals_history', 'sellerType', `sellerType TEXT NOT NULL DEFAULT 'FBM'`);
ensureColumn('deals_history', 'couponCode', 'couponCode TEXT');

const settingsRow = db.prepare(`SELECT COUNT(*) AS count FROM settings`).get();
if (!settingsRow?.count) {
  db.prepare(
    `INSERT INTO settings (repostCooldownEnabled, repostCooldownHours, telegramCopyButtonText) VALUES (1, 12, ?)`
  ).run(DEFAULT_TELEGRAM_COPY_BUTTON_TEXT);
} else {
  db.prepare(`DELETE FROM settings WHERE rowid NOT IN (SELECT MIN(rowid) FROM settings)`).run();
  db.prepare(
    `
      UPDATE settings
      SET repostCooldownEnabled = COALESCE(repostCooldownEnabled, 1),
          repostCooldownHours = COALESCE(repostCooldownHours, 12),
          telegramCopyButtonText = COALESCE(NULLIF(TRIM(telegramCopyButtonText), ''), ?)
    `
  ).run(DEFAULT_TELEGRAM_COPY_BUTTON_TEXT);
}

const appSettingsRow = db.prepare(`SELECT COUNT(*) AS count FROM app_settings`).get();
if (!appSettingsRow?.count) {
  const legacySettings = db
    .prepare(`SELECT repostCooldownEnabled, repostCooldownHours, telegramCopyButtonText FROM settings LIMIT 1`)
    .get();

  db.prepare(
    `
      INSERT INTO app_settings (
        id,
        repostCooldownEnabled,
        repostCooldownHours,
        telegramCopyButtonText,
        copybotEnabled,
        facebookEnabled,
        facebookSessionMode,
        facebookDefaultRetryLimit,
        facebookDefaultTarget
      ) VALUES (1, ?, ?, ?, 0, 0, 'persistent', 3, NULL)
    `
  ).run(
    legacySettings?.repostCooldownEnabled ?? 1,
    legacySettings?.repostCooldownHours ?? 12,
    legacySettings?.telegramCopyButtonText ?? DEFAULT_TELEGRAM_COPY_BUTTON_TEXT
  );
} else {
  db.prepare(`DELETE FROM app_settings WHERE id != 1`).run();
  db.prepare(
    `
      UPDATE app_settings
      SET repostCooldownEnabled = COALESCE(repostCooldownEnabled, 1),
          repostCooldownHours = COALESCE(repostCooldownHours, 12),
          telegramCopyButtonText = COALESCE(NULLIF(TRIM(telegramCopyButtonText), ''), ?),
          copybotEnabled = COALESCE(copybotEnabled, 0),
          facebookEnabled = COALESCE(facebookEnabled, 0),
          facebookSessionMode = COALESCE(NULLIF(TRIM(facebookSessionMode), ''), 'persistent'),
          facebookDefaultRetryLimit = COALESCE(facebookDefaultRetryLimit, 3)
      WHERE id = 1
    `
  ).run(DEFAULT_TELEGRAM_COPY_BUTTON_TEXT);
}

db.exec(`
  UPDATE deals_history
  SET originalUrl = COALESCE(NULLIF(TRIM(originalUrl), ''), url),
      productTitle = COALESCE(NULLIF(TRIM(productTitle), ''), title),
      currentPrice = COALESCE(NULLIF(TRIM(currentPrice), ''), price),
      oldPrice = COALESCE(oldPrice, ''),
      sellerType = COALESCE(NULLIF(TRIM(sellerType), ''), 'FBM'),
      couponCode = COALESCE(couponCode, '')
`);

const now = new Date().toISOString();
db.prepare(
  `
    INSERT OR IGNORE INTO pricing_rules (
      id, name, is_active, keepa_required, idealo_required, autopost_above_score, manual_review_below_score,
      allow_amazon, min_discount_amazon, min_score_amazon, sampling_amazon, max_price_gap_idealo_amazon,
      allow_fba, min_discount_fba, min_score_fba, sampling_fba, max_price_gap_idealo_fba,
      allow_fbm, min_discount_fbm, min_score_fbm, sampling_fbm, max_price_gap_idealo_fbm,
      fbm_requires_manual_review, min_seller_rating_fbm, fake_drop_filter_enabled,
      coupon_only_penalty, variant_switch_penalty, marketplace_switch_penalty,
      manual_blacklist_keywords, manual_whitelist_brands, created_at, updated_at
    ) VALUES (
      1, 'Standard', 1, 0, 0, 85, 45,
      1, 15, 70, 100, NULL,
      1, 20, 75, 60, NULL,
      1, 40, 82, 20, NULL,
      1, NULL, 0,
      5, 8, 6,
      '[]', '[]', @now, @now
    )
  `
).run({ now });

db.prepare(
  `
    INSERT OR IGNORE INTO sampling_rules (
      id, name, is_active, default_sampling, sampling_amazon, sampling_fba, sampling_fbm,
      daily_limit, min_score, min_discount, notes, created_at, updated_at
    ) VALUES (
      1, 'Standard', 1, 100, 100, 100, 100,
      NULL, NULL, NULL, 'Default Sampling fuer neue Quellen', @now, @now
    )
  `
).run({ now });

export function getDb() {
  return db;
}

export { DEFAULT_TELEGRAM_COPY_BUTTON_TEXT };
