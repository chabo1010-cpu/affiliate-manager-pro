import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import Database from 'better-sqlite3';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dataDir = path.join(__dirname, 'data');
const dbPath = path.join(dataDir, 'deals.db');
const DEFAULT_TELEGRAM_COPY_BUTTON_TEXT = '📋 Zum Kopieren hier klicken';

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

  CREATE TABLE IF NOT EXISTS keepa_settings (
    id INTEGER PRIMARY KEY,
    keepa_enabled INTEGER NOT NULL DEFAULT 1,
    scheduler_enabled INTEGER NOT NULL DEFAULT 1,
    domain_id INTEGER NOT NULL DEFAULT 3,
    default_categories_json TEXT NOT NULL DEFAULT '[]',
    default_discount REAL NOT NULL DEFAULT 40,
    default_seller_type TEXT NOT NULL DEFAULT 'ALL',
    default_min_price REAL,
    default_max_price REAL,
    default_page_size INTEGER NOT NULL DEFAULT 24,
    default_interval_minutes INTEGER NOT NULL DEFAULT 60,
    strong_deal_min_discount REAL NOT NULL DEFAULT 40,
    strong_deal_min_comparison_gap_pct REAL NOT NULL DEFAULT 10,
    good_rating_threshold REAL NOT NULL DEFAULT 4,
    alert_telegram_enabled INTEGER NOT NULL DEFAULT 0,
    alert_internal_enabled INTEGER NOT NULL DEFAULT 1,
    alert_whatsapp_placeholder_enabled INTEGER NOT NULL DEFAULT 0,
    alert_cooldown_minutes INTEGER NOT NULL DEFAULT 180,
    alert_max_per_product INTEGER NOT NULL DEFAULT 2,
    telegram_message_prefix TEXT,
    comparison_source_config_json TEXT NOT NULL DEFAULT '{}',
    logging_enabled INTEGER NOT NULL DEFAULT 1,
    estimated_tokens_per_manual_run INTEGER NOT NULL DEFAULT 8,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS keepa_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    min_discount REAL NOT NULL DEFAULT 30,
    seller_type TEXT NOT NULL DEFAULT 'ALL',
    categories_json TEXT NOT NULL DEFAULT '[]',
    min_price REAL,
    max_price REAL,
    min_deal_score REAL NOT NULL DEFAULT 70,
    interval_minutes INTEGER NOT NULL DEFAULT 60,
    only_prime INTEGER NOT NULL DEFAULT 0,
    only_in_stock INTEGER NOT NULL DEFAULT 1,
    only_good_rating INTEGER NOT NULL DEFAULT 0,
    comparison_sources_json TEXT NOT NULL DEFAULT '[]',
    is_active INTEGER NOT NULL DEFAULT 1,
    last_run_at TEXT,
    next_run_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS keepa_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asin TEXT NOT NULL,
    domain_id INTEGER NOT NULL DEFAULT 3,
    title TEXT NOT NULL,
    product_url TEXT,
    image_url TEXT,
    current_price REAL,
    reference_price REAL,
    reference_label TEXT,
    keepa_discount REAL,
    seller_type TEXT NOT NULL DEFAULT 'UNKNOWN',
    category_id INTEGER,
    category_name TEXT,
    rating REAL,
    review_count INTEGER,
    is_prime INTEGER NOT NULL DEFAULT 0,
    is_in_stock INTEGER NOT NULL DEFAULT 0,
    deal_score REAL NOT NULL DEFAULT 0,
    deal_strength TEXT NOT NULL DEFAULT 'pruefenswert',
    strength_reason TEXT,
    workflow_status TEXT NOT NULL DEFAULT 'neu',
    comparison_source TEXT,
    comparison_status TEXT NOT NULL DEFAULT 'not_connected',
    comparison_price REAL,
    price_difference_abs REAL,
    price_difference_pct REAL,
    comparison_checked_at TEXT,
    comparison_payload_json TEXT,
    keepa_payload_json TEXT,
    search_payload_json TEXT,
    origin TEXT NOT NULL DEFAULT 'manual',
    rule_id INTEGER,
    note TEXT,
    alert_count INTEGER NOT NULL DEFAULT 0,
    last_alerted_at TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_synced_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (rule_id) REFERENCES keepa_rules(id)
  );

  CREATE UNIQUE INDEX IF NOT EXISTS idx_keepa_results_asin_domain ON keepa_results (asin, domain_id);
  CREATE INDEX IF NOT EXISTS idx_keepa_results_workflow_status ON keepa_results (workflow_status);
  CREATE INDEX IF NOT EXISTS idx_keepa_results_category_id ON keepa_results (category_id);
  CREATE INDEX IF NOT EXISTS idx_keepa_results_discount ON keepa_results (keepa_discount DESC);
  CREATE INDEX IF NOT EXISTS idx_keepa_results_score ON keepa_results (deal_score DESC);
  CREATE INDEX IF NOT EXISTS idx_keepa_results_updated_at ON keepa_results (updated_at DESC);

  CREATE TABLE IF NOT EXISTS keepa_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keepa_result_id INTEGER,
    asin TEXT NOT NULL,
    channel_type TEXT NOT NULL,
    status TEXT NOT NULL,
    rule_id INTEGER,
    dedupe_key TEXT NOT NULL UNIQUE,
    message_preview TEXT,
    payload_json TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    sent_at TEXT,
    FOREIGN KEY (keepa_result_id) REFERENCES keepa_results(id),
    FOREIGN KEY (rule_id) REFERENCES keepa_rules(id)
  );

  CREATE INDEX IF NOT EXISTS idx_keepa_alerts_channel_created_at ON keepa_alerts (channel_type, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_keepa_alerts_asin_created_at ON keepa_alerts (asin, created_at DESC);

  CREATE TABLE IF NOT EXISTS keepa_usage_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    module TEXT NOT NULL,
    filters_json TEXT,
    result_count INTEGER,
    duration_ms INTEGER,
    request_status TEXT NOT NULL DEFAULT 'success',
    estimated_usage REAL NOT NULL DEFAULT 0,
    official_usage_value REAL,
    official_tokens_left INTEGER,
    rule_id INTEGER,
    error_message TEXT,
    meta_json TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (rule_id) REFERENCES keepa_rules(id)
  );

  CREATE INDEX IF NOT EXISTS idx_keepa_usage_logs_created_at ON keepa_usage_logs (created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_keepa_usage_logs_module_created_at ON keepa_usage_logs (module, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_keepa_usage_logs_action_created_at ON keepa_usage_logs (action, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_keepa_usage_logs_status_created_at ON keepa_usage_logs (request_status, created_at DESC);

  CREATE TABLE IF NOT EXISTS keepa_usage_daily (
    usage_date TEXT NOT NULL,
    module TEXT NOT NULL,
    action TEXT NOT NULL,
    request_count INTEGER NOT NULL DEFAULT 0,
    result_count INTEGER NOT NULL DEFAULT 0,
    estimated_usage REAL NOT NULL DEFAULT 0,
    official_usage_value REAL,
    success_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    total_duration_ms INTEGER NOT NULL DEFAULT 0,
    last_request_at TEXT,
    PRIMARY KEY (usage_date, module, action)
  );

  CREATE TABLE IF NOT EXISTS keepa_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    level TEXT NOT NULL DEFAULT 'info',
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    message TEXT NOT NULL,
    filters_json TEXT,
    result_count INTEGER,
    tokens_left INTEGER,
    tokens_consumed INTEGER,
    payload_json TEXT,
    created_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS keepa_fake_drop_settings (
    id INTEGER PRIMARY KEY,
    engine_enabled INTEGER NOT NULL DEFAULT 1,
    low_risk_threshold REAL NOT NULL DEFAULT 32,
    high_risk_threshold REAL NOT NULL DEFAULT 72,
    review_priority_threshold REAL NOT NULL DEFAULT 58,
    amazon_confidence_strong REAL NOT NULL DEFAULT 72,
    stability_strong REAL NOT NULL DEFAULT 66,
    reference_inflation_threshold REAL NOT NULL DEFAULT 22,
    volatility_warning_threshold REAL NOT NULL DEFAULT 18,
    short_peak_max_days REAL NOT NULL DEFAULT 3,
    spike_sensitivity REAL NOT NULL DEFAULT 16,
    rebound_window_days REAL NOT NULL DEFAULT 7,
    weights_json TEXT NOT NULL DEFAULT '{}',
    engine_version TEXT NOT NULL DEFAULT 'keepa-fake-drop-v1',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS keepa_feature_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keepa_result_id INTEGER NOT NULL UNIQUE,
    asin TEXT NOT NULL,
    seller_type TEXT NOT NULL,
    feature_json TEXT NOT NULL,
    price_series_json TEXT,
    offer_series_json TEXT,
    chart_points_json TEXT,
    engine_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (keepa_result_id) REFERENCES keepa_results(id) ON DELETE CASCADE
  );

  CREATE INDEX IF NOT EXISTS idx_keepa_feature_snapshots_asin ON keepa_feature_snapshots (asin);
  CREATE INDEX IF NOT EXISTS idx_keepa_feature_snapshots_updated_at ON keepa_feature_snapshots (updated_at DESC);

  CREATE TABLE IF NOT EXISTS keepa_fake_drop_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keepa_result_id INTEGER NOT NULL UNIQUE,
    asin TEXT NOT NULL,
    seller_type TEXT NOT NULL,
    classification TEXT NOT NULL,
    stability_score REAL NOT NULL DEFAULT 0,
    manipulation_score REAL NOT NULL DEFAULT 0,
    trust_score REAL NOT NULL DEFAULT 0,
    amazon_confidence REAL NOT NULL DEFAULT 0,
    fake_drop_risk REAL NOT NULL DEFAULT 0,
    review_priority REAL NOT NULL DEFAULT 0,
    reasoning_json TEXT NOT NULL DEFAULT '{}',
    engine_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (keepa_result_id) REFERENCES keepa_results(id) ON DELETE CASCADE
  );

  CREATE INDEX IF NOT EXISTS idx_keepa_fake_drop_scores_classification ON keepa_fake_drop_scores (classification);
  CREATE INDEX IF NOT EXISTS idx_keepa_fake_drop_scores_risk ON keepa_fake_drop_scores (fake_drop_risk DESC);
  CREATE INDEX IF NOT EXISTS idx_keepa_fake_drop_scores_priority ON keepa_fake_drop_scores (review_priority DESC);

  CREATE TABLE IF NOT EXISTS keepa_review_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keepa_result_id INTEGER NOT NULL UNIQUE,
    fake_drop_score_id INTEGER,
    feature_snapshot_id INTEGER,
    asin TEXT NOT NULL,
    seller_type TEXT NOT NULL,
    category_name TEXT,
    classification TEXT NOT NULL,
    review_status TEXT NOT NULL DEFAULT 'open',
    review_priority REAL NOT NULL DEFAULT 0,
    analysis_reason TEXT,
    current_label TEXT,
    tags_json TEXT NOT NULL DEFAULT '[]',
    note TEXT,
    chart_snapshot_json TEXT,
    example_bucket TEXT,
    label_count INTEGER NOT NULL DEFAULT 0,
    last_reviewed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (keepa_result_id) REFERENCES keepa_results(id) ON DELETE CASCADE,
    FOREIGN KEY (fake_drop_score_id) REFERENCES keepa_fake_drop_scores(id) ON DELETE SET NULL,
    FOREIGN KEY (feature_snapshot_id) REFERENCES keepa_feature_snapshots(id) ON DELETE SET NULL
  );

  CREATE INDEX IF NOT EXISTS idx_keepa_review_items_status_priority ON keepa_review_items (review_status, review_priority DESC);
  CREATE INDEX IF NOT EXISTS idx_keepa_review_items_classification ON keepa_review_items (classification);
  CREATE INDEX IF NOT EXISTS idx_keepa_review_items_label ON keepa_review_items (current_label);

  CREATE TABLE IF NOT EXISTS keepa_review_labels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_item_id INTEGER NOT NULL,
    keepa_result_id INTEGER NOT NULL,
    asin TEXT NOT NULL,
    seller_type TEXT NOT NULL,
    label TEXT NOT NULL,
    tags_json TEXT NOT NULL DEFAULT '[]',
    note TEXT,
    engine_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (review_item_id) REFERENCES keepa_review_items(id) ON DELETE CASCADE,
    FOREIGN KEY (keepa_result_id) REFERENCES keepa_results(id) ON DELETE CASCADE
  );

  CREATE INDEX IF NOT EXISTS idx_keepa_review_labels_review_item ON keepa_review_labels (review_item_id, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_keepa_review_labels_label ON keepa_review_labels (label, created_at DESC);

  CREATE TABLE IF NOT EXISTS keepa_example_library (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_item_id INTEGER NOT NULL,
    keepa_result_id INTEGER NOT NULL,
    asin TEXT NOT NULL,
    seller_type TEXT NOT NULL,
    category_name TEXT,
    bucket TEXT NOT NULL,
    label TEXT NOT NULL,
    tags_json TEXT NOT NULL DEFAULT '[]',
    note TEXT,
    snapshot_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (review_item_id) REFERENCES keepa_review_items(id) ON DELETE CASCADE,
    FOREIGN KEY (keepa_result_id) REFERENCES keepa_results(id) ON DELETE CASCADE
  );

  CREATE INDEX IF NOT EXISTS idx_keepa_example_library_bucket ON keepa_example_library (bucket, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_keepa_example_library_label ON keepa_example_library (label, created_at DESC);
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

ensureColumn('keepa_settings', 'scheduler_enabled', `scheduler_enabled INTEGER NOT NULL DEFAULT 1`);
ensureColumn('keepa_settings', 'domain_id', `domain_id INTEGER NOT NULL DEFAULT 3`);
ensureColumn('keepa_settings', 'default_categories_json', `default_categories_json TEXT NOT NULL DEFAULT '[]'`);
ensureColumn('keepa_settings', 'default_discount', `default_discount REAL NOT NULL DEFAULT 40`);
ensureColumn('keepa_settings', 'default_seller_type', `default_seller_type TEXT NOT NULL DEFAULT 'ALL'`);
ensureColumn('keepa_settings', 'default_min_price', `default_min_price REAL`);
ensureColumn('keepa_settings', 'default_max_price', `default_max_price REAL`);
ensureColumn('keepa_settings', 'default_page_size', `default_page_size INTEGER NOT NULL DEFAULT 24`);
ensureColumn('keepa_settings', 'default_interval_minutes', `default_interval_minutes INTEGER NOT NULL DEFAULT 60`);
ensureColumn('keepa_settings', 'strong_deal_min_discount', `strong_deal_min_discount REAL NOT NULL DEFAULT 40`);
ensureColumn(
  'keepa_settings',
  'strong_deal_min_comparison_gap_pct',
  `strong_deal_min_comparison_gap_pct REAL NOT NULL DEFAULT 10`
);
ensureColumn('keepa_settings', 'good_rating_threshold', `good_rating_threshold REAL NOT NULL DEFAULT 4`);
ensureColumn('keepa_settings', 'alert_telegram_enabled', `alert_telegram_enabled INTEGER NOT NULL DEFAULT 0`);
ensureColumn('keepa_settings', 'alert_internal_enabled', `alert_internal_enabled INTEGER NOT NULL DEFAULT 1`);
ensureColumn(
  'keepa_settings',
  'alert_whatsapp_placeholder_enabled',
  `alert_whatsapp_placeholder_enabled INTEGER NOT NULL DEFAULT 0`
);
ensureColumn('keepa_settings', 'alert_cooldown_minutes', `alert_cooldown_minutes INTEGER NOT NULL DEFAULT 180`);
ensureColumn('keepa_settings', 'alert_max_per_product', `alert_max_per_product INTEGER NOT NULL DEFAULT 2`);
ensureColumn('keepa_settings', 'telegram_message_prefix', `telegram_message_prefix TEXT`);
ensureColumn(
  'keepa_settings',
  'comparison_source_config_json',
  `comparison_source_config_json TEXT NOT NULL DEFAULT '{}'`
);
ensureColumn('keepa_settings', 'logging_enabled', `logging_enabled INTEGER NOT NULL DEFAULT 1`);
ensureColumn(
  'keepa_settings',
  'estimated_tokens_per_manual_run',
  `estimated_tokens_per_manual_run INTEGER NOT NULL DEFAULT 8`
);

ensureColumn('keepa_fake_drop_settings', 'engine_enabled', `engine_enabled INTEGER NOT NULL DEFAULT 1`);
ensureColumn('keepa_fake_drop_settings', 'low_risk_threshold', `low_risk_threshold REAL NOT NULL DEFAULT 32`);
ensureColumn('keepa_fake_drop_settings', 'high_risk_threshold', `high_risk_threshold REAL NOT NULL DEFAULT 72`);
ensureColumn(
  'keepa_fake_drop_settings',
  'review_priority_threshold',
  `review_priority_threshold REAL NOT NULL DEFAULT 58`
);
ensureColumn(
  'keepa_fake_drop_settings',
  'amazon_confidence_strong',
  `amazon_confidence_strong REAL NOT NULL DEFAULT 72`
);
ensureColumn('keepa_fake_drop_settings', 'stability_strong', `stability_strong REAL NOT NULL DEFAULT 66`);
ensureColumn(
  'keepa_fake_drop_settings',
  'reference_inflation_threshold',
  `reference_inflation_threshold REAL NOT NULL DEFAULT 22`
);
ensureColumn(
  'keepa_fake_drop_settings',
  'volatility_warning_threshold',
  `volatility_warning_threshold REAL NOT NULL DEFAULT 18`
);
ensureColumn('keepa_fake_drop_settings', 'short_peak_max_days', `short_peak_max_days REAL NOT NULL DEFAULT 3`);
ensureColumn('keepa_fake_drop_settings', 'spike_sensitivity', `spike_sensitivity REAL NOT NULL DEFAULT 16`);
ensureColumn('keepa_fake_drop_settings', 'rebound_window_days', `rebound_window_days REAL NOT NULL DEFAULT 7`);
ensureColumn('keepa_fake_drop_settings', 'weights_json', `weights_json TEXT NOT NULL DEFAULT '{}'`);
ensureColumn(
  'keepa_fake_drop_settings',
  'engine_version',
  `engine_version TEXT NOT NULL DEFAULT 'keepa-fake-drop-v1'`
);

ensureColumn('keepa_feature_snapshots', 'offer_series_json', `offer_series_json TEXT`);
ensureColumn('keepa_feature_snapshots', 'chart_points_json', `chart_points_json TEXT`);
ensureColumn('keepa_feature_snapshots', 'engine_version', `engine_version TEXT NOT NULL DEFAULT 'keepa-fake-drop-v1'`);

ensureColumn('keepa_fake_drop_scores', 'asin', `asin TEXT`);
ensureColumn('keepa_fake_drop_scores', 'seller_type', `seller_type TEXT NOT NULL DEFAULT 'UNKNOWN'`);
ensureColumn('keepa_fake_drop_scores', 'classification', `classification TEXT NOT NULL DEFAULT 'manuelle_pruefung'`);
ensureColumn('keepa_fake_drop_scores', 'stability_score', `stability_score REAL NOT NULL DEFAULT 0`);
ensureColumn('keepa_fake_drop_scores', 'manipulation_score', `manipulation_score REAL NOT NULL DEFAULT 0`);
ensureColumn('keepa_fake_drop_scores', 'trust_score', `trust_score REAL NOT NULL DEFAULT 0`);
ensureColumn('keepa_fake_drop_scores', 'amazon_confidence', `amazon_confidence REAL NOT NULL DEFAULT 0`);
ensureColumn('keepa_fake_drop_scores', 'fake_drop_risk', `fake_drop_risk REAL NOT NULL DEFAULT 0`);
ensureColumn('keepa_fake_drop_scores', 'review_priority', `review_priority REAL NOT NULL DEFAULT 0`);
ensureColumn('keepa_fake_drop_scores', 'reasoning_json', `reasoning_json TEXT NOT NULL DEFAULT '{}'`);
ensureColumn(
  'keepa_fake_drop_scores',
  'engine_version',
  `engine_version TEXT NOT NULL DEFAULT 'keepa-fake-drop-v1'`
);

ensureColumn('keepa_review_items', 'analysis_reason', `analysis_reason TEXT`);
ensureColumn('keepa_review_items', 'current_label', `current_label TEXT`);
ensureColumn('keepa_review_items', 'tags_json', `tags_json TEXT NOT NULL DEFAULT '[]'`);
ensureColumn('keepa_review_items', 'note', `note TEXT`);
ensureColumn('keepa_review_items', 'chart_snapshot_json', `chart_snapshot_json TEXT`);
ensureColumn('keepa_review_items', 'example_bucket', `example_bucket TEXT`);
ensureColumn('keepa_review_items', 'label_count', `label_count INTEGER NOT NULL DEFAULT 0`);
ensureColumn('keepa_review_items', 'last_reviewed_at', `last_reviewed_at TEXT`);

ensureColumn('keepa_review_labels', 'asin', `asin TEXT`);
ensureColumn('keepa_review_labels', 'seller_type', `seller_type TEXT NOT NULL DEFAULT 'UNKNOWN'`);
ensureColumn('keepa_review_labels', 'tags_json', `tags_json TEXT NOT NULL DEFAULT '[]'`);
ensureColumn('keepa_review_labels', 'note', `note TEXT`);
ensureColumn(
  'keepa_review_labels',
  'engine_version',
  `engine_version TEXT NOT NULL DEFAULT 'keepa-fake-drop-v1'`
);

ensureColumn('keepa_example_library', 'tags_json', `tags_json TEXT NOT NULL DEFAULT '[]'`);
ensureColumn('keepa_example_library', 'note', `note TEXT`);
ensureColumn('keepa_example_library', 'category_name', `category_name TEXT`);

ensureColumn('keepa_rules', 'comparison_sources_json', `comparison_sources_json TEXT NOT NULL DEFAULT '[]'`);

ensureColumn('keepa_results', 'deal_strength', `deal_strength TEXT NOT NULL DEFAULT 'pruefenswert'`);
ensureColumn('keepa_results', 'strength_reason', `strength_reason TEXT`);
ensureColumn('keepa_results', 'workflow_status', `workflow_status TEXT NOT NULL DEFAULT 'neu'`);
ensureColumn('keepa_results', 'comparison_source', `comparison_source TEXT`);
ensureColumn('keepa_results', 'comparison_status', `comparison_status TEXT NOT NULL DEFAULT 'not_connected'`);
ensureColumn('keepa_results', 'comparison_price', `comparison_price REAL`);
ensureColumn('keepa_results', 'price_difference_abs', `price_difference_abs REAL`);
ensureColumn('keepa_results', 'price_difference_pct', `price_difference_pct REAL`);
ensureColumn('keepa_results', 'comparison_checked_at', `comparison_checked_at TEXT`);
ensureColumn('keepa_results', 'comparison_payload_json', `comparison_payload_json TEXT`);
ensureColumn('keepa_results', 'search_payload_json', `search_payload_json TEXT`);
ensureColumn('keepa_results', 'origin', `origin TEXT NOT NULL DEFAULT 'manual'`);
ensureColumn('keepa_results', 'note', `note TEXT`);
ensureColumn('keepa_results', 'alert_count', `alert_count INTEGER NOT NULL DEFAULT 0`);
ensureColumn('keepa_results', 'last_alerted_at', `last_alerted_at TEXT`);
ensureColumn('keepa_results', 'first_seen_at', `first_seen_at TEXT`);
ensureColumn('keepa_results', 'last_seen_at', `last_seen_at TEXT`);
ensureColumn('keepa_results', 'last_synced_at', `last_synced_at TEXT`);

ensureColumn('keepa_alerts', 'dedupe_key', `dedupe_key TEXT`);
db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_keepa_alerts_dedupe_key ON keepa_alerts (dedupe_key)`);

ensureColumn('keepa_usage_logs', 'action', `action TEXT NOT NULL DEFAULT 'manual-search'`);
ensureColumn('keepa_usage_logs', 'module', `module TEXT NOT NULL DEFAULT 'manual-search'`);
ensureColumn('keepa_usage_logs', 'filters_json', `filters_json TEXT`);
ensureColumn('keepa_usage_logs', 'result_count', `result_count INTEGER`);
ensureColumn('keepa_usage_logs', 'duration_ms', `duration_ms INTEGER`);
ensureColumn('keepa_usage_logs', 'request_status', `request_status TEXT NOT NULL DEFAULT 'success'`);
ensureColumn('keepa_usage_logs', 'estimated_usage', `estimated_usage REAL NOT NULL DEFAULT 0`);
ensureColumn('keepa_usage_logs', 'official_usage_value', `official_usage_value REAL`);
ensureColumn('keepa_usage_logs', 'official_tokens_left', `official_tokens_left INTEGER`);
ensureColumn('keepa_usage_logs', 'rule_id', `rule_id INTEGER`);
ensureColumn('keepa_usage_logs', 'error_message', `error_message TEXT`);
ensureColumn('keepa_usage_logs', 'meta_json', `meta_json TEXT`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_keepa_usage_logs_created_at ON keepa_usage_logs (created_at DESC)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_keepa_usage_logs_module_created_at ON keepa_usage_logs (module, created_at DESC)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_keepa_usage_logs_action_created_at ON keepa_usage_logs (action, created_at DESC)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_keepa_usage_logs_status_created_at ON keepa_usage_logs (request_status, created_at DESC)`);

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

db.prepare(
  `
    INSERT OR IGNORE INTO keepa_settings (
      id,
      keepa_enabled,
      scheduler_enabled,
      domain_id,
      default_categories_json,
      default_discount,
      default_seller_type,
      default_min_price,
      default_max_price,
      default_page_size,
      default_interval_minutes,
      strong_deal_min_discount,
      strong_deal_min_comparison_gap_pct,
      good_rating_threshold,
      alert_telegram_enabled,
      alert_internal_enabled,
      alert_whatsapp_placeholder_enabled,
      alert_cooldown_minutes,
      alert_max_per_product,
      telegram_message_prefix,
      comparison_source_config_json,
      logging_enabled,
      estimated_tokens_per_manual_run,
      created_at,
      updated_at
    ) VALUES (
      1,
      1,
      1,
      3,
      '[]',
      40,
      'ALL',
      NULL,
      NULL,
      24,
      60,
      40,
      10,
      4,
      0,
      1,
      0,
      180,
      2,
      'Keepa Alert',
      '{"manual-source":{"enabled":1},"idealo":{"enabled":0},"custom-api":{"enabled":0}}',
      1,
      8,
      @now,
      @now
    )
  `
).run({ now });

db.prepare(
  `
    UPDATE keepa_settings
    SET keepa_enabled = COALESCE(keepa_enabled, 1),
        scheduler_enabled = COALESCE(scheduler_enabled, 1),
        domain_id = COALESCE(domain_id, 3),
        default_categories_json = COALESCE(default_categories_json, '[]'),
        default_discount = COALESCE(default_discount, 40),
        default_seller_type = COALESCE(NULLIF(TRIM(default_seller_type), ''), 'ALL'),
        default_page_size = COALESCE(default_page_size, 24),
        default_interval_minutes = COALESCE(default_interval_minutes, 60),
        strong_deal_min_discount = COALESCE(strong_deal_min_discount, 40),
        strong_deal_min_comparison_gap_pct = COALESCE(strong_deal_min_comparison_gap_pct, 10),
        good_rating_threshold = COALESCE(good_rating_threshold, 4),
        alert_telegram_enabled = COALESCE(alert_telegram_enabled, 0),
        alert_internal_enabled = COALESCE(alert_internal_enabled, 1),
        alert_whatsapp_placeholder_enabled = COALESCE(alert_whatsapp_placeholder_enabled, 0),
        alert_cooldown_minutes = COALESCE(alert_cooldown_minutes, 180),
        alert_max_per_product = COALESCE(alert_max_per_product, 2),
        telegram_message_prefix = COALESCE(telegram_message_prefix, 'Keepa Alert'),
        comparison_source_config_json = COALESCE(comparison_source_config_json, '{}'),
        logging_enabled = COALESCE(logging_enabled, 1),
        estimated_tokens_per_manual_run = COALESCE(estimated_tokens_per_manual_run, 8),
        updated_at = @now
    WHERE id = 1
  `
).run({ now });

db.prepare(
  `
    INSERT OR IGNORE INTO keepa_fake_drop_settings (
      id,
      engine_enabled,
      low_risk_threshold,
      high_risk_threshold,
      review_priority_threshold,
      amazon_confidence_strong,
      stability_strong,
      reference_inflation_threshold,
      volatility_warning_threshold,
      short_peak_max_days,
      spike_sensitivity,
      rebound_window_days,
      weights_json,
      engine_version,
      created_at,
      updated_at
    ) VALUES (
      1,
      1,
      32,
      72,
      58,
      72,
      66,
      22,
      18,
      3,
      16,
      7,
      '{"stability":1,"manipulation":1,"amazon":1,"feedback":1}',
      'keepa-fake-drop-v1',
      @now,
      @now
    )
  `
).run({ now });

db.prepare(
  `
    UPDATE keepa_fake_drop_settings
    SET engine_enabled = COALESCE(engine_enabled, 1),
        low_risk_threshold = COALESCE(low_risk_threshold, 32),
        high_risk_threshold = COALESCE(high_risk_threshold, 72),
        review_priority_threshold = COALESCE(review_priority_threshold, 58),
        amazon_confidence_strong = COALESCE(amazon_confidence_strong, 72),
        stability_strong = COALESCE(stability_strong, 66),
        reference_inflation_threshold = COALESCE(reference_inflation_threshold, 22),
        volatility_warning_threshold = COALESCE(volatility_warning_threshold, 18),
        short_peak_max_days = COALESCE(short_peak_max_days, 3),
        spike_sensitivity = COALESCE(spike_sensitivity, 16),
        rebound_window_days = COALESCE(rebound_window_days, 7),
        weights_json = COALESCE(weights_json, '{}'),
        engine_version = COALESCE(NULLIF(TRIM(engine_version), ''), 'keepa-fake-drop-v1'),
        updated_at = @now
    WHERE id = 1
  `
).run({ now });

db.exec(`
  DELETE FROM keepa_usage_daily;

  INSERT INTO keepa_usage_daily (
    usage_date,
    module,
    action,
    request_count,
    result_count,
    estimated_usage,
    official_usage_value,
    success_count,
    error_count,
    total_duration_ms,
    last_request_at
  )
  SELECT
    STRFTIME('%Y-%m-%d', created_at, 'localtime') AS usage_date,
    COALESCE(NULLIF(TRIM(module), ''), 'unknown') AS module,
    COALESCE(NULLIF(TRIM(action), ''), 'unknown') AS action,
    COUNT(*) AS request_count,
    COALESCE(SUM(COALESCE(result_count, 0)), 0) AS result_count,
    COALESCE(SUM(COALESCE(estimated_usage, 0)), 0) AS estimated_usage,
    COALESCE(SUM(official_usage_value), 0) AS official_usage_value,
    COALESCE(SUM(CASE WHEN request_status = 'success' THEN 1 ELSE 0 END), 0) AS success_count,
    COALESCE(SUM(CASE WHEN request_status = 'error' THEN 1 ELSE 0 END), 0) AS error_count,
    COALESCE(SUM(COALESCE(duration_ms, 0)), 0) AS total_duration_ms,
    MAX(created_at) AS last_request_at
  FROM keepa_usage_logs
  GROUP BY STRFTIME('%Y-%m-%d', created_at, 'localtime'), COALESCE(NULLIF(TRIM(module), ''), 'unknown'), COALESCE(NULLIF(TRIM(action), ''), 'unknown');
`);

export function getDb() {
  return db;
}

export { DEFAULT_TELEGRAM_COPY_BUTTON_TEXT };
