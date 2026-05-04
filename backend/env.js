import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const envPath = path.join(__dirname, '.env');

if (!fs.existsSync(envPath)) {
  fs.writeFileSync(envPath, '', 'utf8');
}

dotenv.config({ path: envPath, quiet: true });

function normalizeSecret(rawValue, placeholders = []) {
  const value = rawValue?.trim() || '';
  return placeholders.includes(value.toUpperCase()) ? '' : value;
}

function getReaderRuntimeFlags() {
  const readerTestMode = (process.env.READER_TEST_MODE?.trim() || '0') === '1';
  const readerDebugMode = (process.env.READER_DEBUG_MODE?.trim() || '0') === '1';
  const allowRawReaderFallbackFlag =
    process.env.ALLOW_RAW_READER_FALLBACK?.trim() || process.env.READER_ALLOW_RAW_FALLBACK?.trim() || '0';

  return {
    readerTestMode,
    readerDebugMode,
    allowRawReaderFallback: allowRawReaderFallbackFlag === '1'
  };
}

export function getTelegramConfig() {
  return {
    token: process.env.TELEGRAM_BOT_TOKEN?.trim() || '',
    chatId: process.env.TELEGRAM_CHAT_ID?.trim() || ''
  };
}

export function getStorageConfig() {
  const dataDir = process.env.APP_DATA_DIR?.trim() || path.join(__dirname, 'data');
  const dbPath = process.env.APP_DB_PATH?.trim() || path.join(dataDir, 'deals.db');
  const telegramUserSessionDir = process.env.TELEGRAM_USER_SESSION_DIR?.trim() || path.join(dataDir, 'telegram-user-sessions');

  return {
    envPath,
    dataDir,
    dbPath,
    telegramUserSessionDir
  };
}

export function getDatabaseConfig() {
  const storage = getStorageConfig();

  return {
    envPath: storage.envPath,
    dataDir: storage.dataDir,
    dbPath: storage.dbPath,
    journalMode: 'WAL'
  };
}

export function getTelegramUserReaderConfig() {
  const storage = getStorageConfig();
  const apiId = process.env.TELEGRAM_USER_API_ID?.trim() || '';
  const apiHash = normalizeSecret(process.env.TELEGRAM_USER_API_HASH, ['DEIN_API_HASH_HIER', 'YOUR_API_HASH_HERE', 'CHANGE_ME']);
  const explicitEnabled = process.env.TELEGRAM_USER_API_ENABLED?.trim();
  const runtimeFlags = getReaderRuntimeFlags();

  return {
    enabled: explicitEnabled ? explicitEnabled === '1' : Boolean(apiId && apiHash),
    apiId,
    apiHash,
    phoneNumber: process.env.TELEGRAM_USER_PHONE?.trim() || '',
    loginMode: (process.env.TELEGRAM_USER_LOGIN_MODE?.trim() || 'phone') === 'qr' ? 'qr' : 'phone',
    sessionDir: storage.telegramUserSessionDir,
    readerTestMode: runtimeFlags.readerTestMode,
    readerDebugMode: runtimeFlags.readerDebugMode,
    allowRawReaderFallback: runtimeFlags.allowRawReaderFallback,
    readerTestThresholds: {
      minDiscountPercent: runtimeFlags.readerTestMode ? 5 : 12,
      minScore: runtimeFlags.readerTestMode ? 0 : 58,
      clearFakeRejectRisk: runtimeFlags.readerTestMode ? 100 : 90
    }
  };
}

export function getReaderRuntimeConfig() {
  const runtimeFlags = getReaderRuntimeFlags();

  return {
    ...runtimeFlags,
    dealLockBypass: runtimeFlags.readerTestMode || runtimeFlags.readerDebugMode
  };
}

export function isDealLockBypassEnabled() {
  return getReaderRuntimeConfig().dealLockBypass;
}

export function getTelegramTestGroupConfig() {
  return {
    token: process.env.TELEGRAM_BOT_TOKEN?.trim() || '',
    chatId: process.env.TELEGRAM_TEST_CHAT_ID?.trim() || process.env.TELEGRAM_CHAT_ID?.trim() || ''
  };
}

export function getWhatsappDeliveryConfig() {
  return {
    enabled: (process.env.WHATSAPP_DELIVERY_ENABLED?.trim() || '0') === '1',
    endpoint: process.env.WHATSAPP_DELIVERY_ENDPOINT?.trim() || '',
    token: normalizeSecret(process.env.WHATSAPP_DELIVERY_TOKEN, ['DEIN_TOKEN_HIER', 'YOUR_TOKEN_HERE', 'CHANGE_ME']),
    sender: process.env.WHATSAPP_DELIVERY_SENDER?.trim() || '',
    retryLimit: Number.parseInt(process.env.WHATSAPP_DELIVERY_RETRY_LIMIT || '3', 10) || 3
  };
}

export function getKeepaConfig() {
  const key = normalizeSecret(process.env.KEEPA_API_KEY, ['DEIN_KEY_HIER', 'YOUR_KEY_HERE', 'CHANGE_ME']);

  return {
    key,
    defaultDomainId: Number.parseInt(process.env.KEEPA_DEFAULT_DOMAIN_ID || '3', 10) || 3,
    timeoutMs: Number.parseInt(process.env.KEEPA_TIMEOUT_MS || '12000', 10) || 12000,
    retryLimit: Number.parseInt(process.env.KEEPA_RETRY_LIMIT || '2', 10) || 2,
    requestIntervalMs: Number.parseInt(process.env.KEEPA_REQUEST_INTERVAL_MS || '1200', 10) || 1200
  };
}

export function getAmazonAffiliateConfig() {
  const accessKey = normalizeSecret(process.env.AMAZON_PAAPI_ACCESS_KEY, [
    'DEIN_AMAZON_ACCESS_KEY_HIER',
    'YOUR_AMAZON_ACCESS_KEY_HERE',
    'CHANGE_ME'
  ]);
  const secretKey = normalizeSecret(process.env.AMAZON_PAAPI_SECRET_KEY, [
    'DEIN_AMAZON_SECRET_KEY_HIER',
    'YOUR_AMAZON_SECRET_KEY_HERE',
    'CHANGE_ME'
  ]);
  const partnerTag = normalizeSecret(process.env.AMAZON_PAAPI_PARTNER_TAG, [
    'DEIN_PARTNER_TAG_HIER',
    'YOUR_PARTNER_TAG_HERE',
    'CHANGE_ME'
  ]);

  return {
    accessKey,
    secretKey,
    partnerTag: partnerTag || 'codeundcoup08-21',
    host: process.env.AMAZON_PAAPI_HOST?.trim() || 'webservices.amazon.de',
    region: process.env.AMAZON_PAAPI_REGION?.trim() || 'eu-west-1',
    marketplace: process.env.AMAZON_PAAPI_MARKETPLACE?.trim() || 'www.amazon.de',
    language: process.env.AMAZON_PAAPI_LANGUAGE?.trim() || 'de_DE',
    enabled: (process.env.AMAZON_PAAPI_ENABLED?.trim() || '1') !== '0',
    timeoutMs: Number.parseInt(process.env.AMAZON_PAAPI_TIMEOUT_MS || '12000', 10) || 12000
  };
}

function resolveAmazonCreatorAuthEndpoint(version = '') {
  const normalized = version.trim();

  if (normalized === '2.1') {
    return 'https://creatorsapi.auth.us-east-1.amazoncognito.com/oauth2/token';
  }
  if (normalized === '2.2') {
    return 'https://creatorsapi.auth.eu-south-2.amazoncognito.com/oauth2/token';
  }
  if (normalized === '2.3') {
    return 'https://creatorsapi.auth.us-west-2.amazoncognito.com/oauth2/token';
  }
  if (normalized === '3.1') {
    return 'https://api.amazon.com/auth/o2/token';
  }
  if (normalized === '3.3') {
    return 'https://api.amazon.co.jp/auth/o2/token';
  }

  return 'https://api.amazon.co.uk/auth/o2/token';
}

function mapAmazonCreatorMarketplace(value = '') {
  const rawMarketplace = value.trim();
  const normalizedCode = rawMarketplace.toUpperCase();
  const marketplaceMap = {
    DE: 'www.amazon.de',
    UK: 'www.amazon.co.uk',
    US: 'www.amazon.com',
    FR: 'www.amazon.fr',
    IT: 'www.amazon.it',
    ES: 'www.amazon.es'
  };
  const mappedMarketplace = marketplaceMap[normalizedCode] || rawMarketplace;

  if (rawMarketplace && mappedMarketplace !== rawMarketplace) {
    console.info('[CREATOR_API_MARKETPLACE_MAPPED]', {
      from: rawMarketplace,
      to: mappedMarketplace
    });
  }
  if (mappedMarketplace) {
    console.info('[CREATOR_API_MARKETPLACE_USED]', {
      marketplace: mappedMarketplace
    });
  }

  return mappedMarketplace;
}

export function getAmazonCreatorApiConfig() {
  const paapiConfig = getAmazonAffiliateConfig();
  const credentialVersion = process.env.AMAZON_CREATOR_API_CREDENTIAL_VERSION?.trim() || '3.2';
  const clientId = normalizeSecret(
    process.env.AMAZON_CREATOR_API_KEY ||
      process.env.AMAZON_CREATOR_API_CLIENT_ID ||
      process.env.AMAZON_CREATOR_API_CREDENTIAL_ID,
    [
      'DEIN_CREATOR_API_KEY_HIER',
      'DEIN_CREATOR_CLIENT_ID_HIER',
      'DEIN_CREATOR_CREDENTIAL_ID_HIER',
      'YOUR_CREATOR_API_KEY_HERE',
      'YOUR_CREATOR_CLIENT_ID_HERE',
      'YOUR_CREATOR_CREDENTIAL_ID_HERE',
      'CHANGE_ME'
    ]
  );
  const clientSecret = normalizeSecret(
    process.env.AMAZON_CREATOR_API_SECRET ||
      process.env.AMAZON_CREATOR_API_CLIENT_SECRET ||
      process.env.AMAZON_CREATOR_API_CREDENTIAL_SECRET,
    [
      'DEIN_CREATOR_API_SECRET_HIER',
      'DEIN_CREATOR_CLIENT_SECRET_HIER',
      'DEIN_CREATOR_CREDENTIAL_SECRET_HIER',
      'YOUR_CREATOR_API_SECRET_HERE',
      'YOUR_CREATOR_CLIENT_SECRET_HERE',
      'YOUR_CREATOR_CREDENTIAL_SECRET_HERE',
      'CHANGE_ME'
    ]
  );

  return {
    enabled: (process.env.AMAZON_CREATOR_API_ENABLED?.trim() || '0') === '1',
    clientId,
    clientSecret,
    partnerTag: normalizeSecret(process.env.AMAZON_CREATOR_API_PARTNER_TAG, [
      'DEIN_PARTNER_TAG_HIER',
      'YOUR_PARTNER_TAG_HERE',
      'CHANGE_ME'
    ]) || paapiConfig.partnerTag,
    credentialVersion,
    authEndpoint:
      process.env.AMAZON_CREATOR_API_TOKEN_ENDPOINT?.trim() ||
      process.env.AMAZON_CREATOR_API_AUTH_ENDPOINT?.trim() ||
      resolveAmazonCreatorAuthEndpoint(credentialVersion),
    endpoint: process.env.AMAZON_CREATOR_API_ENDPOINT?.trim() || 'https://creatorsapi.amazon/catalog/v1/getItems',
    marketplace: mapAmazonCreatorMarketplace(
      process.env.AMAZON_CREATOR_API_MARKETPLACE?.trim() || paapiConfig.marketplace || 'www.amazon.de'
    ),
    timeoutMs: Number.parseInt(process.env.AMAZON_CREATOR_API_TIMEOUT_MS || String(paapiConfig.timeoutMs || 12000), 10) || 12000
  };
}

export function getApiPort() {
  return process.env.PORT || 4000;
}
