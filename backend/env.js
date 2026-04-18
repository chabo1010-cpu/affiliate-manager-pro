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

export function getTelegramConfig() {
  return {
    token: process.env.TELEGRAM_BOT_TOKEN?.trim() || '',
    chatId: process.env.TELEGRAM_CHAT_ID?.trim() || ''
  };
}

export function getTelegramTestGroupConfig() {
  return {
    token: process.env.TELEGRAM_BOT_TOKEN?.trim() || '',
    chatId: process.env.TELEGRAM_TEST_CHAT_ID?.trim() || process.env.TELEGRAM_CHAT_ID?.trim() || ''
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

export function getApiPort() {
  return process.env.PORT || 4000;
}
