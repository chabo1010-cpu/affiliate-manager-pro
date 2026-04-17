import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const envPath = path.join(__dirname, '.env');

function loadDotEnvFile() {
  if (!fs.existsSync(envPath)) {
    return;
  }

  const contents = fs.readFileSync(envPath, 'utf8');
  const lines = contents.split(/\r?\n/);

  for (const line of lines) {
    const trimmed = line.trim();

    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }

    const separatorIndex = trimmed.indexOf('=');
    if (separatorIndex === -1) {
      continue;
    }

    const key = trimmed.slice(0, separatorIndex).trim();
    const rawValue = trimmed.slice(separatorIndex + 1).trim();
    const value = rawValue.replace(/^['"]|['"]$/g, '');

    if (key && process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

loadDotEnvFile();

export function getTelegramConfig() {
  return {
    token: process.env.TELEGRAM_BOT_TOKEN?.trim() || '',
    chatId: process.env.TELEGRAM_CHAT_ID?.trim() || ''
  };
}

export function getKeepaConfig() {
  return {
    key: process.env.KEEPA_API_KEY?.trim() || '',
    defaultDomainId: Number.parseInt(process.env.KEEPA_DEFAULT_DOMAIN_ID || '3', 10) || 3,
    timeoutMs: Number.parseInt(process.env.KEEPA_TIMEOUT_MS || '12000', 10) || 12000,
    retryLimit: Number.parseInt(process.env.KEEPA_RETRY_LIMIT || '2', 10) || 2,
    requestIntervalMs: Number.parseInt(process.env.KEEPA_REQUEST_INTERVAL_MS || '1200', 10) || 1200
  };
}

export function getApiPort() {
  return process.env.PORT || 4000;
}
