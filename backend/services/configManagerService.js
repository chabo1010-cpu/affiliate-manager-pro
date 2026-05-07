import fs from 'fs';
import {
  getApiPort,
  getDatabaseConfig,
  getKeepaConfig,
  getStorageConfig,
  getTelegramConfig,
  getTelegramTestGroupConfig,
  getTelegramUserReaderConfig,
  getWhatsappControlConfig,
  getWhatsappDeliveryConfig,
  getWhatsappPlaywrightConfig
} from '../env.js';
import { getTelegramBotClientConfig } from './telegramBotClientService.js';

function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function maskSecret(value, options = {}) {
  const trimmed = cleanText(value);
  if (!trimmed) {
    return '';
  }

  const visibleStart = options.visibleStart ?? 3;
  const visibleEnd = options.visibleEnd ?? 2;

  if (trimmed.length <= visibleStart + visibleEnd) {
    return `${trimmed.slice(0, 1)}***`;
  }

  return `${trimmed.slice(0, visibleStart)}***${trimmed.slice(-visibleEnd)}`;
}

export const CONFIG_MANAGER_ENV_STRUCTURE = [
  {
    section: 'telegramUserApi',
    title: 'Telegram User API',
    keys: [
      {
        key: 'TELEGRAM_USER_API_ENABLED',
        required: false,
        secret: false,
        defaultValue: '0',
        example: '1',
        description: 'Aktiviert den Telegram User Client fuer Lesen per User API.'
      },
      {
        key: 'TELEGRAM_USER_API_ID',
        required: true,
        secret: false,
        defaultValue: '',
        example: '12345678',
        description: 'Telegram API ID fuer GramJS / User Login.'
      },
      {
        key: 'TELEGRAM_USER_API_HASH',
        required: true,
        secret: true,
        defaultValue: '',
        example: '0123456789abcdef0123456789abcdef',
        description: 'Telegram API Hash fuer die User API.'
      },
      {
        key: 'TELEGRAM_USER_PHONE',
        required: false,
        secret: false,
        defaultValue: '',
        example: '+491234567890',
        description: 'Optionale Telefonnummer fuer Telefon-Login.'
      },
      {
        key: 'TELEGRAM_USER_LOGIN_MODE',
        required: false,
        secret: false,
        defaultValue: 'phone',
        example: 'qr',
        description: 'Standard-Loginmodus: phone oder qr.'
      },
      {
        key: 'TELEGRAM_USER_SESSION_DIR',
        required: false,
        secret: false,
        defaultValue: './backend/data/telegram-user-sessions',
        example: 'C:\\\\app\\\\data\\\\telegram-user-sessions',
        description: 'Persistenter Speicherort fuer Telegram-User-Sessions.'
      }
    ]
  },
  {
    section: 'telegramBot',
    title: 'Telegram Bot Output',
    keys: [
      {
        key: 'TELEGRAM_BOT_TOKEN',
        required: true,
        secret: true,
        defaultValue: '',
        example: '123456:ABCDEF_your_bot_token',
        description: 'Bot Token fuer Output nach Telegram.'
      },
      {
        key: 'TELEGRAM_CHAT_ID',
        required: false,
        secret: false,
        defaultValue: '',
        example: '-1001234567890',
        description: 'Fallback-Ziel fuer Telegram Output.'
      },
      {
        key: 'TELEGRAM_TEST_CHAT_ID',
        required: false,
        secret: false,
        defaultValue: '',
        example: '-1009876543210',
        description: 'Optionale Testgruppe fuer manuelle Generator-Posts.'
      }
    ]
  },
  {
    section: 'keepa',
    title: 'Keepa API',
    keys: [
      {
        key: 'KEEPA_API_KEY',
        required: true,
        secret: true,
        defaultValue: '',
        example: 'your_keepa_key',
        description: 'Keepa API Key fuer Fallback-Daten.'
      },
      {
        key: 'KEEPA_DEFAULT_DOMAIN_ID',
        required: false,
        secret: false,
        defaultValue: '3',
        example: '3',
        description: 'Standard-Domain fuer Amazon.de.'
      },
      {
        key: 'KEEPA_TIMEOUT_MS',
        required: false,
        secret: false,
        defaultValue: '12000',
        example: '12000',
        description: 'HTTP Timeout fuer Keepa Requests.'
      },
      {
        key: 'KEEPA_RETRY_LIMIT',
        required: false,
        secret: false,
        defaultValue: '2',
        example: '2',
        description: 'Retry-Limit fuer Keepa Requests.'
      },
      {
        key: 'KEEPA_REQUEST_INTERVAL_MS',
        required: false,
        secret: false,
        defaultValue: '1200',
        example: '1200',
        description: 'Abstand zwischen Keepa Requests.'
      }
    ]
  },
  {
    section: 'whatsapp',
    title: 'WhatsApp Delivery',
    keys: [
      {
        key: 'WHATSAPP_DELIVERY_ENABLED',
        required: false,
        secret: false,
        defaultValue: '0',
        example: '1',
        description: 'Aktiviert WhatsApp Output.'
      },
      {
        key: 'WHATSAPP_DELIVERY_ENDPOINT',
        required: false,
        secret: false,
        defaultValue: '',
        example: 'https://example.internal/whatsapp/send',
        description: 'API Endpoint fuer WhatsApp Versand.'
      },
      {
        key: 'WHATSAPP_DELIVERY_TOKEN',
        required: false,
        secret: true,
        defaultValue: '',
        example: 'whatsapp_secret_token',
        description: 'Optionales Auth-Token fuer den WhatsApp Endpoint.'
      },
      {
        key: 'WHATSAPP_DELIVERY_SENDER',
        required: false,
        secret: false,
        defaultValue: '',
        example: 'affiliate-manager-pro',
        description: 'Optionaler Absender / Kanalname fuer WhatsApp.'
      },
      {
        key: 'WHATSAPP_DELIVERY_RETRY_LIMIT',
        required: false,
        secret: false,
        defaultValue: '3',
        example: '3',
        description: 'Retry-Limit fuer WhatsApp Jobs.'
      },
      {
        key: 'WHATSAPP_KEEP_BROWSER_OPEN',
        required: false,
        secret: false,
        defaultValue: '0',
        example: '1',
        description: 'Debug-Modus: laesst den sichtbaren Playwright-Browser offen und ueberspringt automatisches Close/Recovery.'
      },
      {
        key: 'WHATSAPP_CONTROL_ENDPOINT',
        required: false,
        secret: false,
        defaultValue: '',
        example: 'http://127.0.0.1:8787/whatsapp/control',
        description: 'Optionaler Control Endpoint fuer Session-, QR- und Health-Aktionen.'
      },
      {
        key: 'WHATSAPP_CONTROL_TOKEN',
        required: false,
        secret: true,
        defaultValue: '',
        example: 'whatsapp_control_secret',
        description: 'Optionales Auth-Token fuer den WhatsApp Control Endpoint.'
      },
      {
        key: 'WHATSAPP_INSTANCE_ID',
        required: false,
        secret: false,
        defaultValue: 'primary',
        example: 'primary',
        description: 'Technische Instanz-ID fuer die erste WhatsApp Worker Instanz.'
      },
      {
        key: 'WHATSAPP_SESSION_DIR',
        required: false,
        secret: false,
        defaultValue: './backend/data/whatsapp-session',
        example: 'C:\\\\app\\\\data\\\\whatsapp-session',
        description: 'Persistenter Speicherort fuer lokale WhatsApp Session-Metadaten.'
      },
      {
        key: 'WHATSAPP_PLAYWRIGHT_BROWSER_CHANNEL',
        required: false,
        secret: false,
        defaultValue: 'msedge',
        example: 'chrome',
        description: 'Bevorzugter lokaler Chromium-Kanal fuer den internen WhatsApp Worker.'
      },
      {
        key: 'WHATSAPP_PLAYWRIGHT_EXECUTABLE_PATH',
        required: false,
        secret: false,
        defaultValue: '',
        example: 'C:\\\\Program Files\\\\Google\\\\Chrome\\\\Application\\\\chrome.exe',
        description: 'Optionaler Pfad zu einem lokalen Chromium/Edge Browser fuer Playwright.'
      },
      {
        key: 'WHATSAPP_PLAYWRIGHT_HEADLESS',
        required: false,
        secret: false,
        defaultValue: '0',
        example: '1',
        description: 'Startet den internen WhatsApp Browser auf Wunsch headless.'
      },
      {
        key: 'WHATSAPP_PLAYWRIGHT_WEB_URL',
        required: false,
        secret: false,
        defaultValue: 'https://web.whatsapp.com/',
        example: 'https://web.whatsapp.com/',
        description: 'Basis-URL fuer den internen WhatsApp Web Worker.'
      }
    ]
  },
  {
    section: 'database',
    title: 'Database / Storage',
    keys: [
      {
        key: 'APP_DATA_DIR',
        required: false,
        secret: false,
        defaultValue: './backend/data',
        example: 'C:\\\\app\\\\data',
        description: 'Basisverzeichnis fuer persistente Daten.'
      },
      {
        key: 'APP_DB_PATH',
        required: false,
        secret: false,
        defaultValue: './backend/data/deals.db',
        example: 'C:\\\\app\\\\data\\\\deals.db',
        description: 'Pfad zur zentralen SQLite Datenbank.'
      },
      {
        key: 'PORT',
        required: false,
        secret: false,
        defaultValue: '4000',
        example: '4000',
        description: 'Backend-Port.'
      }
    ]
  }
];

export function getConfigManagerEnvStructure() {
  return CONFIG_MANAGER_ENV_STRUCTURE.map((section) => ({
    ...section,
    keys: section.keys.map((entry) => ({
      ...entry
    }))
  }));
}

export function getConfigManagerSnapshot() {
  const storageConfig = getStorageConfig();
  const databaseConfig = getDatabaseConfig();
  const telegramUserReaderConfig = getTelegramUserReaderConfig();
  const telegramConfig = getTelegramConfig();
  const telegramTestGroupConfig = getTelegramTestGroupConfig();
  const telegramBotClientConfig = getTelegramBotClientConfig();
  const keepaConfig = getKeepaConfig();
  const whatsappConfig = getWhatsappDeliveryConfig();
  const whatsappControlConfig = getWhatsappControlConfig();
  const whatsappPlaywrightConfig = getWhatsappPlaywrightConfig();

  return {
    envPath: storageConfig.envPath,
    port: Number.parseInt(String(getApiPort() || '4000'), 10) || 4000,
    modules: {
      telegramUserApi: {
        enabled: telegramUserReaderConfig.enabled,
        apiIdConfigured: Boolean(cleanText(telegramUserReaderConfig.apiId)),
        apiHashConfigured: Boolean(cleanText(telegramUserReaderConfig.apiHash)),
        phoneConfigured: Boolean(cleanText(telegramUserReaderConfig.phoneNumber)),
        loginMode: telegramUserReaderConfig.loginMode,
        sessionDir: telegramUserReaderConfig.sessionDir
      },
      telegramBot: {
        tokenConfigured: Boolean(cleanText(telegramConfig.token)),
        maskedToken: maskSecret(telegramConfig.token, { visibleStart: 4, visibleEnd: 3 }),
        defaultChatConfigured: Boolean(cleanText(telegramConfig.chatId)),
        testChatConfigured: Boolean(cleanText(telegramTestGroupConfig.chatId)),
        maskedDefaultChatId: maskSecret(telegramConfig.chatId),
        maskedTestChatId: maskSecret(telegramTestGroupConfig.chatId),
        configuredTargets: telegramBotClientConfig.targets.length,
        publishTargets: telegramBotClientConfig.effectiveTargets.length
      },
      keepa: {
        keyConfigured: Boolean(cleanText(keepaConfig.key)),
        maskedKey: maskSecret(keepaConfig.key, { visibleStart: 4, visibleEnd: 2 }),
        defaultDomainId: keepaConfig.defaultDomainId,
        timeoutMs: keepaConfig.timeoutMs,
        retryLimit: keepaConfig.retryLimit,
        requestIntervalMs: keepaConfig.requestIntervalMs
      },
      whatsapp: {
        enabled: whatsappConfig.enabled,
        endpointConfigured: Boolean(cleanText(whatsappConfig.endpoint)),
        endpoint: cleanText(whatsappConfig.endpoint),
        tokenConfigured: Boolean(cleanText(whatsappConfig.token)),
        maskedToken: maskSecret(whatsappConfig.token, { visibleStart: 3, visibleEnd: 2 }),
        sender: cleanText(whatsappConfig.sender),
        retryLimit: whatsappConfig.retryLimit,
        controlEndpointConfigured: Boolean(cleanText(whatsappControlConfig.endpoint)),
        controlEndpoint: cleanText(whatsappControlConfig.endpoint),
        controlTokenConfigured: Boolean(cleanText(whatsappControlConfig.token)),
        maskedControlToken: maskSecret(whatsappControlConfig.token, { visibleStart: 3, visibleEnd: 2 }),
        instanceId: cleanText(whatsappControlConfig.instanceId),
        sessionDir: cleanText(whatsappControlConfig.sessionDir),
        playwrightBrowserChannel: cleanText(whatsappPlaywrightConfig.browserChannel),
        playwrightExecutablePath: cleanText(whatsappPlaywrightConfig.executablePath),
        playwrightHeadless: whatsappPlaywrightConfig.headless === true,
        playwrightWebUrl: cleanText(whatsappPlaywrightConfig.webUrl)
      },
      database: {
        dataDir: databaseConfig.dataDir,
        dbPath: databaseConfig.dbPath,
        envPath: databaseConfig.envPath,
        journalMode: databaseConfig.journalMode,
        telegramUserSessionDir: storageConfig.telegramUserSessionDir,
        whatsappSessionDir: storageConfig.whatsappSessionDir,
        dbExists: fs.existsSync(databaseConfig.dbPath),
        dataDirExists: fs.existsSync(databaseConfig.dataDir)
      }
    }
  };
}
