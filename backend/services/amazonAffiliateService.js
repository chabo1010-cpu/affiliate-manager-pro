import crypto from 'crypto';
import { getDb } from '../db.js';
import { getAmazonAffiliateConfig, getAmazonCreatorApiConfig, getStorageConfig } from '../env.js';
import { buildAmazonAffiliateLinkRecord, cleanText } from './dealHistoryService.js';
import { logGeneratorDebug } from './generatorFlowService.js';

const db = getDb();
const AMAZON_DEPRECATION_DATE = '2026-04-30';
const AMAZON_TEST_ASIN = 'B0DDKZBYK6';
const AMAZON_SERVICE_NAME = 'ProductAdvertisingAPI';
const AMAZON_API_PATH = '/paapi5/getitems';
const AMAZON_SEARCH_API_PATH = '/paapi5/searchitems';
const AMAZON_VARIATIONS_API_PATH = '/paapi5/getvariations';
const AMAZON_TARGET = 'com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems';
const AMAZON_SEARCH_TARGET = 'com.amazon.paapi5.v1.ProductAdvertisingAPIv1.SearchItems';
const AMAZON_VARIATIONS_TARGET = 'com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetVariations';
const AMAZON_OFFER_RESOURCES = [
  'ItemInfo.Title',
  'ItemInfo.ByLineInfo',
  'ItemInfo.Features',
  'Images.Primary.Large',
  'Images.Primary.Medium',
  'OffersV2.Listings.Price',
  'OffersV2.Listings.MerchantInfo',
  'OffersV2.Listings.DeliveryInfo.IsAmazonFulfilled',
  'OffersV2.Listings.DeliveryInfo.IsPrimeEligible',
  'Offers.Listings.Price',
  'Offers.Listings.MerchantInfo',
  'Offers.Listings.DeliveryInfo.IsAmazonFulfilled',
  'Offers.Listings.DeliveryInfo.IsPrimeEligible',
  'Offers.Listings.Availability.Message'
];
const AMAZON_OFFER_FALLBACK_RESOURCES = AMAZON_OFFER_RESOURCES.filter((resource) => !resource.startsWith('OffersV2.'));
const AMAZON_DEFAULT_RESOURCES = AMAZON_OFFER_RESOURCES;
const AMAZON_SEARCH_RESOURCES = [
  ...AMAZON_OFFER_RESOURCES,
  'ItemInfo.Classifications'
];
const AMAZON_SEARCH_FALLBACK_RESOURCES = AMAZON_SEARCH_RESOURCES.filter((resource) => !resource.startsWith('OffersV2.'));
const AMAZON_VARIATION_RESOURCES = [
  ...AMAZON_OFFER_RESOURCES,
  'ItemInfo.Classifications',
  'VariationSummary.VariationDimension',
  'VariationSummary.Price.HighestPrice',
  'VariationSummary.Price.LowestPrice'
];
const AMAZON_VARIATION_FALLBACK_RESOURCES = AMAZON_OFFER_RESOURCES;
const AMAZON_CREATOR_DEFAULT_RESOURCES = [
  'itemInfo.title',
  'itemInfo.byLineInfo',
  'itemInfo.features',
  'itemInfo.classifications',
  'images.primary.small',
  'images.primary.medium',
  'images.primary.large',
  'offersV2.listings.price',
  'offersV2.listings.availability',
  'offersV2.listings.merchantInfo'
];
const AMAZON_PAAPI_MARKETPLACE_DE = 'www.amazon.de';
const AMAZON_PAAPI_CONDITION = 'New';
const AMAZON_PAAPI_MERCHANT = 'All';
const AMAZON_PAAPI_OFFER_COUNT = 1;
const AMAZON_CREATOR_TOKEN_REFRESH_BUFFER_MS = 60 * 1000;
const AMAZON_TEST_RATE_LIMIT_MS = 1000;
const AMAZON_THROTTLE_RETRY_DELAYS_MS = [10_000, 20_000];

let amazonConfigLogWritten = false;
let amazonCreatorTokenCache = {
  cacheKey: '',
  accessToken: '',
  expiresIn: 0,
  expiresAt: 0,
  storedAt: ''
};
let amazonApiRateLimitQueue = Promise.resolve();
let amazonApiNextAllowedAt = 0;
const amazonOfferDataCache = new Map();
const amazonVariationDataCache = new Map();

function nowIso() {
  return new Date().toISOString();
}

function parseJson(value, fallback = null) {
  try {
    return value ? JSON.parse(value) : fallback;
  } catch {
    return fallback;
  }
}

function toJson(value) {
  return JSON.stringify(value ?? null);
}

function maskSecret(value, visibleStart = 4, visibleEnd = 2) {
  const trimmed = cleanText(value);
  if (!trimmed) {
    return '';
  }

  if (trimmed.length <= visibleStart + visibleEnd) {
    return `${trimmed.slice(0, 2)}***`;
  }

  return `${trimmed.slice(0, visibleStart)}***${trimmed.slice(-visibleEnd)}`;
}

function buildAmazonPaapiEndpoint(config = {}) {
  return `https://${cleanText(config.host)}${AMAZON_API_PATH}`;
}

function getAccessKeyPrefix(accessKey = '') {
  return cleanText(accessKey).slice(0, 4) || '';
}

function hasAmazonAffiliateCredentials(config = {}) {
  return Boolean(config.enabled && config.accessKey && config.secretKey && config.partnerTag);
}

function hasAmazonCreatorApiCredentials(config = {}) {
  return Boolean(config.enabled && config.clientId && config.clientSecret && config.partnerTag && config.endpoint && config.authEndpoint);
}

function buildAmazonCreatorEndpoint(config = {}) {
  return cleanText(config.endpoint) || 'https://creatorsapi.amazon/catalog/v1/getItems';
}

function buildAmazonCreatorTokenCacheKey(config = {}) {
  return [
    cleanText(config.authEndpoint),
    cleanText(config.clientId),
    cleanText(config.credentialVersion),
    cleanText(config.marketplace)
  ].join('|');
}

function isAmazonCreatorTokenCacheValid(config = {}) {
  return (
    amazonCreatorTokenCache.cacheKey === buildAmazonCreatorTokenCacheKey(config) &&
    Boolean(amazonCreatorTokenCache.accessToken) &&
    Date.now() + AMAZON_CREATOR_TOKEN_REFRESH_BUFFER_MS < Number(amazonCreatorTokenCache.expiresAt || 0)
  );
}

function buildAmazonCreatorTokenRequest(config = {}) {
  const isLegacyCredential = cleanText(config.credentialVersion).startsWith('2.');

  if (isLegacyCredential) {
    return {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Authorization: `Basic ${Buffer.from(`${config.clientId}:${config.clientSecret}`).toString('base64')}`
      },
      body: new URLSearchParams({
        grant_type: 'client_credentials',
        scope: 'creatorsapi/default'
      }).toString()
    };
  }

  return {
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      grant_type: 'client_credentials',
      client_id: config.clientId,
      client_secret: config.clientSecret,
      scope: 'creatorsapi::default'
    })
  };
}

function ensureAmazonConfigLog(config = {}) {
  if (amazonConfigLogWritten) {
    return;
  }

  amazonConfigLogWritten = true;
  const storageConfig = getStorageConfig();
  const creatorConfig = getAmazonCreatorApiConfig();
  const accessKeyMasked = maskSecret(config.accessKey, 4, 4);
  const secretLength = cleanText(config.secretKey).length;

  console.info('[PAAPI_ENV_LOADED]', {
    envPath: storageConfig.envPath,
    loadedKeys: [
      'AMAZON_PAAPI_ACCESS_KEY',
      'AMAZON_PAAPI_SECRET_KEY',
      'AMAZON_PAAPI_PARTNER_TAG',
      'AMAZON_PAAPI_HOST',
      'AMAZON_PAAPI_REGION',
      'AMAZON_PAAPI_MARKETPLACE',
      'AMAZON_PAAPI_LANGUAGE',
      'AMAZON_PAAPI_ENABLED',
      'AMAZON_PAAPI_TIMEOUT_MS'
    ],
    accessKeyLoaded: Boolean(config.accessKey),
    secretKeyLoaded: Boolean(config.secretKey),
    partnerTagLoaded: Boolean(config.partnerTag)
  });
  console.info('[PAAPI_ACCESS_KEY_MASKED]', {
    accessKeyMasked
  });
  console.info('[PAAPI_SECRET_LENGTH]', {
    secretLength
  });
  console.info('[PAAPI_PARTNER_TAG]', {
    partnerTag: config.partnerTag || ''
  });
  console.info('[PAAPI_HOST_REGION_MARKETPLACE]', {
    host: config.host || '',
    region: config.region || '',
    marketplace: config.marketplace || '',
    language: config.language || '',
    enabled: config.enabled === true
  });
  console.info('[CREATOR_API_ENV_LOADED]', {
    envPath: storageConfig.envPath,
    loadedKeys: [
      'AMAZON_CREATOR_API_ENABLED',
      'AMAZON_CREATOR_API_KEY',
      'AMAZON_CREATOR_API_CLIENT_ID',
      'AMAZON_CREATOR_API_CREDENTIAL_ID',
      'AMAZON_CREATOR_API_SECRET',
      'AMAZON_CREATOR_API_CLIENT_SECRET',
      'AMAZON_CREATOR_API_CREDENTIAL_SECRET',
      'AMAZON_CREATOR_API_PARTNER_TAG',
      'AMAZON_CREATOR_API_CREDENTIAL_VERSION',
      'AMAZON_CREATOR_API_TOKEN_ENDPOINT',
      'AMAZON_CREATOR_API_AUTH_ENDPOINT',
      'AMAZON_CREATOR_API_ENDPOINT',
      'AMAZON_CREATOR_API_MARKETPLACE',
      'AMAZON_CREATOR_API_TIMEOUT_MS'
    ],
    enabled: creatorConfig.enabled === true,
    configured: hasAmazonCreatorApiCredentials(creatorConfig),
    credentialVersion: creatorConfig.credentialVersion || '',
    authEndpoint: creatorConfig.authEndpoint || '',
    endpoint: creatorConfig.endpoint || '',
    marketplace: creatorConfig.marketplace || '',
    clientIdLoaded: Boolean(creatorConfig.clientId),
    clientSecretLoaded: Boolean(creatorConfig.clientSecret),
    partnerTagLoaded: Boolean(creatorConfig.partnerTag)
  });
  logGeneratorDebug('AMAZON API CONFIG LOADED', {
    enabled: config.enabled === true,
    configured: hasAmazonAffiliateCredentials(config),
    host: config.host || '',
    region: config.region || '',
    marketplace: config.marketplace || '',
    partnerTagConfigured: Boolean(config.partnerTag),
    deprecationDate: AMAZON_DEPRECATION_DATE
  });
  logGeneratorDebug('AMAZON CREATOR API CONFIG LOADED', {
    enabled: creatorConfig.enabled === true,
    configured: hasAmazonCreatorApiCredentials(creatorConfig),
    credentialVersion: creatorConfig.credentialVersion || '',
    endpoint: creatorConfig.endpoint || '',
    marketplace: creatorConfig.marketplace || '',
    partnerTagConfigured: Boolean(creatorConfig.partnerTag)
  });
}

function logAmazonApiEvent(level = 'info', eventType, operation, message, extra = {}) {
  db.prepare(
    `
      INSERT INTO amazon_api_logs (
        level,
        event_type,
        operation,
        asin,
        status,
        message,
        request_meta_json,
        response_meta_json,
        created_at
      ) VALUES (
        @level,
        @eventType,
        @operation,
        @asin,
        @status,
        @message,
        @requestMetaJson,
        @responseMetaJson,
        @createdAt
      )
    `
  ).run({
    level,
    eventType,
    operation,
    asin: cleanText(extra.asin).toUpperCase() || null,
    status: cleanText(extra.status) || 'info',
    message,
    requestMetaJson: extra.requestMeta ? toJson(extra.requestMeta) : null,
    responseMetaJson: extra.responseMeta ? toJson(extra.responseMeta) : null,
    createdAt: nowIso()
  });
}

function createAmazonAffiliateError(message, code = 'AMAZON_API_ERROR', statusCode = 500, details = null) {
  const error = new Error(message);
  error.code = code;
  error.statusCode = statusCode;
  error.details = details;
  return error;
}

function hashSha256Hex(value) {
  return crypto.createHash('sha256').update(value, 'utf8').digest('hex');
}

function hmacSha256(key, value, encoding = undefined) {
  return crypto.createHmac('sha256', key).update(value, 'utf8').digest(encoding);
}

function buildAmzDate(date = new Date()) {
  return date.toISOString().replace(/[:-]|\.\d{3}/g, '');
}

function buildDateStamp(date = new Date()) {
  return buildAmzDate(date).slice(0, 8);
}

function buildSigningKey(secretKey, dateStamp, region, service) {
  const kDate = hmacSha256(`AWS4${secretKey}`, dateStamp);
  const kRegion = hmacSha256(kDate, region);
  const kService = hmacSha256(kRegion, service);
  return hmacSha256(kService, 'aws4_request');
}

function buildCanonicalRequest(payload, host, amzDate, target = AMAZON_TARGET, apiPath = AMAZON_API_PATH) {
  const canonicalHeaders = {
    'content-encoding': 'amz-1.0',
    'content-type': 'application/json; charset=utf-8',
    host,
    'x-amz-date': amzDate,
    'x-amz-target': target
  };
  const signedHeaders = Object.keys(canonicalHeaders).sort().join(';');
  const canonicalHeaderString = Object.keys(canonicalHeaders)
    .sort()
    .map((key) => `${key}:${canonicalHeaders[key]}`)
    .join('\n');

  return {
    canonicalRequest: [
      'POST',
      apiPath,
      '',
      `${canonicalHeaderString}\n`,
      signedHeaders,
      hashSha256Hex(payload)
    ].join('\n'),
    signedHeaders,
    headers: canonicalHeaders
  };
}

function buildAuthorizationHeader(config, payload, date = new Date(), target = AMAZON_TARGET, apiPath = AMAZON_API_PATH) {
  const amzDate = buildAmzDate(date);
  const dateStamp = buildDateStamp(date);
  console.info('[PAAPI_SIGNING_START]', {
    host: config.host,
    region: config.region,
    service: AMAZON_SERVICE_NAME,
    target,
    path: apiPath,
    marketplace: config.marketplace,
    amzDate
  });
  const { canonicalRequest, signedHeaders, headers } = buildCanonicalRequest(payload, config.host, amzDate, target, apiPath);
  const credentialScope = `${dateStamp}/${config.region}/${AMAZON_SERVICE_NAME}/aws4_request`;
  const stringToSign = ['AWS4-HMAC-SHA256', amzDate, credentialScope, hashSha256Hex(canonicalRequest)].join('\n');
  const signingKey = buildSigningKey(config.secretKey, dateStamp, config.region, AMAZON_SERVICE_NAME);
  const signature = hmacSha256(signingKey, stringToSign, 'hex');
  const authorizationHeader = `AWS4-HMAC-SHA256 Credential=${config.accessKey}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

  console.info('[PAAPI_CANONICAL_REQUEST]', {
    canonicalRequest
  });
  console.info('[PAAPI_STRING_TO_SIGN]', {
    stringToSign
  });
  console.info('[PAAPI_AUTH_HEADER_CREATED]', {
    credentialScope,
    signedHeaders,
    accessKeyPrefix: getAccessKeyPrefix(config.accessKey),
    authHeaderPreview: `${authorizationHeader.slice(0, 96)}...`
  });
  console.info('[PAAPI_INVALID_SIGNATURE_FIXED]', {
    signingService: AMAZON_SERVICE_NAME,
    target,
    note: 'AWS SigV4 uses ProductAdvertisingAPI while x-amz-target stays ProductAdvertisingAPIv1.*'
  });

  return {
    headers: {
      ...headers,
      Authorization: authorizationHeader
    },
    amzDate,
    canonicalRequest,
    stringToSign,
    credentialScope
  };
}

function deriveErrorStatus(error) {
  const code = cleanText(error?.code || '').toUpperCase();
  const statusCode = Number(error?.statusCode || 0);
  const message = cleanText(error?.message).toLowerCase();

  if (isAmazonApiThrottlingError(error) || message.includes('request throttling')) {
    return 'throttled';
  }

  if (code.includes('MISSING') || code.includes('NOT_CONFIGURED')) {
    return 'not_configured';
  }

  if (code.includes('AUTH') || statusCode === 401 || statusCode === 403) {
    return 'auth_error';
  }

  if (code.includes('NO_ITEMS') || code.includes('NOT_FOUND')) {
    return 'no_hits';
  }

  return 'api_error';
}

function sleep(ms = 0) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
}

function isAmazonApiTestModeActive() {
  return process.env.READER_TEST_MODE === '1';
}

function isAmazonApiThrottlingError(error = {}) {
  const code = cleanText(error?.code || '').toLowerCase();
  const message = cleanText(error?.message || '').toLowerCase();
  const statusCode = Number(error?.statusCode || error?.details?.httpStatus || 0);

  return statusCode === 429 || code.includes('toomanyrequests') || code.includes('throttl') || message.includes('request throttling');
}

function getAmazonThrottleRetryDelayMs(retry = 0) {
  return AMAZON_THROTTLE_RETRY_DELAYS_MS[Math.min(Math.max(0, Number(retry) || 0), AMAZON_THROTTLE_RETRY_DELAYS_MS.length - 1)];
}

async function waitForAmazonApiRateLimit(context = {}) {
  if (!isAmazonApiTestModeActive()) {
    return;
  }

  const operation = cleanText(context.operation) || 'AmazonAPI';
  const asin = cleanText(context.asin).toUpperCase() || null;
  const query = cleanText(context.query) || null;
  const queuedWait = amazonApiRateLimitQueue.then(async () => {
    const waitMs = Math.max(0, amazonApiNextAllowedAt - Date.now());
    if (waitMs > 0) {
      console.info('[AMAZON_API_RATE_LIMIT_WAIT]', {
        operation,
        asin,
        query,
        waitMs
      });
      await sleep(waitMs);
    }
    amazonApiNextAllowedAt = Date.now() + AMAZON_TEST_RATE_LIMIT_MS;
  });

  amazonApiRateLimitQueue = queuedWait.catch(() => {});
  await queuedWait;
}

function getSimilarOfferCacheTtlMs() {
  const hours = Number.parseInt(process.env.SIMILAR_OFFER_CACHE_TTL_HOURS || '', 10);
  if (Number.isFinite(hours) && hours > 0) {
    return hours * 60 * 60 * 1000;
  }

  const minutes = Number.parseInt(process.env.SIMILAR_OFFER_CACHE_TTL_MINUTES || '60', 10);
  return Math.max(1, Number.isFinite(minutes) ? minutes : 60) * 60 * 1000;
}

function getSimilarVariantCacheTtlMs() {
  const hours = Number.parseInt(process.env.SIMILAR_VARIANT_CACHE_TTL_HOURS || '', 10);
  if (Number.isFinite(hours) && hours > 0) {
    return hours * 60 * 60 * 1000;
  }

  return getSimilarOfferCacheTtlMs();
}

function getSimilarVariantLimit(defaultLimit = 10) {
  const envLimit = Number.parseInt(process.env.SIMILAR_VARIANT_LIMIT || '', 10);
  const limit = Number.isFinite(envLimit) && envLimit > 0 ? envLimit : defaultLimit;
  return Math.max(1, Math.min(10, limit));
}

function getSimilarEnrichLimit(defaultLimit = 10) {
  const envLimit = Number.parseInt(process.env.SIMILAR_ENRICH_LIMIT || '', 10);
  const fallback = isAmazonApiTestModeActive() ? 3 : Math.min(defaultLimit, 3);
  const limit = Number.isFinite(envLimit) && envLimit > 0 ? envLimit : fallback;
  return Math.max(1, Math.min(10, limit));
}

function getCachedAmazonOfferData(asin = '') {
  const normalizedAsin = cleanText(asin).toUpperCase();
  const cached = amazonOfferDataCache.get(normalizedAsin);
  if (!cached) {
    return null;
  }

  if (Date.now() - Number(cached.cachedAtMs || 0) > getSimilarOfferCacheTtlMs()) {
    amazonOfferDataCache.delete(normalizedAsin);
    return null;
  }

  return {
    ...cached.item,
    offerCacheHit: true,
    offerCachedAt: cached.cachedAt
  };
}

function setCachedAmazonOfferData(item = {}) {
  const asin = cleanText(item.asin).toUpperCase();
  if (!asin) {
    return;
  }

  const sellerDebug = classifyMappedAmazonSellerForDebug(item);
  amazonOfferDataCache.set(asin, {
    cachedAtMs: Date.now(),
    cachedAt: nowIso(),
    item: {
      ...item,
      sellerClass: sellerDebug.sellerClass,
      sellerSource: sellerDebug.source,
      sellerRecognitionReason: sellerDebug.reason
    }
  });
}

function getCachedAmazonVariationData(asin = '') {
  const normalizedAsin = cleanText(asin).toUpperCase();
  const cached = amazonVariationDataCache.get(normalizedAsin);
  if (!cached) {
    return null;
  }

  if (Date.now() - Number(cached.cachedAtMs || 0) > getSimilarVariantCacheTtlMs()) {
    amazonVariationDataCache.delete(normalizedAsin);
    return null;
  }

  return {
    ...cached,
    cacheHit: true
  };
}

function setCachedAmazonVariationData(asin = '', payload = {}) {
  const normalizedAsin = cleanText(asin).toUpperCase();
  if (!normalizedAsin) {
    return;
  }

  amazonVariationDataCache.set(normalizedAsin, {
    cachedAtMs: Date.now(),
    cachedAt: nowIso(),
    ...payload
  });
}

function parseAmazonAffiliatePriceValue(value = null) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  const text = cleanText(value);
  if (!text) {
    return null;
  }

  const raw = text.replace(/[^0-9.,-]/g, '');
  if (!raw) {
    return null;
  }

  let normalized = raw;
  if (raw.includes(',') && raw.includes('.')) {
    normalized = raw.lastIndexOf(',') > raw.lastIndexOf('.') ? raw.replace(/\./g, '').replace(',', '.') : raw.replace(/,/g, '');
  } else if (raw.includes(',')) {
    normalized = raw.replace(',', '.');
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function parsePositiveAmazonAffiliatePriceValue(value = null) {
  const parsed = parseAmazonAffiliatePriceValue(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function resolveAmazonAffiliateListing(item = {}) {
  return (
    item?.OffersV2?.Listings?.[0] ||
    item?.Offers?.Listings?.[0] ||
    item?.offersV2?.listings?.[0] ||
    item?.offers?.listings?.[0] ||
    null
  );
}

function resolveAmazonAffiliateRawPriceObject(item = {}, listing = null) {
  return (
    listing?.Price ||
    listing?.price ||
    item?.OffersV2?.Listings?.[0]?.Price ||
    item?.OffersV2?.Listings?.[0]?.price ||
    item?.Offers?.Listings?.[0]?.Price ||
    item?.Offers?.Listings?.[0]?.price ||
    item?.offersV2?.listings?.[0]?.price ||
    item?.offersV2?.listings?.[0]?.Price ||
    item?.offers?.listings?.[0]?.price ||
    item?.offers?.listings?.[0]?.Price ||
    item?.price ||
    item?.Price ||
    {}
  );
}

function resolveAmazonAffiliatePriceDisplay(rawPriceObject = {}, item = {}) {
  return (
    cleanText(rawPriceObject?.DisplayAmount) ||
    cleanText(rawPriceObject?.displayAmount) ||
    cleanText(rawPriceObject?.Money?.DisplayAmount) ||
    cleanText(rawPriceObject?.money?.displayAmount) ||
    cleanText(item?.OffersV2?.Summaries?.[0]?.LowestPrice?.DisplayAmount) ||
    cleanText(item?.OffersV2?.Summaries?.[0]?.LowestPrice?.Money?.DisplayAmount) ||
    cleanText(item?.offersV2?.summaries?.[0]?.lowestPrice?.displayAmount) ||
    cleanText(item?.offersV2?.summaries?.[0]?.lowestPrice?.money?.displayAmount) ||
    ''
  );
}

function resolveAmazonAffiliatePriceValue(rawPriceObject = {}, item = {}, priceDisplay = '') {
  const rawCandidates = [
    rawPriceObject?.Money?.Amount,
    rawPriceObject?.money?.amount,
    rawPriceObject?.Amount,
    rawPriceObject?.amount,
    item?.OffersV2?.Listings?.[0]?.Price?.Money?.Amount,
    item?.OffersV2?.Listings?.[0]?.Price?.Amount,
    item?.Offers?.Listings?.[0]?.Price?.Amount,
    item?.offersV2?.listings?.[0]?.price?.money?.amount,
    item?.offersV2?.listings?.[0]?.price?.amount,
    item?.offers?.listings?.[0]?.price?.amount,
    item?.OffersV2?.Summaries?.[0]?.LowestPrice?.Money?.Amount,
    item?.OffersV2?.Summaries?.[0]?.LowestPrice?.Amount,
    item?.offersV2?.summaries?.[0]?.lowestPrice?.money?.amount,
    item?.offersV2?.summaries?.[0]?.lowestPrice?.amount,
    item?.price?.amount,
    item?.Price?.Amount
  ];

  for (const candidate of rawCandidates) {
    const parsed = parsePositiveAmazonAffiliatePriceValue(candidate);
    if (parsed !== null) {
      return parsed;
    }
  }

  return parsePositiveAmazonAffiliatePriceValue(priceDisplay);
}

function collectAmazonSellerKeys(item = {}, listing = null) {
  const keys = [];
  const addKey = (condition, key) => {
    if (condition) {
      keys.push(key);
    }
  };

  addKey(Boolean(listing?.MerchantInfo || listing?.merchantInfo), 'Offers.Listings.MerchantInfo');
  addKey(Boolean(listing?.MerchantInfo?.Name || listing?.merchantInfo?.name), 'Offers.Listings.MerchantInfo.Name');
  addKey(Boolean(listing?.MerchantInfo?.DisplayName || listing?.merchantInfo?.displayName), 'Offers.Listings.MerchantInfo.DisplayName');
  addKey(Boolean(listing?.DeliveryInfo || listing?.deliveryInfo), 'Offers.Listings.DeliveryInfo');
  addKey(
    listing?.DeliveryInfo?.IsAmazonFulfilled !== undefined || listing?.deliveryInfo?.isAmazonFulfilled !== undefined,
    'Offers.Listings.DeliveryInfo.IsAmazonFulfilled'
  );
  addKey(
    listing?.DeliveryInfo?.IsPrimeEligible !== undefined || listing?.deliveryInfo?.isPrimeEligible !== undefined,
    'Offers.Listings.DeliveryInfo.IsPrimeEligible'
  );
  addKey(Boolean(listing?.Availability?.Message || listing?.availability?.message), 'Offers.Listings.Availability.Message');
  addKey(Boolean(item?.MerchantInfo || item?.merchantInfo), 'Item.MerchantInfo');

  return [...new Set(keys)];
}

function normalizeAmazonVariationAttributes(item = {}) {
  const rawAttributes = firstCreatorArray(
    item?.VariationAttributes,
    item?.variationAttributes,
    item?.VariationSummary?.VariationDimension,
    item?.variationSummary?.variationDimension,
    item?._creatorRawItem?.variationAttributes,
    item?._creatorRawItem?.VariationAttributes
  );

  return rawAttributes
    .map((entry) => {
      if (typeof entry === 'string') {
        return {
          name: '',
          value: cleanText(entry)
        };
      }

      return {
        name: cleanText(entry?.Name || entry?.name || entry?.DisplayName || entry?.displayName),
        value: cleanText(entry?.Value || entry?.value || entry?.DisplayValue || entry?.displayValue)
      };
    })
    .filter((entry) => entry.name || entry.value);
}

function resolveAmazonVariationLabel(item = {}) {
  const attributes = normalizeAmazonVariationAttributes(item);
  const label = attributes
    .map((entry) => [entry.name, entry.value].filter(Boolean).join(': '))
    .filter(Boolean)
    .join(' / ');

  return label || cleanText(item?.VariationSummary?.VariationDimension?.DisplayValue || item?.variationSummary?.variationDimension?.displayValue);
}

function buildAmazonSellerDebug(item = {}, listing = null, api = 'paapi') {
  const rawSellerKeysFound = collectAmazonSellerKeys(item, listing);
  const hasMerchantInfo = rawSellerKeysFound.some((key) => key.includes('MerchantInfo'));
  const hasDeliveryInfo = rawSellerKeysFound.some((key) => key.includes('DeliveryInfo'));

  return {
    api,
    rawSellerKeysFound,
    hasMerchantInfo,
    hasDeliveryInfo,
    sellerDataMissing: !hasMerchantInfo && !hasDeliveryInfo
  };
}

function summarizeAmazonRawItemKeys(item = {}) {
  const keys = Object.keys(item || {});
  const listing = resolveAmazonAffiliateListing(item);
  const nestedKeys = [
    item?.ItemInfo ? 'ItemInfo' : '',
    item?.ItemInfo?.Title ? 'ItemInfo.Title' : '',
    item?.ItemInfo?.ByLineInfo ? 'ItemInfo.ByLineInfo' : '',
    item?.ItemInfo?.Features ? 'ItemInfo.Features' : '',
    item?.Images?.Primary ? 'Images.Primary' : '',
    item?.Offers ? 'Offers' : '',
    item?.Offers?.Listings ? 'Offers.Listings' : '',
    item?.OffersV2 ? 'OffersV2' : '',
    item?.OffersV2?.Listings ? 'OffersV2.Listings' : '',
    listing?.Price || listing?.price ? 'Listing.Price' : '',
    listing?.MerchantInfo || listing?.merchantInfo ? 'Listing.MerchantInfo' : '',
    listing?.DeliveryInfo || listing?.deliveryInfo ? 'Listing.DeliveryInfo' : ''
  ].filter(Boolean);

  return [...new Set([...keys, ...nestedKeys])];
}

function isAmazonResourceValidationError(error) {
  const text = cleanText(error?.message || error?.code || '').toLowerCase();
  return text.includes('resource') || text.includes('resources') || text.includes('validation');
}

function classifyMappedAmazonSellerForDebug(item = {}) {
  const merchantName = cleanText(item.merchantName);
  const merchantIsAmazon = Boolean(merchantName && /amazon/i.test(merchantName));

  if (merchantIsAmazon) {
    return {
      sellerClass: 'AMAZON_DIRECT',
      source: 'MerchantInfo.Name enthaelt Amazon.',
      reason: 'Amazon verkauft direkt.'
    };
  }

  if (item.isAmazonFulfilled === true) {
    return {
      sellerClass: 'FBA',
      source: 'DeliveryInfo.IsAmazonFulfilled=true.',
      reason: 'Drittanbieter mit Versand durch Amazon.'
    };
  }

  if (merchantName && item.isAmazonFulfilled !== true) {
    return {
      sellerClass: 'FBM',
      source: 'MerchantInfo.Name ist Drittanbieter und IsAmazonFulfilled ist nicht true.',
      reason: 'Drittanbieter ohne Amazon-Fulfillment.'
    };
  }

  if (item.isPrimeEligible === true) {
    return {
      sellerClass: 'FBA_OR_AMAZON_UNKNOWN',
      source: 'Prime=true, aber MerchantInfo/Fulfillment fehlen oder sind unklar.',
      reason: 'Amazon-Fulfillment wahrscheinlich, aber nicht eindeutig.'
    };
  }

  return {
    sellerClass: 'UNKNOWN',
    source: item.sellerDataMissing ? 'Keine echten Seller Felder in API Response gefunden.' : item.sellerSource || 'Seller unklar.',
    reason: 'API hat keine eindeutigen Offer/Seller-Daten geliefert.'
  };
}

function mapAmazonItem(item = {}, fallbackAsin = '', options = {}) {
  const asin = cleanText(item?.ASIN || fallbackAsin).toUpperCase();
  const detailPageUrl = cleanText(item?.DetailPageURL);
  const linkRecord = buildAmazonAffiliateLinkRecord(detailPageUrl || asin, {
    asin
  });
  const listing = resolveAmazonAffiliateListing(item);
  const deliveryInfo = listing?.DeliveryInfo || listing?.deliveryInfo || {};
  const merchantInfo = listing?.MerchantInfo || listing?.merchantInfo || {};
  const sellerDebug = buildAmazonSellerDebug(item, listing, cleanText(options.api) || 'paapi');
  const rawPriceObject = resolveAmazonAffiliateRawPriceObject(item, listing);
  const image =
    item?.Images?.Primary?.Large?.URL ||
    item?.Images?.Primary?.Medium?.URL ||
    item?.Images?.Primary?.Small?.URL ||
    '';
  const features = Array.isArray(item?.ItemInfo?.Features?.DisplayValues)
    ? item.ItemInfo.Features.DisplayValues.filter((value) => cleanText(value))
    : [];
  const priceDisplay = resolveAmazonAffiliatePriceDisplay(rawPriceObject, item);
  const priceValue = resolveAmazonAffiliatePriceValue(rawPriceObject, item, priceDisplay);
  const availability =
    cleanText(listing?.Availability?.Message) ||
    cleanText(item?.OffersV2?.Summaries?.[0]?.Condition?.DisplayValue) ||
    '';
  const variationAttributes = normalizeAmazonVariationAttributes(item);
  const variationLabel = resolveAmazonVariationLabel(item);

  if (sellerDebug.sellerDataMissing) {
    console.warn('[SELLER_DATA_MISSING]', {
      asin,
      api: sellerDebug.api,
      availableKeys: sellerDebug.rawSellerKeysFound
    });
  }

  return {
    asin,
    parentAsin: cleanText(item?.ParentASIN || item?.parentAsin || item?.ParentAsin).toUpperCase(),
    variationAttributes,
    variationLabel,
    title: cleanText(item?.ItemInfo?.Title?.DisplayValue),
    brand: cleanText(item?.ItemInfo?.ByLineInfo?.Brand?.DisplayValue),
    categoryName:
      cleanText(item?.ItemInfo?.Classifications?.ProductGroup?.DisplayValue) ||
      cleanText(item?.ItemInfo?.Classifications?.Binding?.DisplayValue),
    imageUrl: image,
    detailPageUrl,
    normalizedUrl: linkRecord.valid ? linkRecord.normalizedUrl : '',
    affiliateUrl: linkRecord.valid ? linkRecord.affiliateUrl : detailPageUrl,
    features,
    priceDisplay,
    priceValue,
    extractedPrice: priceValue,
    priceValid: Number.isFinite(priceValue) && priceValue > 0,
    rawPriceObject,
    availability,
    merchantName: cleanText(merchantInfo?.Name || merchantInfo?.name),
    isAmazonFulfilled: deliveryInfo?.IsAmazonFulfilled === true || deliveryInfo?.isAmazonFulfilled === true,
    isPrimeEligible: deliveryInfo?.IsPrimeEligible === true || deliveryInfo?.isPrimeEligible === true,
    sellerSource: sellerDebug.sellerDataMissing ? 'Keine Seller Felder in API Response gefunden.' : 'Amazon API Seller-Felder gelesen.',
    rawSellerKeysFound: sellerDebug.rawSellerKeysFound,
    sellerDataMissing: sellerDebug.sellerDataMissing,
    sellerDebug,
    rawItem: item
  };
}

function cleanCreatorText(value) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value);
  }

  return typeof value === 'string' ? value.trim() : '';
}

function firstCleanCreatorText(...values) {
  return values.map((value) => cleanCreatorText(value)).find(Boolean) || '';
}

function firstCreatorArray(...values) {
  return values.find((value) => Array.isArray(value)) || [];
}

function normalizeCreatorItemToPaapiShape(item = {}, fallbackAsin = '') {
  const listing =
    item?.offers?.listings?.[0] ||
    item?.offersV2?.listings?.[0] ||
    item?.Offers?.Listings?.[0] ||
    item?.OffersV2?.Listings?.[0] ||
    {};
  const offerSummary =
    item?.offersV2?.summaries?.[0] ||
    item?.OffersV2?.Summaries?.[0] ||
    item?.offers?.summaries?.[0] ||
    item?.Offers?.Summaries?.[0] ||
    {};
  const merchantInfo = listing?.merchantInfo || listing?.MerchantInfo || item?.merchantInfo || item?.MerchantInfo || {};
  const price = listing?.price || listing?.Price || offerSummary?.lowestPrice || offerSummary?.LowestPrice || {};
  const availability = listing?.availability || listing?.Availability || {};
  const asin = firstCleanCreatorText(item?.asin, item?.ASIN, fallbackAsin).toUpperCase();
  const detailPageUrl = firstCleanCreatorText(item?.detailPageUrl, item?.detailPageURL, item?.DetailPageURL);
  const imageUrl = firstCleanCreatorText(
    item?.images?.primary?.large?.url,
    item?.images?.primary?.medium?.url,
    item?.images?.primary?.small?.url,
    item?.Images?.Primary?.Large?.URL,
    item?.Images?.Primary?.Medium?.URL,
    item?.Images?.Primary?.Small?.URL
  );
  const featureValues = firstCreatorArray(
    item?.itemInfo?.features?.displayValues,
    item?.ItemInfo?.Features?.DisplayValues,
    item?.itemInfo?.features,
    item?.ItemInfo?.Features
  );
  const merchantName = firstCleanCreatorText(
    item?.merchantName,
    item?.sellerName,
    item?.merchantName?.displayValue,
    item?.sellerName?.displayValue,
    listing?.merchantName,
    listing?.sellerName,
    listing?.merchant?.name,
    listing?.seller?.name,
    merchantInfo?.name,
    merchantInfo?.Name,
    item?.offers?.listings?.[0]?.merchantName,
    item?.offers?.listings?.[0]?.sellerName,
    item?.Offers?.Listings?.[0]?.merchantName,
    item?.Offers?.Listings?.[0]?.sellerName
  );

  return {
    ASIN: asin,
    DetailPageURL: detailPageUrl,
    ItemInfo: {
      Title: {
        DisplayValue: firstCleanCreatorText(item?.itemInfo?.title?.displayValue, item?.ItemInfo?.Title?.DisplayValue, item?.title)
      },
      ByLineInfo: {
        Brand: {
          DisplayValue: firstCleanCreatorText(
            item?.itemInfo?.byLineInfo?.brand?.displayValue,
            item?.ItemInfo?.ByLineInfo?.Brand?.DisplayValue,
            item?.brand
          )
        }
      },
      Classifications: {
        ProductGroup: {
          DisplayValue: firstCleanCreatorText(
            item?.itemInfo?.classifications?.productGroup?.displayValue,
            item?.ItemInfo?.Classifications?.ProductGroup?.DisplayValue
          )
        },
        Binding: {
          DisplayValue: firstCleanCreatorText(
            item?.itemInfo?.classifications?.binding?.displayValue,
            item?.ItemInfo?.Classifications?.Binding?.DisplayValue
          )
        }
      },
      Features: {
        DisplayValues: featureValues.map((value) => cleanCreatorText(value)).filter(Boolean)
      }
    },
    Images: {
      Primary: {
        Large: { URL: imageUrl },
        Medium: { URL: imageUrl },
        Small: { URL: imageUrl }
      }
    },
    Offers: {
      Listings: [
        {
          Price: {
            DisplayAmount: firstCleanCreatorText(
              price?.displayAmount,
              price?.DisplayAmount,
              price?.money?.displayAmount,
              price?.Money?.DisplayAmount
            ),
            Amount: price?.money?.amount ?? price?.Money?.Amount ?? price?.amount ?? price?.Amount ?? null,
            Money: {
              Amount: price?.money?.amount ?? price?.Money?.Amount ?? price?.amount ?? price?.Amount ?? null,
              Currency: firstCleanCreatorText(price?.money?.currency, price?.Money?.Currency, price?.currency, price?.Currency)
            },
            Currency: firstCleanCreatorText(price?.money?.currency, price?.Money?.Currency, price?.currency, price?.Currency)
          },
          Availability: {
            Message: firstCleanCreatorText(availability?.message, availability?.Message)
          },
          MerchantInfo: {
            Name: merchantName,
            DisplayName: merchantName
          }
        }
      ]
    },
    OffersV2: item?.offersV2 || item?.OffersV2 || null,
    _creatorRawItem: item
  };
}

function mapAmazonCreatorItem(item = {}, fallbackAsin = '', config = {}) {
  const normalizedItem = normalizeCreatorItemToPaapiShape(item, fallbackAsin);

  return {
    ...mapAmazonItem(normalizedItem, fallbackAsin, {
      api: 'creator_api'
    }),
    sourceLabel: 'Amazon Creator API',
    dataSource: 'creator_api',
    marketplace: config.marketplace || '',
    creatorApi: true,
    rawCreatorItem: item,
    rawItem: normalizedItem
  };
}

function extractAmazonCreatorItems(responseJson = {}) {
  return firstCreatorArray(
    responseJson?.itemsResult?.items,
    responseJson?.ItemsResult?.Items,
    responseJson?.items,
    responseJson?.Items,
    responseJson?.data?.itemsResult?.items,
    responseJson?.data?.ItemsResult?.Items,
    responseJson?.data?.items,
    responseJson?.data?.Items
  );
}

function normalizeAmazonAsinList(value = []) {
  const rawValues = Array.isArray(value) ? value : [value];
  return [
    ...new Set(
      rawValues
        .map((entry) => cleanText(entry).toUpperCase())
        .filter(Boolean)
    )
  ];
}

async function requestAmazonCreatorAccessTokenOnce(config = {}, context = {}) {
  const cacheKey = buildAmazonCreatorTokenCacheKey(config);
  const existingTokenForConfig = amazonCreatorTokenCache.cacheKey === cacheKey && Boolean(amazonCreatorTokenCache.accessToken);
  const refreshRequired = existingTokenForConfig && !isAmazonCreatorTokenCacheValid(config);

  if (isAmazonCreatorTokenCacheValid(config)) {
    return amazonCreatorTokenCache.accessToken;
  }

  const requestMeta = {
    asin: cleanText(context.asin).toUpperCase() || null,
    credentialVersion: config.credentialVersion || '',
    authEndpoint: config.authEndpoint || '',
    clientIdMasked: maskSecret(config.clientId, 4, 4),
    cacheHit: false,
    refreshRequired
  };
  const { headers, body } = buildAmazonCreatorTokenRequest(config);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(1000, Number(config.timeoutMs || 12000)));

  console.info('[CREATOR_API_TOKEN_REQUEST_START]', requestMeta);
  logAmazonApiEvent('info', 'amazon.creator.token.start', 'CreatorToken', 'Amazon Creator API Token Request gestartet.', {
    asin: context.asin,
    status: 'pending',
    requestMeta
  });

  try {
    const response = await fetch(config.authEndpoint, {
      method: 'POST',
      headers,
      body,
      signal: controller.signal
    });
    const responseText = await response.text();
    const responseJson = parseJson(responseText, null);
    const accessToken = cleanText(responseJson?.access_token || responseJson?.accessToken);

    if (!response.ok || !accessToken) {
      const errorMessage =
        cleanText(responseJson?.error_description) ||
        cleanText(responseJson?.message) ||
        cleanText(responseJson?.error) ||
        `Amazon Creator API Token Request fehlgeschlagen (${response.status}).`;
      const error = createAmazonAffiliateError(errorMessage, 'AMAZON_CREATOR_TOKEN_ERROR', response.status || 502, {
        response: responseJson,
        httpStatus: response.status || 502
      });

      console.error('[CREATOR_API_TOKEN_FAILED]', {
        ...requestMeta,
        httpStatus: response.status,
        errorMessage
      });
      logAmazonApiEvent('error', 'amazon.creator.token.error', 'CreatorToken', errorMessage, {
        asin: context.asin,
        status: deriveErrorStatus(error),
        requestMeta,
        responseMeta: {
          statusCode: response.status
        }
      });
      throw error;
    }

    const expiresIn = Number(responseJson?.expires_in || responseJson?.expiresIn || 3600) || 3600;
    amazonCreatorTokenCache = {
      cacheKey,
      accessToken,
      expiresIn,
      expiresAt: Date.now() + expiresIn * 1000,
      storedAt: nowIso()
    };

    console.info('[CREATOR_API_TOKEN_SUCCESS]', {
      ...requestMeta,
      expiresIn,
      expiresAt: new Date(amazonCreatorTokenCache.expiresAt).toISOString()
    });
    if (refreshRequired) {
      console.info('[CREATOR_API_TOKEN_REFRESHED]', {
        ...requestMeta,
        expiresIn,
        expiresAt: new Date(amazonCreatorTokenCache.expiresAt).toISOString()
      });
    }
    logAmazonApiEvent('info', 'amazon.creator.token.success', 'CreatorToken', 'Amazon Creator API Token empfangen.', {
      asin: context.asin,
      status: 'token_success',
      requestMeta,
      responseMeta: {
        statusCode: response.status,
        expiresIn,
        expiresAt: new Date(amazonCreatorTokenCache.expiresAt).toISOString()
      }
    });

    return accessToken;
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') {
      const timeoutError = createAmazonAffiliateError(
        'Amazon Creator API Token Request hat das Timeout ueberschritten.',
        'AMAZON_CREATOR_TOKEN_TIMEOUT',
        504
      );
      console.error('[CREATOR_API_TOKEN_FAILED]', {
        ...requestMeta,
        httpStatus: 504,
        errorMessage: timeoutError.message
      });
      logAmazonApiEvent('error', 'amazon.creator.token.timeout', 'CreatorToken', timeoutError.message, {
        asin: context.asin,
        status: 'api_error',
        requestMeta,
        responseMeta: {
          statusCode: 504
        }
      });
      throw timeoutError;
    }

    if (error instanceof Error && error.code && error.statusCode) {
      throw error;
    }

    const networkError = createAmazonAffiliateError(
      error instanceof Error ? error.message : 'Amazon Creator API Token Request fehlgeschlagen.',
      'AMAZON_CREATOR_TOKEN_NETWORK_ERROR',
      502
    );
    console.error('[CREATOR_API_TOKEN_FAILED]', {
      ...requestMeta,
      httpStatus: 502,
      errorMessage: networkError.message
    });
    logAmazonApiEvent('error', 'amazon.creator.token.network_error', 'CreatorToken', networkError.message, {
      asin: context.asin,
      status: 'api_error',
      requestMeta,
      responseMeta: {
        statusCode: 502
      }
    });
    throw networkError;
  } finally {
    clearTimeout(timeout);
  }
}

async function requestAmazonCreatorAccessToken(config = {}, context = {}) {
  const operation = 'CreatorToken';
  const asin = cleanText(context.asin).toUpperCase() || null;
  const maxRetries = isAmazonApiTestModeActive() ? 1 : 0;
  let lastError = null;

  for (let retry = 0; retry <= maxRetries; retry += 1) {
    await waitForAmazonApiRateLimit({
      operation,
      asin
    });

    try {
      return await requestAmazonCreatorAccessTokenOnce(config, context);
    } catch (error) {
      if (!isAmazonApiThrottlingError(error)) {
        throw error;
      }

      lastError = error;
      error.apiStatus = 'THROTTLED';
      console.warn('[AMAZON_API_THROTTLED]', {
        operation,
        asin,
        retry,
        maxRetries,
        statusCode: error.statusCode || error.details?.httpStatus || null,
        message: error instanceof Error ? error.message : 'Amazon Creator token throttled.'
      });

      if (retry >= maxRetries) {
        console.warn('[AMAZON_API_THROTTLED_ABORTED]', {
          operation,
          asin,
          retry,
          maxRetries,
          statusCode: error.statusCode || error.details?.httpStatus || null,
          message: error instanceof Error ? error.message : 'Amazon Creator token throttled.'
        });
        throw error;
      }

      const waitMs = getAmazonThrottleRetryDelayMs(retry);
      console.info('[AMAZON_API_RATE_LIMIT_WAIT]', {
        operation,
        asin,
        waitMs,
        reason: 'throttled_retry'
      });
      await sleep(waitMs);
    }
  }

  throw lastError || createAmazonAffiliateError('Amazon Creator API Token Request fehlgeschlagen.', 'AMAZON_CREATOR_TOKEN_ERROR', 502);
}

async function requestAmazonCreatorApiGetItemsOnce(asinInput, config = {}) {
  const itemIds = normalizeAmazonAsinList(asinInput);
  const asin = itemIds.join(',');
  const requestMeta = {
    operation: 'GetItems',
    asin,
    itemIds,
    endpoint: buildAmazonCreatorEndpoint(config),
    marketplace: config.marketplace || '',
    credentialVersion: config.credentialVersion || '',
    partnerTagMasked: maskSecret(config.partnerTag, 4, 3)
  };
  const token = await requestAmazonCreatorAccessToken(config, { asin });
  const isLegacyCredential = cleanText(config.credentialVersion).startsWith('2.');
  const payload = {
    itemIds,
    itemIdType: 'ASIN',
    marketplace: config.marketplace,
    partnerTag: config.partnerTag,
    resources: AMAZON_CREATOR_DEFAULT_RESOURCES
  };
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(1000, Number(config.timeoutMs || 12000)));

  console.info('[CREATOR_API_RESOURCES_FIXED]', {
    asin,
    resources: AMAZON_CREATOR_DEFAULT_RESOURCES
  });
  console.info('[CREATOR_API_VALID_RESOURCES_USED]', {
    asin,
    resources: payload.resources
  });
  console.info('[CREATOR_API_REQUEST_START]', {
    ...requestMeta,
    resources: payload.resources
  });
  logAmazonApiEvent('info', 'amazon.creator.request.start', 'CreatorGetItems', 'Amazon Creator API GetItems gestartet.', {
    asin,
    status: 'pending',
    requestMeta
  });

  try {
    const response = await fetch(buildAmazonCreatorEndpoint(config), {
      method: 'POST',
      headers: {
        Authorization: isLegacyCredential ? `Bearer ${token}, Version ${config.credentialVersion}` : `Bearer ${token}`,
        'Content-Type': 'application/json',
        'x-marketplace': config.marketplace
      },
      body: JSON.stringify(payload),
      signal: controller.signal
    });
    const responseText = await response.text();
    const responseJson = parseJson(responseText, null);
    const apiErrors = firstCreatorArray(responseJson?.errors, responseJson?.Errors);

    console.info('[CREATOR_API_HTTP_STATUS]', {
      asin,
      httpStatus: response.status,
      ok: response.ok
    });

    if (!response.ok || apiErrors.length > 0) {
      const topError = apiErrors[0] || {};
      const errorMessage =
        cleanText(topError?.message || topError?.Message) ||
        cleanText(responseJson?.message || responseJson?.Message) ||
        `Amazon Creator API GetItems fehlgeschlagen (${response.status}).`;
      const error = createAmazonAffiliateError(errorMessage, 'AMAZON_CREATOR_API_ERROR', response.status || 502, {
        response: responseJson,
        httpStatus: response.status || 502
      });

      console.error('[CREATOR_API_REQUEST_ERROR]', {
        ...requestMeta,
        httpStatus: response.status,
        errorMessage
      });
      logAmazonApiEvent('error', 'amazon.creator.request.error', 'CreatorGetItems', errorMessage, {
        asin,
        status: deriveErrorStatus(error),
        requestMeta,
        responseMeta: {
          statusCode: response.status
        }
      });
      throw error;
    }

    const items = extractAmazonCreatorItems(responseJson);
    console.info('[CREATOR_API_REQUEST_SUCCESS]', {
      ...requestMeta,
      itemCount: items.length
    });
    logAmazonApiEvent(
      'info',
      'amazon.creator.request.success',
      'CreatorGetItems',
      'Amazon Creator API GetItems Antwort empfangen.',
      {
        asin,
        status: items.length > 0 ? 'success' : 'no_hits',
        requestMeta,
        responseMeta: {
          statusCode: response.status,
          itemCount: items.length
        }
      }
    );

    return responseJson;
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') {
      const timeoutError = createAmazonAffiliateError(
        'Amazon Creator API GetItems hat das Timeout ueberschritten.',
        'AMAZON_CREATOR_API_TIMEOUT',
        504
      );
      console.error('[CREATOR_API_REQUEST_ERROR]', {
        ...requestMeta,
        httpStatus: 504,
        errorMessage: timeoutError.message
      });
      logAmazonApiEvent('error', 'amazon.creator.request.timeout', 'CreatorGetItems', timeoutError.message, {
        asin,
        status: 'api_error',
        requestMeta,
        responseMeta: {
          statusCode: 504
        }
      });
      throw timeoutError;
    }

    if (error instanceof Error && error.code && error.statusCode) {
      throw error;
    }

    const networkError = createAmazonAffiliateError(
      error instanceof Error ? error.message : 'Amazon Creator API GetItems fehlgeschlagen.',
      'AMAZON_CREATOR_API_NETWORK_ERROR',
      502
    );
    console.error('[CREATOR_API_REQUEST_ERROR]', {
      ...requestMeta,
      httpStatus: 502,
      errorMessage: networkError.message
    });
    logAmazonApiEvent('error', 'amazon.creator.request.network_error', 'CreatorGetItems', networkError.message, {
      asin,
      status: 'api_error',
      requestMeta,
      responseMeta: {
        statusCode: 502
      }
    });
    throw networkError;
  } finally {
    clearTimeout(timeout);
  }
}

async function requestAmazonCreatorApiGetItems(asin, config = {}) {
  const operation = 'CreatorGetItems';
  const asinContext = normalizeAmazonAsinList(asin).join(',');
  const maxRetries = isAmazonApiTestModeActive() ? 1 : 0;
  let lastError = null;

  for (let retry = 0; retry <= maxRetries; retry += 1) {
    await waitForAmazonApiRateLimit({
      operation,
      asin: asinContext
    });

    try {
      return await requestAmazonCreatorApiGetItemsOnce(asin, config);
    } catch (error) {
      if (!isAmazonApiThrottlingError(error)) {
        throw error;
      }

      lastError = error;
      error.apiStatus = 'THROTTLED';
      console.warn('[AMAZON_API_THROTTLED]', {
        operation,
        asin: asinContext,
        retry,
        maxRetries,
        statusCode: error.statusCode || error.details?.httpStatus || null,
        message: error instanceof Error ? error.message : 'Amazon Creator API throttled.'
      });

      if (retry >= maxRetries) {
        console.warn('[AMAZON_API_THROTTLED_ABORTED]', {
          operation,
          asin: asinContext,
          retry,
          maxRetries,
          statusCode: error.statusCode || error.details?.httpStatus || null,
          message: error instanceof Error ? error.message : 'Amazon Creator API throttled.'
        });
        throw error;
      }

      const waitMs = getAmazonThrottleRetryDelayMs(retry);
      console.info('[AMAZON_API_RATE_LIMIT_WAIT]', {
        operation,
        asin: asinContext,
        waitMs,
        reason: 'throttled_retry'
      });
      await sleep(waitMs);
    }
  }

  throw lastError || createAmazonAffiliateError('Amazon Creator API GetItems fehlgeschlagen.', 'AMAZON_CREATOR_API_ERROR', 502);
}

async function requestAmazonProductAdvertisingApiOnce(payload, context = {}) {
  const config = getAmazonAffiliateConfig();
  ensureAmazonConfigLog(config);

  if (!config.enabled) {
    throw createAmazonAffiliateError(
      'Amazon Product Advertising API ist im Backend deaktiviert.',
      'AMAZON_API_DISABLED',
      400
    );
  }

  if (!hasAmazonAffiliateCredentials(config)) {
    throw createAmazonAffiliateError(
      'Amazon Product Advertising API ist nicht vollstaendig im Backend konfiguriert.',
      'AMAZON_API_NOT_CONFIGURED',
      400
    );
  }

  const requestPayload = JSON.stringify(payload);
  const operation = cleanText(context.operation) || 'GetItems';
  const target = cleanText(context.target) || AMAZON_TARGET;
  const apiPath = cleanText(context.path) || AMAZON_API_PATH;
  const { headers, amzDate } = buildAuthorizationHeader(config, requestPayload, new Date(), target, apiPath);
  const requestMeta = {
    operation,
    asin: cleanText(context.asin).toUpperCase() || null,
    query: cleanText(context.query) || null,
    target,
    host: config.host,
    region: config.region,
    marketplace: config.marketplace,
    endpoint: `https://${config.host}${apiPath}`,
    accessKeyMasked: maskSecret(config.accessKey, 4, 3),
    partnerTagMasked: maskSecret(config.partnerTag, 4, 3),
    requestedAt: amzDate
  };

  logGeneratorDebug('AMAZON API REQUEST START', requestMeta);
  logAmazonApiEvent('info', 'amazon.request.start', operation, 'Amazon Product Advertising API Request gestartet.', {
    asin: context.asin,
    status: 'pending',
    requestMeta
  });

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(1000, Number(config.timeoutMs || 12000)));

  try {
    const response = await fetch(`https://${config.host}${apiPath}`, {
      method: 'POST',
      headers,
      body: requestPayload,
      signal: controller.signal
    });
    const responseText = await response.text();
    const responseJson = parseJson(responseText, null);
    const apiErrors = Array.isArray(responseJson?.Errors) ? responseJson.Errors : [];
    console.info('[PAAPI_HTTP_STATUS]', {
      asin: cleanText(context.asin).toUpperCase() || null,
      httpStatus: response.status,
      ok: response.ok
    });
    const requestId =
      cleanText(response.headers.get('x-amzn-requestid')) ||
      cleanText(response.headers.get('x-amzn-RequestId')) ||
      cleanText(responseJson?.RequestId) ||
      null;

    if (!response.ok || apiErrors.length) {
      const topError = apiErrors[0] || null;
      const errorMessage =
        cleanText(topError?.Message) ||
        cleanText(responseJson?.message) ||
        `Amazon API Request fehlgeschlagen (${response.status}).`;
      const errorCode = cleanText(topError?.Code) || `HTTP_${response.status || 500}`;
      const statusCode =
        errorCode === 'TooManyRequests'
          ? 429
          : errorCode === 'AccessDenied' || errorCode === 'InvalidSignature'
            ? 403
            : response.status || 502;
      const error = createAmazonAffiliateError(errorMessage, errorCode || 'AMAZON_API_ERROR', statusCode, {
        response: responseJson,
        requestId,
        httpStatus: statusCode
      });

      logGeneratorDebug('AMAZON API ERROR', {
        ...requestMeta,
        statusCode,
        errorCode,
        message: errorMessage
      });
      console.error('[AMAZON_PAAPI_REQUEST_ERROR]', {
        accessKeyMasked: requestMeta.accessKeyMasked,
        region: requestMeta.region,
        endpoint: requestMeta.endpoint,
        errorCode,
        errorMessage,
        httpStatus: statusCode,
        requestId
      });
      logAmazonApiEvent('error', 'amazon.request.error', operation, errorMessage, {
        asin: context.asin,
        status: deriveErrorStatus(error),
        requestMeta,
        responseMeta: {
          statusCode,
          errorCode
        }
      });

      throw error;
    }

    const responseMeta = {
      statusCode: response.status,
      itemCount:
        (Array.isArray(responseJson?.ItemsResult?.Items) ? responseJson.ItemsResult.Items.length : 0) ||
        (Array.isArray(responseJson?.SearchResult?.Items) ? responseJson.SearchResult.Items.length : 0)
    };
    logGeneratorDebug('AMAZON API RESPONSE RECEIVED', {
      ...requestMeta,
      ...responseMeta
    });
    logAmazonApiEvent(
      'info',
      'amazon.request.success',
      operation,
      'Amazon Product Advertising API Antwort empfangen.',
      {
        asin: context.asin,
        status: responseMeta.itemCount > 0 ? 'success' : 'no_hits',
        requestMeta,
        responseMeta
      }
    );

    return responseJson;
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') {
      const timeoutError = createAmazonAffiliateError(
        'Amazon API Request hat das Timeout ueberschritten.',
        'AMAZON_API_TIMEOUT',
        504
      );
      logGeneratorDebug('AMAZON API ERROR', {
        ...requestMeta,
        statusCode: 504,
        errorCode: timeoutError.code,
        message: timeoutError.message
      });
      console.error('[AMAZON_PAAPI_REQUEST_ERROR]', {
        accessKeyMasked: requestMeta.accessKeyMasked,
        region: requestMeta.region,
        endpoint: requestMeta.endpoint,
        errorCode: timeoutError.code,
        errorMessage: timeoutError.message,
        httpStatus: 504,
        requestId: null
      });
      logAmazonApiEvent('error', 'amazon.request.timeout', operation, timeoutError.message, {
        asin: context.asin,
        status: 'api_error',
        requestMeta,
        responseMeta: {
          statusCode: 504
        }
      });
      throw timeoutError;
    }

    if (error instanceof Error && error.code && error.statusCode) {
      throw error;
    }

    const networkError = createAmazonAffiliateError(
      error instanceof Error ? error.message : 'Amazon API Request fehlgeschlagen.',
      'AMAZON_API_NETWORK_ERROR',
      502
    );
    logGeneratorDebug('AMAZON API ERROR', {
      ...requestMeta,
      statusCode: 502,
      errorCode: networkError.code,
      message: networkError.message
    });
    console.error('[AMAZON_PAAPI_REQUEST_ERROR]', {
      accessKeyMasked: requestMeta.accessKeyMasked,
      region: requestMeta.region,
      endpoint: requestMeta.endpoint,
      errorCode: networkError.code,
      errorMessage: networkError.message,
      httpStatus: 502,
      requestId: null
    });
    logAmazonApiEvent('error', 'amazon.request.network_error', operation, networkError.message, {
      asin: context.asin,
      status: 'api_error',
      requestMeta,
      responseMeta: {
        statusCode: 502
      }
    });
    throw networkError;
  } finally {
    clearTimeout(timeout);
  }
}

async function requestAmazonProductAdvertisingApi(payload, context = {}) {
  const operation = cleanText(context.operation) || 'GetItems';
  const asin = cleanText(context.asin).toUpperCase() || null;
  const query = cleanText(context.query) || null;
  const maxRetries = context.noRetry === true ? 0 : isAmazonApiTestModeActive() ? 1 : 0;
  let lastError = null;

  for (let retry = 0; retry <= maxRetries; retry += 1) {
    await waitForAmazonApiRateLimit({
      operation,
      asin,
      query
    });

    try {
      return await requestAmazonProductAdvertisingApiOnce(payload, context);
    } catch (error) {
      if (!isAmazonApiThrottlingError(error)) {
        throw error;
      }

      lastError = error;
      error.apiStatus = 'THROTTLED';
      console.warn('[AMAZON_API_THROTTLED]', {
        operation,
        asin,
        query,
        retry,
        maxRetries,
        statusCode: error.statusCode || error.details?.httpStatus || null,
        message: error instanceof Error ? error.message : 'Amazon API throttled.'
      });

      if (retry >= maxRetries) {
        console.warn('[AMAZON_API_THROTTLED_ABORTED]', {
          operation,
          asin,
          query,
          retry,
          maxRetries,
          statusCode: error.statusCode || error.details?.httpStatus || null,
          message: error instanceof Error ? error.message : 'Amazon API throttled.'
        });
        throw error;
      }

      const waitMs = getAmazonThrottleRetryDelayMs(retry);
      console.info('[AMAZON_API_RATE_LIMIT_WAIT]', {
        operation,
        asin,
        query,
        waitMs,
        reason: 'throttled_retry'
      });
      await sleep(waitMs);
    }
  }

  throw lastError || createAmazonAffiliateError('Amazon API Request fehlgeschlagen.', 'AMAZON_API_ERROR', 502);
}

export async function searchAmazonAffiliateProducts(input = {}) {
  const keywords = cleanText(input.keywords || input.query).slice(0, 160);
  const searchIndex = cleanText(input.searchIndex) || 'All';
  const itemCount = Math.max(1, Math.min(10, Number.parseInt(input.itemCount || '10', 10) || 10));
  const requestedAt = nowIso();
  const config = getAmazonAffiliateConfig();
  ensureAmazonConfigLog(config);

  if (!keywords) {
    return {
      available: false,
      status: 'missing_query',
      requestedAt,
      reason: 'Suchquery fehlt fuer Similar Product Search.',
      items: []
    };
  }

  try {
    const requestSearchItems = async (resources = AMAZON_SEARCH_RESOURCES) => {
      console.info('[PAAPI_SEARCH_RESOURCES_USED]', {
        query: keywords,
        resources
      });
      return requestAmazonProductAdvertisingApi(
        {
          Keywords: keywords,
          SearchIndex: searchIndex,
          ItemCount: itemCount,
          PartnerTag: config.partnerTag,
          PartnerType: 'Associates',
          Marketplace: config.marketplace || AMAZON_PAAPI_MARKETPLACE_DE,
          Resources: resources
        },
        {
          operation: 'SearchItems',
          target: AMAZON_SEARCH_TARGET,
          path: AMAZON_SEARCH_API_PATH,
          query: keywords
        }
      );
    };

    let responseJson;
    try {
      responseJson = await requestSearchItems(AMAZON_SEARCH_RESOURCES);
    } catch (error) {
      if (!isAmazonResourceValidationError(error)) {
        throw error;
      }
      console.warn('[PAAPI_SEARCH_RESOURCES_FALLBACK]', {
        query: keywords,
        reason: error instanceof Error ? error.message : 'SearchItems Resource-Fallback',
        resources: AMAZON_SEARCH_FALLBACK_RESOURCES
      });
      responseJson = await requestSearchItems(AMAZON_SEARCH_FALLBACK_RESOURCES);
    }
    const rawItems = Array.isArray(responseJson?.SearchResult?.Items) ? responseJson.SearchResult.Items : [];
    for (const item of rawItems) {
      console.info('[PAAPI_SEARCH_ITEM_RAW_KEYS]', {
        asin: cleanText(item?.ASIN).toUpperCase(),
        keys: summarizeAmazonRawItemKeys(item)
      });
    }
    const items = rawItems.map((item) => mapAmazonItem(item));

    return {
      available: true,
      status: items.length ? 'success' : 'no_hits',
      requestedAt,
      query: keywords,
      items,
      rawResponse: responseJson
    };
  } catch (error) {
    return {
      available: false,
      status: deriveErrorStatus(error),
      requestedAt,
      query: keywords,
      reason: error instanceof Error ? error.message : 'Amazon SearchItems fehlgeschlagen.',
      items: []
    };
  }
}

function extractAmazonVariationItems(responseJson = {}) {
  return firstCreatorArray(
    responseJson?.VariationsResult?.Items,
    responseJson?.VariationsResult?.Variations,
    responseJson?.GetVariationsResult?.Items,
    responseJson?.GetVariationsResult?.Variations,
    responseJson?.variationsResult?.items,
    responseJson?.variationsResult?.variations,
    responseJson?.data?.VariationsResult?.Items,
    responseJson?.data?.GetVariationsResult?.Variations
  );
}

export async function loadAmazonAffiliateVariations(input = {}) {
  const asin = cleanText(input.asin).toUpperCase();
  const requestedAt = nowIso();
  const limit = getSimilarVariantLimit(input.limit || 10);
  const config = getAmazonAffiliateConfig();
  ensureAmazonConfigLog(config);

  if ((process.env.SIMILAR_VARIANT_CHECK_ENABLED || '0') !== '1') {
    return {
      available: false,
      status: 'disabled',
      requestedAt,
      asin,
      items: [],
      reason: 'SIMILAR_VARIANT_CHECK_ENABLED ist nicht aktiv.'
    };
  }

  if (!asin) {
    return {
      available: false,
      status: 'missing_asin',
      requestedAt,
      asin,
      items: [],
      reason: 'ASIN fehlt fuer Varianten-Scan.'
    };
  }

  const cached = getCachedAmazonVariationData(asin);
  if (cached) {
    console.info('[VARIANT_SCAN_DONE]', {
      asin,
      status: cached.status || 'cache_hit',
      count: Array.isArray(cached.items) ? cached.items.length : 0,
      cacheHit: true
    });
    return {
      available: true,
      status: cached.status || 'cache_hit',
      requestedAt,
      asin,
      items: Array.isArray(cached.items) ? cached.items.slice(0, limit) : [],
      cacheHit: true
    };
  }

  if (!config.enabled || !hasAmazonAffiliateCredentials(config)) {
    return {
      available: false,
      status: 'not_configured',
      requestedAt,
      asin,
      items: [],
      reason: 'PAAPI fuer GetVariations nicht konfiguriert.'
    };
  }

  console.info('[VARIANT_SCAN_START]', {
    asin,
    limit,
    cacheTtlMs: getSimilarVariantCacheTtlMs()
  });

  try {
    const requestGetVariations = async (resources = AMAZON_VARIATION_RESOURCES) => {
      return requestAmazonProductAdvertisingApi(
        {
          ASIN: asin,
          Marketplace: config.marketplace || AMAZON_PAAPI_MARKETPLACE_DE,
          LanguagesOfPreference: [config.language],
          Condition: AMAZON_PAAPI_CONDITION,
          Merchant: AMAZON_PAAPI_MERCHANT,
          OfferCount: AMAZON_PAAPI_OFFER_COUNT,
          VariationCount: limit,
          VariationPage: 1,
          PartnerTag: config.partnerTag,
          PartnerType: 'Associates',
          Resources: resources
        },
        {
          operation: 'GetVariations',
          target: AMAZON_VARIATIONS_TARGET,
          path: AMAZON_VARIATIONS_API_PATH,
          asin,
          noRetry: true
        }
      );
    };

    let responseJson;
    try {
      responseJson = await requestGetVariations(AMAZON_VARIATION_RESOURCES);
    } catch (error) {
      if (!isAmazonResourceValidationError(error)) {
        throw error;
      }
      console.warn('[PAAPI_VARIATIONS_RESOURCES_FALLBACK]', {
        asin,
        reason: error instanceof Error ? error.message : 'GetVariations Resource-Fallback',
        resources: AMAZON_VARIATION_FALLBACK_RESOURCES
      });
      responseJson = await requestGetVariations(AMAZON_VARIATION_FALLBACK_RESOURCES);
    }

    const rawItems = extractAmazonVariationItems(responseJson);
    const items = rawItems
      .map((item) => {
        const mappedItem = mapAmazonItem(item, cleanText(item?.ASIN).toUpperCase(), {
          api: 'paapi_getvariations'
        });
        setCachedAmazonOfferData(mappedItem);
        return mappedItem;
      })
      .filter((item) => item.asin)
      .slice(0, limit);

    setCachedAmazonVariationData(asin, {
      status: items.length ? 'success' : 'no_hits',
      items
    });
    console.info('[VARIANT_SCAN_DONE]', {
      asin,
      status: items.length ? 'success' : 'no_hits',
      count: items.length,
      cacheHit: false
    });

    return {
      available: true,
      status: items.length ? 'success' : 'no_hits',
      requestedAt,
      asin,
      items,
      rawResponse: responseJson
    };
  } catch (error) {
    const throttled = isAmazonApiThrottlingError(error);
    console.warn('[VARIANT_SCAN_DONE]', {
      asin,
      status: throttled ? 'throttled' : deriveErrorStatus(error),
      count: 0,
      reason: error instanceof Error ? error.message : 'GetVariations fehlgeschlagen.'
    });
    return {
      available: false,
      status: throttled ? 'throttled' : deriveErrorStatus(error),
      requestedAt,
      asin,
      items: [],
      reason: error instanceof Error ? error.message : 'GetVariations fehlgeschlagen.'
    };
  }
}

export async function enrichAmazonAffiliateProductsWithOfferData(candidates = [], input = {}) {
  const requestedAt = nowIso();
  const config = getAmazonAffiliateConfig();
  ensureAmazonConfigLog(config);
  const candidateList = Array.isArray(candidates) ? candidates : [];
  const requestedLimit = Math.max(1, Math.min(10, Number.parseInt(input.limit || '10', 10) || 10));
  const limit = getSimilarEnrichLimit(requestedLimit);
  const asins = [
    ...new Set(
      candidateList
        .map((candidate) => cleanText(candidate?.asin).toUpperCase())
        .filter(Boolean)
    )
  ].slice(0, limit);

  console.info('[API_SAVING_MODE_ACTIVE]', {
    readerTestMode: isAmazonApiTestModeActive(),
    candidateCount: candidateList.length,
    enrichLimit: limit,
    cacheTtlMs: getSimilarOfferCacheTtlMs(),
    rateLimitMs: AMAZON_TEST_RATE_LIMIT_MS,
    maxRetries: isAmazonApiTestModeActive() ? 1 : 0
  });
  console.info('[SIMILAR_ENRICH_LIMIT_APPLIED]', {
    limit,
    candidateCount: candidateList.length
  });
  console.info('[CREATOR_GETITEMS_LIMIT_APPLIED]', {
    limit,
    candidateCount: candidateList.length,
    asins
  });

  if (!asins.length) {
    return candidateList;
  }

  const enrichCandidateList = candidateList.filter((candidate) => asins.includes(cleanText(candidate?.asin).toUpperCase()));

  const markMissing = (candidate, reason = 'offer_data_missing') => ({
    ...candidate,
    offerEnriched: false,
    offerDataMissing: true,
    offerEnrichmentStatus: reason,
    offerApiStatus: reason === 'api_throttled' ? 'THROTTLED' : cleanText(candidate?.offerApiStatus || '').toUpperCase()
  });
  const mergeCandidateWithOfferData = (candidate = {}, enriched = {}, extra = {}) => ({
    ...candidate,
    ...enriched,
    title: enriched.title || candidate.title,
    brand: enriched.brand || candidate.brand,
    imageUrl: enriched.imageUrl || candidate.imageUrl,
    detailPageUrl: enriched.detailPageUrl || candidate.detailPageUrl,
    normalizedUrl: enriched.normalizedUrl || candidate.normalizedUrl,
    affiliateUrl: enriched.affiliateUrl || candidate.affiliateUrl,
    features: Array.isArray(enriched.features) && enriched.features.length ? enriched.features : candidate.features || [],
    priceValue: enriched.priceValue,
    extractedPrice: enriched.extractedPrice,
    priceDisplay: enriched.priceDisplay || candidate.priceDisplay || '',
    rawSearchItem: candidate.rawSearchItem || candidate.rawItem || null,
    similarSearchQuery: candidate.similarSearchQuery || '',
    similarSearchQueryType: candidate.similarSearchQueryType || '',
    sourceLabel: extra.sourceLabel || 'Amazon PAAPI GetItems Enrichment',
    dataSource: extra.dataSource || 'paapi_getitems_enrichment',
    offerEnriched: true,
    offerDataMissing: enriched.sellerDataMissing === true || !enriched.priceValid,
    offerApiStatus: '',
    ...extra
  });

  const cachedByAsin = new Map();
  const missingAsins = [];
  for (const asin of asins) {
    const cached = getCachedAmazonOfferData(asin);
    if (cached) {
      cachedByAsin.set(asin, cached);
      console.info('[CACHE_HIT]', {
        asin,
        cachedAt: cached.offerCachedAt || '',
        price: cached.priceValid ? cached.priceValue : null,
        sellerClass: cached.sellerClass || classifyMappedAmazonSellerForDebug(cached).sellerClass
      });
      console.info('[OFFER_CACHE_HIT]', {
        asin,
        cachedAt: cached.offerCachedAt || '',
        price: cached.priceValid ? cached.priceValue : null,
        merchantName: cached.merchantName || null,
        isAmazonFulfilled: cached.isAmazonFulfilled === true,
        isPrimeEligible: cached.isPrimeEligible === true,
        sellerClass: cached.sellerClass || classifyMappedAmazonSellerForDebug(cached).sellerClass
      });
    } else {
      missingAsins.push(asin);
      console.info('[CACHE_MISS]', {
        asin
      });
      console.info('[OFFER_CACHE_MISS]', {
        asin
      });
    }
  }

  const enrichedByAsin = new Map(cachedByAsin);
  const creatorConfig = getAmazonCreatorApiConfig();
  let paapiMissingAsins = [...missingAsins];

  if (missingAsins.length && creatorConfig.enabled === true && hasAmazonCreatorApiCredentials(creatorConfig)) {
    console.info('[CREATOR_ENRICH_START]', {
      asins: missingAsins,
      limit,
      resources: AMAZON_CREATOR_DEFAULT_RESOURCES
    });

    try {
      const responseJson = await requestAmazonCreatorApiGetItems(missingAsins, creatorConfig);
      const rawItems = extractAmazonCreatorItems(responseJson);
      const creatorMappedByAsin = new Map(
        rawItems
          .map((item) => {
            const mappedItem = mapAmazonCreatorItem(item, firstCleanCreatorText(item?.asin, item?.ASIN), creatorConfig);
            setCachedAmazonOfferData(mappedItem);
            return mappedItem;
          })
          .filter((item) => item.asin)
          .map((item) => [item.asin, item])
      );

      for (const asin of missingAsins) {
        const creatorItem = creatorMappedByAsin.get(asin);
        if (creatorItem) {
          enrichedByAsin.set(asin, creatorItem);
        }

        const sellerDebug = creatorItem ? classifyMappedAmazonSellerForDebug(creatorItem) : { sellerClass: 'UNKNOWN' };
        console.info('[CREATOR_ENRICH_RESULT]', {
          asin,
          price: creatorItem?.priceValid ? creatorItem.priceValue : null,
          priceDisplay: creatorItem?.priceDisplay || '',
          merchantName: creatorItem?.merchantName || null,
          isAmazonFulfilled: creatorItem?.isAmazonFulfilled === true,
          isPrimeEligible: creatorItem?.isPrimeEligible === true,
          sellerClass: sellerDebug.sellerClass,
          status: creatorItem ? 'loaded' : 'missing_item',
          rawSellerKeysFound: creatorItem?.rawSellerKeysFound || []
        });
      }

      paapiMissingAsins = missingAsins.filter((asin) => {
        const enriched = enrichedByAsin.get(asin);
        const hasSeller =
          Boolean(enriched?.merchantName) ||
          enriched?.isAmazonFulfilled === true ||
          enriched?.isPrimeEligible === true ||
          Array.isArray(enriched?.rawSellerKeysFound) && enriched.rawSellerKeysFound.length > 0;
        return !enriched || enriched.priceValid !== true || !hasSeller;
      });
    } catch (error) {
      const throttled = isAmazonApiThrottlingError(error);
      console.warn('[CREATOR_ENRICH_FAILED]', {
        asins: missingAsins,
        reason: error instanceof Error ? error.message : 'Creator GetItems Enrichment fehlgeschlagen.',
        apiStatus: throttled ? 'THROTTLED' : deriveErrorStatus(error)
      });
      if (throttled) {
        console.warn('[AMAZON_API_THROTTLED_ABORTED]', {
          operation: 'CreatorGetItems',
          asin: missingAsins.join(','),
          retry: 1,
          maxRetries: 1,
          message: error instanceof Error ? error.message : 'Creator GetItems Enrichment throttled.'
        });
        return enrichCandidateList.map((candidate) => {
          const asin = cleanText(candidate?.asin).toUpperCase();
          const enriched = enrichedByAsin.get(asin);
          if (enriched) {
            return mergeCandidateWithOfferData(candidate, enriched, {
              offerCacheHit: cachedByAsin.has(asin),
              dataSource: cachedByAsin.has(asin) ? 'offer_cache' : enriched.dataSource || 'creator_api',
              sourceLabel: cachedByAsin.has(asin) ? 'Amazon Offer Cache' : enriched.sourceLabel || 'Amazon Creator API'
            });
          }
          return markMissing(candidate, 'api_throttled');
        });
      }
      paapiMissingAsins = [...missingAsins];
    }
  } else if (missingAsins.length) {
    console.info('[CREATOR_ENRICH_SKIPPED]', {
      asins: missingAsins,
      enabled: creatorConfig.enabled === true,
      configured: hasAmazonCreatorApiCredentials(creatorConfig),
      reason: creatorConfig.enabled === true ? 'missing_config' : 'disabled'
    });
  }

  if (!paapiMissingAsins.length) {
    return enrichCandidateList.map((candidate) => {
      const asin = cleanText(candidate?.asin).toUpperCase();
      const enriched = enrichedByAsin.get(asin);
      return enriched
        ? mergeCandidateWithOfferData(candidate, enriched, {
            offerCacheHit: cachedByAsin.has(asin),
            dataSource: cachedByAsin.has(asin) ? 'offer_cache' : enriched.dataSource || 'creator_api',
            sourceLabel: cachedByAsin.has(asin) ? 'Amazon Offer Cache' : enriched.sourceLabel || 'Amazon Creator API'
          })
        : markMissing(candidate, 'missing_item');
    });
  }

  console.info('[PAAPI_GETITEMS_ENRICH_START]', {
    asins: paapiMissingAsins,
    resources: AMAZON_DEFAULT_RESOURCES
  });

  try {
    const requestGetItems = async (resources = AMAZON_DEFAULT_RESOURCES) => {
      return requestAmazonProductAdvertisingApi(
        {
          ItemIds: paapiMissingAsins,
          ItemIdType: 'ASIN',
          Marketplace: config.marketplace || AMAZON_PAAPI_MARKETPLACE_DE,
          LanguagesOfPreference: [config.language],
          Condition: AMAZON_PAAPI_CONDITION,
          Merchant: AMAZON_PAAPI_MERCHANT,
          OfferCount: AMAZON_PAAPI_OFFER_COUNT,
          PartnerTag: config.partnerTag,
          PartnerType: 'Associates',
          Resources: resources
        },
        {
          operation: 'GetItems',
          asin: paapiMissingAsins.join(',')
        }
      );
    };

    let responseJson;
    try {
      responseJson = await requestGetItems(AMAZON_DEFAULT_RESOURCES);
    } catch (error) {
      if (!isAmazonResourceValidationError(error)) {
        throw error;
      }
      console.warn('[PAAPI_GETITEMS_ENRICH_RESOURCES_FALLBACK]', {
        asins: paapiMissingAsins,
        reason: error instanceof Error ? error.message : 'GetItems Resource-Fallback',
        resources: AMAZON_OFFER_FALLBACK_RESOURCES
      });
      responseJson = await requestGetItems(AMAZON_OFFER_FALLBACK_RESOURCES);
    }

    const rawItems = Array.isArray(responseJson?.ItemsResult?.Items) ? responseJson.ItemsResult.Items : [];
    const mappedByAsin = new Map(
      rawItems
        .map((item) => {
          const mappedItem = mapAmazonItem(item, cleanText(item?.ASIN).toUpperCase());
          setCachedAmazonOfferData(mappedItem);
          enrichedByAsin.set(mappedItem.asin, mappedItem);
          return mappedItem;
        })
        .filter((item) => item.asin)
        .map((item) => [item.asin, item])
    );

    return enrichCandidateList.map((candidate) => {
      const asin = cleanText(candidate?.asin).toUpperCase();
      const enriched = enrichedByAsin.get(asin) || mappedByAsin.get(asin);

      if (!enriched) {
        console.warn('[PAAPI_GETITEMS_ENRICH_RESULT]', {
          asin,
          price: null,
          merchantName: null,
          isAmazonFulfilled: null,
          isPrimeEligible: null,
          sellerClass: 'UNKNOWN',
          status: 'missing_item'
        });
        return markMissing(candidate, 'missing_item');
      }

      const sellerDebug = classifyMappedAmazonSellerForDebug(enriched);
      const merged = mergeCandidateWithOfferData(candidate, enriched, {
        offerCacheHit: cachedByAsin.has(asin),
        sourceLabel: cachedByAsin.has(asin) ? 'Amazon Offer Cache' : enriched.sourceLabel || 'Amazon PAAPI GetItems Enrichment',
        dataSource: cachedByAsin.has(asin) ? 'offer_cache' : enriched.dataSource || 'paapi_getitems_enrichment'
      });

      console.info('[PAAPI_GETITEMS_ENRICH_RESULT]', {
        asin,
        price: enriched.priceValid ? enriched.priceValue : null,
        merchantName: enriched.merchantName || null,
        isAmazonFulfilled: enriched.isAmazonFulfilled === true,
        isPrimeEligible: enriched.isPrimeEligible === true,
        sellerClass: sellerDebug.sellerClass,
        source: sellerDebug.source,
        reason: sellerDebug.reason,
        rawSellerKeysFound: enriched.rawSellerKeysFound,
        priceValid: enriched.priceValid === true,
        rawPriceObject: enriched.rawPriceObject
      });

      return merged;
    });
  } catch (error) {
    const throttled = isAmazonApiThrottlingError(error);
    console.warn('[PAAPI_GETITEMS_ENRICH_FAILED]', {
      asins: paapiMissingAsins,
      requestedAt,
      reason: error instanceof Error ? error.message : 'GetItems Enrichment fehlgeschlagen.',
      apiStatus: throttled ? 'THROTTLED' : deriveErrorStatus(error)
    });
    return enrichCandidateList.map((candidate) => {
      const asin = cleanText(candidate?.asin).toUpperCase();
      const enriched = enrichedByAsin.get(asin);
      if (enriched) {
        return mergeCandidateWithOfferData(candidate, enriched, {
          offerCacheHit: cachedByAsin.has(asin),
          dataSource: cachedByAsin.has(asin) ? 'offer_cache' : enriched.dataSource || 'creator_api',
          sourceLabel: cachedByAsin.has(asin) ? 'Amazon Offer Cache' : enriched.sourceLabel || 'Amazon Creator API'
        });
      }
      return paapiMissingAsins.includes(asin) ? markMissing(candidate, throttled ? 'api_throttled' : 'api_error') : markMissing(candidate, 'missing_item');
    });
  }
}

export async function loadAmazonAffiliateContext(input = {}) {
  const asin = cleanText(input.asin).toUpperCase();
  const requestedAt = nowIso();
  const config = getAmazonAffiliateConfig();
  const creatorConfig = getAmazonCreatorApiConfig();
  ensureAmazonConfigLog(config);

  if (!asin) {
    return {
      available: false,
      status: 'missing_asin',
      requestedAt,
      reason: 'ASIN fehlt fuer die Amazon-Affiliate-Pruefung.'
    };
  }

  let creatorFallbackContext = null;

  if (creatorConfig.enabled) {
    if (hasAmazonCreatorApiCredentials(creatorConfig)) {
      try {
        console.info('[CREATOR_API_PREFERRED]', {
          asin,
          endpoint: buildAmazonCreatorEndpoint(creatorConfig),
          marketplace: creatorConfig.marketplace,
          credentialVersion: creatorConfig.credentialVersion
        });
        const response = await requestAmazonCreatorApiGetItems(asin, creatorConfig);
        const items = extractAmazonCreatorItems(response);
        const item =
          items.find((entry) => firstCleanCreatorText(entry?.asin, entry?.ASIN).toUpperCase() === asin) || items[0] || null;

        if (item) {
          const mappedItem = mapAmazonCreatorItem(item, asin, creatorConfig);
          const paapiFallbackConfigured = config.enabled === true && hasAmazonAffiliateCredentials(config);
          const creatorHasCompleteProductData = Boolean(mappedItem.title && mappedItem.imageUrl && mappedItem.priceDisplay);

          if (!creatorHasCompleteProductData && paapiFallbackConfigured) {
            creatorFallbackContext = {
              status: 'partial_data',
              reason: 'Amazon Creator API hat nur unvollstaendige Produktdaten geliefert.',
              errorCode: 'AMAZON_CREATOR_API_PARTIAL_DATA',
              httpStatus: 206
            };
            console.warn('[CREATOR_API_FALLBACK_PAAPI]', {
              asin,
              reason: creatorFallbackContext.reason,
              titleLoaded: Boolean(mappedItem.title),
              imageLoaded: Boolean(mappedItem.imageUrl),
              priceLoaded: Boolean(mappedItem.priceDisplay)
            });
          } else {
            return {
              available: true,
              status: 'loaded',
              requestedAt,
              result: {
                ...mappedItem,
                host: buildAmazonCreatorEndpoint(creatorConfig),
                region: 'creator_api',
                fallbackAvailable: paapiFallbackConfigured
              }
            };
          }
        } else {
          creatorFallbackContext = {
            status: 'not_found',
            reason: 'Amazon Creator API hat keine Produktdaten fuer diese ASIN geliefert.',
            errorCode: 'AMAZON_CREATOR_API_NO_ITEMS',
            httpStatus: 404
          };
          console.warn('[CREATOR_API_FALLBACK_PAAPI]', {
            asin,
            reason: creatorFallbackContext.reason,
            status: creatorFallbackContext.status
          });
        }
      } catch (error) {
        creatorFallbackContext = {
          status: deriveErrorStatus(error),
          reason: error instanceof Error ? error.message : 'Amazon Creator API Request fehlgeschlagen.',
          errorCode: error instanceof Error ? error.code || 'AMAZON_CREATOR_API_ERROR' : 'AMAZON_CREATOR_API_ERROR',
          httpStatus: error instanceof Error ? error.statusCode || 502 : 502
        };
        console.warn('[CREATOR_API_FALLBACK_PAAPI]', {
          asin,
          status: creatorFallbackContext.status,
          errorCode: creatorFallbackContext.errorCode,
          reason: creatorFallbackContext.reason
        });
      }
    } else {
      console.info('[CREATOR_API_SKIP]', {
        asin,
        reason: 'missing_config',
        enabled: creatorConfig.enabled === true,
        clientIdLoaded: Boolean(creatorConfig.clientId),
        clientSecretLoaded: Boolean(creatorConfig.clientSecret),
        partnerTagLoaded: Boolean(creatorConfig.partnerTag)
      });
    }
  }

  if (!config.enabled) {
    if (creatorFallbackContext) {
      return {
        available: false,
        requestedAt,
        ...creatorFallbackContext
      };
    }

    return {
      available: false,
      status: 'disabled',
      requestedAt,
      reason: 'Amazon Product Advertising API ist deaktiviert.'
    };
  }

  if (!hasAmazonAffiliateCredentials(config)) {
    if (creatorFallbackContext) {
      return {
        available: false,
        requestedAt,
        ...creatorFallbackContext
      };
    }

    return {
      available: false,
      status: 'missing_config',
      requestedAt,
      reason: 'Amazon Product Advertising API ist nicht vollstaendig konfiguriert.'
    };
  }

  try {
    const payload = {
      ItemIds: [asin],
      ItemIdType: 'ASIN',
      Marketplace: AMAZON_PAAPI_MARKETPLACE_DE,
      LanguagesOfPreference: [config.language],
      Condition: AMAZON_PAAPI_CONDITION,
      Merchant: AMAZON_PAAPI_MERCHANT,
      OfferCount: AMAZON_PAAPI_OFFER_COUNT,
      PartnerTag: config.partnerTag,
      PartnerType: 'Associates',
      Resources: AMAZON_DEFAULT_RESOURCES
    };
    console.info('[PAAPI_REQUEST_RESOURCES]', {
      asin,
      marketplace: payload.Marketplace,
      condition: payload.Condition,
      merchant: payload.Merchant,
      offerCount: payload.OfferCount,
      resources: payload.Resources
    });
    let response;
    try {
      response = await requestAmazonProductAdvertisingApi(payload, { asin });
    } catch (error) {
      if (!isAmazonResourceValidationError(error)) {
        throw error;
      }
      const fallbackPayload = {
        ...payload,
        Resources: AMAZON_OFFER_FALLBACK_RESOURCES
      };
      console.warn('[PAAPI_GETITEMS_RESOURCES_FALLBACK]', {
        asin,
        reason: error instanceof Error ? error.message : 'GetItems Resource-Fallback',
        resources: fallbackPayload.Resources
      });
      response = await requestAmazonProductAdvertisingApi(fallbackPayload, { asin });
    }
    const items = Array.isArray(response?.ItemsResult?.Items) ? response.ItemsResult.Items : [];
    const item = items.find((entry) => cleanText(entry?.ASIN).toUpperCase() === asin) || items[0] || null;

    if (!item) {
      const notFoundError = createAmazonAffiliateError(
        'Amazon API hat keine Produktdaten fuer diese ASIN geliefert.',
        'AMAZON_API_NO_ITEMS',
        404
      );
      logAmazonApiEvent('warning', 'amazon.request.no_items', 'GetItems', notFoundError.message, {
        asin,
        status: 'no_hits',
        requestMeta: {
          marketplace: config.marketplace
        }
      });

      return {
        available: false,
        status: 'not_found',
        requestedAt,
        reason: notFoundError.message
      };
    }

    const offersCount = Array.isArray(item?.Offers?.Listings)
      ? item.Offers.Listings.length
      : Array.isArray(item?.OffersV2?.Listings)
        ? item.OffersV2.Listings.length
        : 0;
    if (offersCount > 0) {
      console.info('[PAAPI_RESPONSE_HAS_OFFERS]', {
        asin,
        offersCount
      });
    } else {
      console.warn('[PAAPI_RESPONSE_NO_OFFERS]', {
        asin,
        offersCount,
        hasOffersObject: Boolean(item?.Offers || item?.OffersV2)
      });
    }

    const mappedItem = mapAmazonItem(item, asin);
    return {
      available: true,
      status: 'loaded',
      requestedAt,
      result: {
        ...mappedItem,
        sourceLabel: 'Amazon Product Advertising API',
        marketplace: config.marketplace,
        host: config.host,
        region: config.region
      }
    };
  } catch (error) {
    return {
      available: false,
      status: deriveErrorStatus(error),
      requestedAt,
      reason: error instanceof Error ? error.message : 'Amazon API Request fehlgeschlagen.',
      errorCode: error instanceof Error ? error.code || 'AMAZON_API_ERROR' : 'AMAZON_API_ERROR',
      httpStatus: error instanceof Error ? error.statusCode || 502 : 502,
      requestId: error instanceof Error ? error.details?.requestId || null : null
    };
  }
}

export async function runAmazonAffiliateApiTest(input = {}) {
  const asin = cleanText(input.asin || AMAZON_TEST_ASIN).toUpperCase() || AMAZON_TEST_ASIN;
  const config = getAmazonAffiliateConfig();
  ensureAmazonConfigLog(config);
  const diagnosticMeta = {
    asin,
    AccessKeyPrefix: getAccessKeyPrefix(config.accessKey),
    PartnerTag: config.partnerTag,
    Host: config.host,
    Region: config.region,
    Marketplace: config.marketplace,
    Endpoint: buildAmazonPaapiEndpoint(config),
    Timestamp: nowIso()
  };

  console.info('[PAAPI_TEST_START]', diagnosticMeta);
  logGeneratorDebug('PAAPI TEST START', {
    ...diagnosticMeta,
    accessKeyMasked: maskSecret(config.accessKey, 4, 3),
    region: config.region,
    endpoint: buildAmazonPaapiEndpoint(config)
  });

  if (!config.enabled) {
    throw createAmazonAffiliateError('Amazon Product Advertising API ist im Backend deaktiviert.', 'AMAZON_API_DISABLED', 400);
  }

  if (!hasAmazonAffiliateCredentials(config)) {
    throw createAmazonAffiliateError(
      'Amazon Product Advertising API ist nicht vollstaendig im Backend konfiguriert.',
      'AMAZON_API_NOT_CONFIGURED',
      400
    );
  }

  const context = await loadAmazonAffiliateContext({ asin });
  if (!context.available || !context.result) {
    const error = createAmazonAffiliateError(
      context.reason || 'Amazon API Test fehlgeschlagen.',
      context.errorCode || 'AMAZON_API_TEST_FAILED',
      context.httpStatus || 502,
      {
        requestId: context.requestId || null
      }
    );
    console.error('[PAAPI_TEST_ERROR]', {
      ...diagnosticMeta,
      errorMessage: error.message,
      errorCode: error.code || 'AMAZON_API_TEST_FAILED',
      httpStatus: error.statusCode || 502,
      requestId: error.details?.requestId || null
    });
    throw error;
  }

  console.info('[PAAPI_TEST_SUCCESS]', {
    ...diagnosticMeta,
    title: context.result.title || '',
    detailPageUrl: context.result.detailPageUrl || ''
  });

  return {
    success: true,
    requestedAt: context.requestedAt,
    asin,
    config: {
      host: config.host,
      region: config.region,
      marketplace: config.marketplace,
      language: config.language,
      partnerTagMasked: maskSecret(config.partnerTag, 4, 3),
      deprecationDate: AMAZON_DEPRECATION_DATE
    },
    item: {
      asin: context.result.asin,
      title: context.result.title,
      brand: context.result.brand,
      categoryName: context.result.categoryName,
      imageUrl: context.result.imageUrl,
      detailPageUrl: context.result.detailPageUrl,
      affiliateUrl: context.result.affiliateUrl,
      priceDisplay: context.result.priceDisplay,
      availability: context.result.availability
    }
  };
}

export async function testCreatorApi(input = {}) {
  const asin = cleanText(input.asin || AMAZON_TEST_ASIN).toUpperCase() || AMAZON_TEST_ASIN;
  const requestedAt = nowIso();
  const creatorConfig = getAmazonCreatorApiConfig();
  const paapiConfig = getAmazonAffiliateConfig();
  ensureAmazonConfigLog(paapiConfig);
  const configured = hasAmazonCreatorApiCredentials(creatorConfig);
  const diagnosticMeta = {
    asin,
    requestedAt,
    enabled: creatorConfig.enabled === true,
    configured,
    endpoint: buildAmazonCreatorEndpoint(creatorConfig),
    authEndpoint: creatorConfig.authEndpoint || '',
    marketplace: creatorConfig.marketplace || '',
    credentialVersion: creatorConfig.credentialVersion || '',
    clientIdMasked: maskSecret(creatorConfig.clientId, 4, 4),
    partnerTagMasked: maskSecret(creatorConfig.partnerTag, 4, 3)
  };

  console.info('[CREATOR_API_TEST_START]', diagnosticMeta);

  if (!creatorConfig.enabled) {
    console.warn('[CREATOR_API_TEST_FAILED]', {
      ...diagnosticMeta,
      reason: 'creator_api_disabled'
    });
    return {
      success: false,
      creatorUsed: false,
      fallbackUsed: false,
      status: 'disabled',
      reason: 'AMAZON_CREATOR_API_ENABLED ist nicht aktiv.',
      requestedAt,
      config: diagnosticMeta
    };
  }

  if (!configured) {
    console.warn('[CREATOR_API_TEST_FAILED]', {
      ...diagnosticMeta,
      reason: 'creator_api_missing_config',
      clientIdConfigured: Boolean(creatorConfig.clientId),
      clientSecretConfigured: Boolean(creatorConfig.clientSecret),
      partnerTagConfigured: Boolean(creatorConfig.partnerTag)
    });
    return {
      success: false,
      creatorUsed: false,
      fallbackUsed: false,
      status: 'missing_config',
      reason: 'Amazon Creator API ist nicht vollstaendig konfiguriert.',
      requestedAt,
      config: {
        ...diagnosticMeta,
        clientIdConfigured: Boolean(creatorConfig.clientId),
        clientSecretConfigured: Boolean(creatorConfig.clientSecret),
        partnerTagConfigured: Boolean(creatorConfig.partnerTag)
      }
    };
  }

  try {
    const token = await requestAmazonCreatorAccessToken(creatorConfig, { asin });
    console.info('[CREATOR_API_TEST_TOKEN_OK]', {
      ...diagnosticMeta,
      tokenReceived: Boolean(token),
      tokenPreview: token ? `${token.slice(0, 8)}...` : ''
    });

    const response = await requestAmazonCreatorApiGetItems(asin, creatorConfig);
    const rawItems = extractAmazonCreatorItems(response);
    const rawItem =
      rawItems.find((entry) => firstCleanCreatorText(entry?.asin, entry?.ASIN).toUpperCase() === asin) || rawItems[0] || null;
    const mappedItem = rawItem ? mapAmazonCreatorItem(rawItem, asin, creatorConfig) : null;
    const complete = Boolean(mappedItem?.title && mappedItem?.imageUrl && mappedItem?.priceDisplay);

    console.info('[CREATOR_API_TEST_GETITEMS_OK]', {
      ...diagnosticMeta,
      itemCount: rawItems.length,
      rawKeys: rawItem && typeof rawItem === 'object' ? Object.keys(rawItem) : [],
      titleLoaded: Boolean(mappedItem?.title),
      imageLoaded: Boolean(mappedItem?.imageUrl),
      priceLoaded: Boolean(mappedItem?.priceDisplay),
      complete
    });
    console.info('[CREATOR_API_TEST_RESULT]', {
      asin,
      creatorUsed: true,
      fallbackUsed: false,
      status: rawItem ? 'loaded' : 'no_items',
      complete
    });

    return {
      success: Boolean(rawItem),
      creatorUsed: true,
      fallbackUsed: false,
      status: rawItem ? 'loaded' : 'no_items',
      requestedAt,
      config: diagnosticMeta,
      token: {
        received: Boolean(token),
        preview: token ? `${token.slice(0, 8)}...` : '',
        expiresAt: amazonCreatorTokenCache.expiresAt ? new Date(amazonCreatorTokenCache.expiresAt).toISOString() : ''
      },
      response: {
        itemCount: rawItems.length,
        rawKeys: rawItem && typeof rawItem === 'object' ? Object.keys(rawItem) : []
      },
      item: mappedItem
        ? {
            asin: mappedItem.asin,
            title: mappedItem.title,
            brand: mappedItem.brand,
            imageUrl: mappedItem.imageUrl,
            detailPageUrl: mappedItem.detailPageUrl,
            affiliateUrl: mappedItem.affiliateUrl,
            priceDisplay: mappedItem.priceDisplay,
            availability: mappedItem.availability,
            merchantName: mappedItem.merchantName || '',
            isAmazonFulfilled: mappedItem.isAmazonFulfilled === true,
            isPrimeEligible: mappedItem.isPrimeEligible === true,
            sourceLabel: mappedItem.sourceLabel,
            dataSource: mappedItem.dataSource
          }
        : null,
      completeness: {
        titleLoaded: Boolean(mappedItem?.title),
        imageLoaded: Boolean(mappedItem?.imageUrl),
        priceLoaded: Boolean(mappedItem?.priceDisplay),
        complete
      }
    };
  } catch (error) {
    console.error('[CREATOR_API_TEST_FAILED]', {
      ...diagnosticMeta,
      status: deriveErrorStatus(error),
      errorCode: error instanceof Error ? error.code || 'CREATOR_API_TEST_FAILED' : 'CREATOR_API_TEST_FAILED',
      errorMessage: error instanceof Error ? error.message : 'Creator API Test fehlgeschlagen.',
      httpStatus: error instanceof Error ? error.statusCode || error.details?.httpStatus || null : null
    });

    return {
      success: false,
      creatorUsed: true,
      fallbackUsed: false,
      status: deriveErrorStatus(error),
      reason: error instanceof Error ? error.message : 'Creator API Test fehlgeschlagen.',
      errorCode: error instanceof Error ? error.code || 'CREATOR_API_TEST_FAILED' : 'CREATOR_API_TEST_FAILED',
      httpStatus: error instanceof Error ? error.statusCode || error.details?.httpStatus || null : null,
      requestedAt,
      config: diagnosticMeta
    };
  }
}

export function getAmazonAffiliateStatus() {
  const config = getAmazonAffiliateConfig();
  const creatorConfig = getAmazonCreatorApiConfig();
  ensureAmazonConfigLog(config);
  const configured = hasAmazonAffiliateCredentials(config);
  const creatorConfigured = hasAmazonCreatorApiCredentials(creatorConfig);
  const lastSuccess = db
    .prepare(
      `
        SELECT *
        FROM amazon_api_logs
        WHERE status = 'success'
        ORDER BY created_at DESC
        LIMIT 1
      `
    )
    .get();
  const lastError = db
    .prepare(
      `
        SELECT *
        FROM amazon_api_logs
        WHERE status IN ('auth_error', 'api_error', 'no_hits', 'not_configured')
        ORDER BY created_at DESC
        LIMIT 1
      `
    )
    .get();
  const lastAuthError = db
    .prepare(
      `
        SELECT *
        FROM amazon_api_logs
        WHERE status = 'auth_error'
        ORDER BY created_at DESC
        LIMIT 1
      `
    )
    .get();
  const last24Hours = db
    .prepare(
      `
        SELECT
          COUNT(*) AS requestCount,
          SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successCount,
          SUM(CASE WHEN status = 'auth_error' THEN 1 ELSE 0 END) AS authErrorCount,
          SUM(CASE WHEN status = 'api_error' THEN 1 ELSE 0 END) AS apiErrorCount,
          SUM(CASE WHEN status = 'no_hits' THEN 1 ELSE 0 END) AS noHitsCount
        FROM amazon_api_logs
        WHERE created_at >= ?
      `
    )
    .get(new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString());

  const apiStatus = !config.enabled
    ? 'deaktiviert'
    : !configured
      ? 'nicht_konfiguriert'
      : lastSuccess
        ? 'verbunden'
        : lastAuthError
          ? 'auth_fehler'
          : lastError
            ? 'fehler'
            : 'vorbereitet';

  const overview = {
    apiStatus,
    connected: apiStatus === 'verbunden',
    lastSuccessfulFetch: lastSuccess?.created_at || null,
    lastErrorAt: lastError?.created_at || null,
    lastErrorMessage: lastError?.message || '',
    lastAuthErrorAt: lastAuthError?.created_at || null,
    requestCount24h: Number(last24Hours?.requestCount || 0),
    successCount24h: Number(last24Hours?.successCount || 0),
    authErrorCount24h: Number(last24Hours?.authErrorCount || 0),
    apiErrorCount24h: Number(last24Hours?.apiErrorCount || 0),
    noHitsCount24h: Number(last24Hours?.noHitsCount || 0)
  };

  logGeneratorDebug('FLOW STATUS UPDATED', {
    source: 'amazon_api',
    apiStatus: overview.apiStatus,
    connected: overview.connected,
    lastSuccessfulFetch: overview.lastSuccessfulFetch,
    lastErrorAt: overview.lastErrorAt
  });

  return {
    settings: {
      enabled: config.enabled,
      configured,
      host: config.host,
      region: config.region,
      marketplace: config.marketplace,
      language: config.language,
      partnerTagMasked: maskSecret(config.partnerTag, 4, 3),
      accessKeyConfigured: Boolean(config.accessKey),
      secretKeyConfigured: Boolean(config.secretKey),
      creatorApi: {
        enabled: creatorConfig.enabled,
        configured: creatorConfigured,
        credentialVersion: creatorConfig.credentialVersion,
        endpoint: creatorConfig.endpoint,
        authEndpoint: creatorConfig.authEndpoint,
        marketplace: creatorConfig.marketplace,
        partnerTagMasked: maskSecret(creatorConfig.partnerTag, 4, 3),
        clientIdConfigured: Boolean(creatorConfig.clientId),
        clientSecretConfigured: Boolean(creatorConfig.clientSecret),
        preferredWhenConfigured: true
      }
    },
    connection: {
      connected: overview.connected,
      configured,
      apiStatus: overview.apiStatus,
      checkedAt: lastSuccess?.created_at || lastError?.created_at || null,
      lastSuccessfulFetch: overview.lastSuccessfulFetch,
      lastErrorAt: overview.lastErrorAt,
      lastErrorMessage: overview.lastErrorMessage
    },
    overview,
    deprecation: {
      active: true,
      date: AMAZON_DEPRECATION_DATE,
      message: 'Amazon Product Advertising API 5.0 ist laut offizieller Doku bis 30. April 2026 veraltet/deprecated.'
    },
    latest: {
      success: lastSuccess
        ? {
            createdAt: lastSuccess.created_at,
            operation: lastSuccess.operation,
            asin: lastSuccess.asin,
            responseMeta: parseJson(lastSuccess.response_meta_json, null)
          }
        : null,
      error: lastError
        ? {
            createdAt: lastError.created_at,
            operation: lastError.operation,
            asin: lastError.asin,
            status: lastError.status,
            message: lastError.message,
            responseMeta: parseJson(lastError.response_meta_json, null)
          }
        : null
    }
  };
}
