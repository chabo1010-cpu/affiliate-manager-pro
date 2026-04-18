import crypto from 'crypto';
import { getDb } from '../db.js';
import { getAmazonAffiliateConfig } from '../env.js';
import { buildAmazonAffiliateLinkRecord, cleanText } from './dealHistoryService.js';
import { logGeneratorDebug } from './generatorFlowService.js';

const db = getDb();
const AMAZON_DEPRECATION_DATE = '2026-04-30';
const AMAZON_TEST_ASIN = 'B0DDKZBYK6';
const AMAZON_SERVICE_NAME = 'ProductAdvertisingAPIv1';
const AMAZON_API_PATH = '/paapi5/getitems';
const AMAZON_TARGET = 'com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems';
const AMAZON_DEFAULT_RESOURCES = [
  'Images.Primary.Large',
  'Images.Primary.Medium',
  'ItemInfo.ByLineInfo',
  'ItemInfo.Classifications',
  'ItemInfo.Features',
  'ItemInfo.ProductInfo',
  'ItemInfo.Title',
  'Offers.Listings.Availability.Message',
  'Offers.Listings.Price'
];

let amazonConfigLogWritten = false;

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

function hasAmazonAffiliateCredentials(config = {}) {
  return Boolean(config.enabled && config.accessKey && config.secretKey && config.partnerTag);
}

function ensureAmazonConfigLog(config = {}) {
  if (amazonConfigLogWritten) {
    return;
  }

  amazonConfigLogWritten = true;
  logGeneratorDebug('AMAZON API CONFIG LOADED', {
    enabled: config.enabled === true,
    configured: hasAmazonAffiliateCredentials(config),
    host: config.host || '',
    region: config.region || '',
    marketplace: config.marketplace || '',
    partnerTagConfigured: Boolean(config.partnerTag),
    deprecationDate: AMAZON_DEPRECATION_DATE
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

function buildCanonicalRequest(payload, host, amzDate) {
  const canonicalHeaders = {
    'content-encoding': 'amz-1.0',
    'content-type': 'application/json; charset=utf-8',
    host,
    'x-amz-date': amzDate,
    'x-amz-target': AMAZON_TARGET
  };
  const signedHeaders = Object.keys(canonicalHeaders).sort().join(';');
  const canonicalHeaderString = Object.keys(canonicalHeaders)
    .sort()
    .map((key) => `${key}:${canonicalHeaders[key]}`)
    .join('\n');

  return {
    canonicalRequest: [
      'POST',
      AMAZON_API_PATH,
      '',
      `${canonicalHeaderString}\n`,
      signedHeaders,
      hashSha256Hex(payload)
    ].join('\n'),
    signedHeaders,
    headers: canonicalHeaders
  };
}

function buildAuthorizationHeader(config, payload, date = new Date()) {
  const amzDate = buildAmzDate(date);
  const dateStamp = buildDateStamp(date);
  const { canonicalRequest, signedHeaders, headers } = buildCanonicalRequest(payload, config.host, amzDate);
  const credentialScope = `${dateStamp}/${config.region}/${AMAZON_SERVICE_NAME}/aws4_request`;
  const stringToSign = ['AWS4-HMAC-SHA256', amzDate, credentialScope, hashSha256Hex(canonicalRequest)].join('\n');
  const signingKey = buildSigningKey(config.secretKey, dateStamp, config.region, AMAZON_SERVICE_NAME);
  const signature = hmacSha256(signingKey, stringToSign, 'hex');

  return {
    headers: {
      ...headers,
      Authorization: `AWS4-HMAC-SHA256 Credential=${config.accessKey}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`
    },
    amzDate
  };
}

function deriveErrorStatus(error) {
  const code = cleanText(error?.code || '').toUpperCase();
  const statusCode = Number(error?.statusCode || 0);

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

function mapAmazonItem(item = {}, fallbackAsin = '') {
  const asin = cleanText(item?.ASIN || fallbackAsin).toUpperCase();
  const detailPageUrl = cleanText(item?.DetailPageURL);
  const linkRecord = buildAmazonAffiliateLinkRecord(detailPageUrl || asin, {
    asin
  });
  const listing = item?.Offers?.Listings?.[0] || item?.OffersV2?.Listings?.[0] || null;
  const image =
    item?.Images?.Primary?.Large?.URL ||
    item?.Images?.Primary?.Medium?.URL ||
    item?.Images?.Primary?.Small?.URL ||
    '';
  const features = Array.isArray(item?.ItemInfo?.Features?.DisplayValues)
    ? item.ItemInfo.Features.DisplayValues.filter((value) => cleanText(value))
    : [];
  const priceDisplay =
    cleanText(listing?.Price?.DisplayAmount) ||
    cleanText(item?.OffersV2?.Summaries?.[0]?.LowestPrice?.DisplayAmount) ||
    '';
  const availability =
    cleanText(listing?.Availability?.Message) ||
    cleanText(item?.OffersV2?.Summaries?.[0]?.Condition?.DisplayValue) ||
    '';

  return {
    asin,
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
    availability,
    rawItem: item
  };
}

async function requestAmazonProductAdvertisingApi(payload, context = {}) {
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
  const { headers, amzDate } = buildAuthorizationHeader(config, requestPayload);
  const requestMeta = {
    operation: 'GetItems',
    asin: cleanText(context.asin).toUpperCase() || null,
    host: config.host,
    region: config.region,
    marketplace: config.marketplace,
    partnerTagMasked: maskSecret(config.partnerTag, 4, 3),
    requestedAt: amzDate
  };

  logGeneratorDebug('AMAZON API REQUEST START', requestMeta);
  logAmazonApiEvent('info', 'amazon.request.start', 'GetItems', 'Amazon Product Advertising API Request gestartet.', {
    asin: context.asin,
    status: 'pending',
    requestMeta
  });

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(1000, Number(config.timeoutMs || 12000)));

  try {
    const response = await fetch(`https://${config.host}${AMAZON_API_PATH}`, {
      method: 'POST',
      headers,
      body: requestPayload,
      signal: controller.signal
    });
    const responseText = await response.text();
    const responseJson = parseJson(responseText, null);
    const apiErrors = Array.isArray(responseJson?.Errors) ? responseJson.Errors : [];

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
        response: responseJson
      });

      logGeneratorDebug('AMAZON API ERROR', {
        ...requestMeta,
        statusCode,
        errorCode,
        message: errorMessage
      });
      logAmazonApiEvent('error', 'amazon.request.error', 'GetItems', errorMessage, {
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
      itemCount: Array.isArray(responseJson?.ItemsResult?.Items) ? responseJson.ItemsResult.Items.length : 0
    };
    logGeneratorDebug('AMAZON API RESPONSE RECEIVED', {
      ...requestMeta,
      ...responseMeta
    });
    logAmazonApiEvent(
      'info',
      'amazon.request.success',
      'GetItems',
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
      logAmazonApiEvent('error', 'amazon.request.timeout', 'GetItems', timeoutError.message, {
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
    logAmazonApiEvent('error', 'amazon.request.network_error', 'GetItems', networkError.message, {
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

export async function loadAmazonAffiliateContext(input = {}) {
  const asin = cleanText(input.asin).toUpperCase();
  const requestedAt = nowIso();
  const config = getAmazonAffiliateConfig();
  ensureAmazonConfigLog(config);

  if (!asin) {
    return {
      available: false,
      status: 'missing_asin',
      requestedAt,
      reason: 'ASIN fehlt fuer die Amazon-Affiliate-Pruefung.'
    };
  }

  if (!config.enabled) {
    return {
      available: false,
      status: 'disabled',
      requestedAt,
      reason: 'Amazon Product Advertising API ist deaktiviert.'
    };
  }

  if (!hasAmazonAffiliateCredentials(config)) {
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
      Marketplace: config.marketplace,
      LanguagesOfPreference: [config.language],
      PartnerTag: config.partnerTag,
      PartnerType: 'Associates',
      Resources: AMAZON_DEFAULT_RESOURCES
    };
    const response = await requestAmazonProductAdvertisingApi(payload, { asin });
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
      reason: error instanceof Error ? error.message : 'Amazon API Request fehlgeschlagen.'
    };
  }
}

export async function runAmazonAffiliateApiTest(input = {}) {
  const asin = cleanText(input.asin || AMAZON_TEST_ASIN).toUpperCase() || AMAZON_TEST_ASIN;
  const config = getAmazonAffiliateConfig();
  ensureAmazonConfigLog(config);

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
    throw createAmazonAffiliateError(context.reason || 'Amazon API Test fehlgeschlagen.', 'AMAZON_API_TEST_FAILED', 502);
  }

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

export function getAmazonAffiliateStatus() {
  const config = getAmazonAffiliateConfig();
  ensureAmazonConfigLog(config);
  const configured = hasAmazonAffiliateCredentials(config);
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
      secretKeyConfigured: Boolean(config.secretKey)
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
