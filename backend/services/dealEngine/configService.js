import { getDb } from '../../db.js';
import { clamp, nowIso, parseBool, parseInteger, parseNumber, round } from './shared.js';
import { normalizeUnknownSellerMode } from '../sellerClassificationService.js';

const db = getDb();

const DEFAULT_SETTINGS = {
  version: 'deal-engine-v1',
  amazon: {
    dayMinMarketAdvantagePct: 15,
    nightMinMarketAdvantagePct: 25
  },
  fbm: {
    dayMinMarketAdvantagePct: 20,
    nightMinMarketAdvantagePct: 30
  },
  global: {
    keepaApproveScore: 70,
    keepaQueueScore: 50,
    queueMarginPct: 3,
    queueEnabled: true,
    nightModeEnabled: true,
    nightStartHour: 22,
    nightEndHour: 6,
    cheapProductLimit: 20,
    requireMarketForCheapProducts: true,
    requireMarketForNoNameProducts: true
  },
  output: {
    telegramEnabled: true,
    whatsappEnabled: true
  },
  ai: {
    resolverEnabled: false,
    amazonDirectEnabled: true,
    onlyOnUncertainty: true,
    alwaysInDebug: true
  },
  quality: {
    marketCompareAmazonDirectEnabled: true,
    marketCompareAmazonDirectOnly: true,
    aiAmazonDirectOnly: true,
    allowFbaThirdPartyMarketCompare: false,
    allowFbaThirdPartyAi: false,
    allowFbmMarketCompare: false,
    allowFbmAi: false,
    unknownSellerMode: 'review'
  }
};

function mapSettingsRow(row = {}) {
  return {
    version: DEFAULT_SETTINGS.version,
    amazon: {
      dayMinMarketAdvantagePct: round(
        parseNumber(row.amazon_day_min_market_pct, DEFAULT_SETTINGS.amazon.dayMinMarketAdvantagePct) ??
          DEFAULT_SETTINGS.amazon.dayMinMarketAdvantagePct,
        2
      ),
      nightMinMarketAdvantagePct: round(
        parseNumber(row.amazon_night_min_market_pct, DEFAULT_SETTINGS.amazon.nightMinMarketAdvantagePct) ??
          DEFAULT_SETTINGS.amazon.nightMinMarketAdvantagePct,
        2
      )
    },
    fbm: {
      dayMinMarketAdvantagePct: round(
        parseNumber(row.fbm_day_min_market_pct, DEFAULT_SETTINGS.fbm.dayMinMarketAdvantagePct) ??
          DEFAULT_SETTINGS.fbm.dayMinMarketAdvantagePct,
        2
      ),
      nightMinMarketAdvantagePct: round(
        parseNumber(row.fbm_night_min_market_pct, DEFAULT_SETTINGS.fbm.nightMinMarketAdvantagePct) ??
          DEFAULT_SETTINGS.fbm.nightMinMarketAdvantagePct,
        2
      )
    },
    global: {
      keepaApproveScore: clamp(
        round(parseNumber(row.keepa_approve_score, DEFAULT_SETTINGS.global.keepaApproveScore) ?? 70, 2),
        0,
        100
      ),
      keepaQueueScore: clamp(
        round(parseNumber(row.keepa_queue_score, DEFAULT_SETTINGS.global.keepaQueueScore) ?? 50, 2),
        0,
        100
      ),
      queueMarginPct: clamp(
        round(parseNumber(row.queue_margin_pct, DEFAULT_SETTINGS.global.queueMarginPct) ?? 3, 2),
        0,
        30
      ),
      queueEnabled: parseBool(row.queue_enabled, DEFAULT_SETTINGS.global.queueEnabled),
      nightModeEnabled: parseBool(row.night_mode_enabled, DEFAULT_SETTINGS.global.nightModeEnabled),
      nightStartHour: clamp(parseInteger(row.night_start_hour, DEFAULT_SETTINGS.global.nightStartHour), 0, 23),
      nightEndHour: clamp(parseInteger(row.night_end_hour, DEFAULT_SETTINGS.global.nightEndHour), 0, 23),
      cheapProductLimit: clamp(
        round(parseNumber(row.cheap_product_limit, DEFAULT_SETTINGS.global.cheapProductLimit) ?? 20, 2),
        1,
        500
      ),
      requireMarketForCheapProducts: parseBool(
        row.require_market_for_cheap,
        DEFAULT_SETTINGS.global.requireMarketForCheapProducts
      ),
      requireMarketForNoNameProducts: parseBool(
        row.require_market_for_no_name,
        DEFAULT_SETTINGS.global.requireMarketForNoNameProducts
      )
    },
    output: {
      telegramEnabled: parseBool(row.telegram_output_enabled, DEFAULT_SETTINGS.output.telegramEnabled),
      whatsappEnabled: parseBool(row.whatsapp_output_enabled, DEFAULT_SETTINGS.output.whatsappEnabled)
    },
    ai: {
      resolverEnabled: parseBool(row.ai_resolver_enabled, DEFAULT_SETTINGS.ai.resolverEnabled),
      amazonDirectEnabled: parseBool(row.ai_amazon_direct_enabled, DEFAULT_SETTINGS.ai.amazonDirectEnabled),
      onlyOnUncertainty: parseBool(row.ai_only_on_uncertainty, DEFAULT_SETTINGS.ai.onlyOnUncertainty),
      alwaysInDebug: parseBool(row.ai_always_in_debug, DEFAULT_SETTINGS.ai.alwaysInDebug)
    },
    quality: {
      marketCompareAmazonDirectEnabled: parseBool(
        row.market_compare_amazon_direct_enabled,
        DEFAULT_SETTINGS.quality.marketCompareAmazonDirectEnabled
      ),
      marketCompareAmazonDirectOnly: parseBool(
        row.market_compare_amazon_direct_only,
        DEFAULT_SETTINGS.quality.marketCompareAmazonDirectOnly
      ),
      aiAmazonDirectOnly: parseBool(row.ai_amazon_direct_only, DEFAULT_SETTINGS.quality.aiAmazonDirectOnly),
      allowFbaThirdPartyMarketCompare: parseBool(
        row.allow_fba_market_compare,
        DEFAULT_SETTINGS.quality.allowFbaThirdPartyMarketCompare
      ),
      allowFbaThirdPartyAi: parseBool(row.allow_fba_ai, DEFAULT_SETTINGS.quality.allowFbaThirdPartyAi),
      allowFbmMarketCompare: parseBool(row.allow_fbm_market_compare, DEFAULT_SETTINGS.quality.allowFbmMarketCompare),
      allowFbmAi: parseBool(row.allow_fbm_ai, DEFAULT_SETTINGS.quality.allowFbmAi),
      unknownSellerMode: normalizeUnknownSellerMode(row.unknown_seller_mode || DEFAULT_SETTINGS.quality.unknownSellerMode)
    },
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

export function getDealEngineSettings() {
  const row = db.prepare(`SELECT * FROM deal_engine_settings WHERE id = 1`).get() || null;
  return mapSettingsRow(row);
}

export function saveDealEngineSettings(input = {}) {
  const current = getDealEngineSettings();
  const next = {
    amazon: {
      dayMinMarketAdvantagePct: clamp(
        parseNumber(input?.amazon?.dayMinMarketAdvantagePct, current.amazon.dayMinMarketAdvantagePct) ??
          current.amazon.dayMinMarketAdvantagePct,
        0,
        100
      ),
      nightMinMarketAdvantagePct: clamp(
        parseNumber(input?.amazon?.nightMinMarketAdvantagePct, current.amazon.nightMinMarketAdvantagePct) ??
          current.amazon.nightMinMarketAdvantagePct,
        0,
        100
      )
    },
    fbm: {
      dayMinMarketAdvantagePct: clamp(
        parseNumber(input?.fbm?.dayMinMarketAdvantagePct, current.fbm.dayMinMarketAdvantagePct) ??
          current.fbm.dayMinMarketAdvantagePct,
        0,
        100
      ),
      nightMinMarketAdvantagePct: clamp(
        parseNumber(input?.fbm?.nightMinMarketAdvantagePct, current.fbm.nightMinMarketAdvantagePct) ??
          current.fbm.nightMinMarketAdvantagePct,
        0,
        100
      )
    },
    global: {
      keepaApproveScore: clamp(
        parseNumber(input?.global?.keepaApproveScore, current.global.keepaApproveScore) ?? current.global.keepaApproveScore,
        0,
        100
      ),
      keepaQueueScore: clamp(
        parseNumber(input?.global?.keepaQueueScore, current.global.keepaQueueScore) ?? current.global.keepaQueueScore,
        0,
        100
      ),
      queueMarginPct: clamp(
        parseNumber(input?.global?.queueMarginPct, current.global.queueMarginPct) ?? current.global.queueMarginPct,
        0,
        30
      ),
      queueEnabled:
        input?.global?.queueEnabled === undefined ? current.global.queueEnabled : parseBool(input.global.queueEnabled),
      nightModeEnabled:
        input?.global?.nightModeEnabled === undefined
          ? current.global.nightModeEnabled
          : parseBool(input.global.nightModeEnabled),
      nightStartHour: clamp(
        parseInteger(input?.global?.nightStartHour, current.global.nightStartHour),
        0,
        23
      ),
      nightEndHour: clamp(parseInteger(input?.global?.nightEndHour, current.global.nightEndHour), 0, 23),
      cheapProductLimit: clamp(
        parseNumber(input?.global?.cheapProductLimit, current.global.cheapProductLimit) ?? current.global.cheapProductLimit,
        1,
        500
      ),
      requireMarketForCheapProducts:
        input?.global?.requireMarketForCheapProducts === undefined
          ? current.global.requireMarketForCheapProducts
          : parseBool(input.global.requireMarketForCheapProducts),
      requireMarketForNoNameProducts:
        input?.global?.requireMarketForNoNameProducts === undefined
          ? current.global.requireMarketForNoNameProducts
          : parseBool(input.global.requireMarketForNoNameProducts)
    },
    output: {
      telegramEnabled:
        input?.output?.telegramEnabled === undefined ? current.output.telegramEnabled : parseBool(input.output.telegramEnabled),
      whatsappEnabled:
        input?.output?.whatsappEnabled === undefined ? current.output.whatsappEnabled : parseBool(input.output.whatsappEnabled)
    },
    ai: {
      resolverEnabled:
        input?.ai?.resolverEnabled === undefined ? current.ai.resolverEnabled : parseBool(input.ai.resolverEnabled),
      amazonDirectEnabled:
        input?.ai?.amazonDirectEnabled === undefined
          ? current.ai.amazonDirectEnabled
          : parseBool(input.ai.amazonDirectEnabled),
      onlyOnUncertainty:
        input?.ai?.onlyOnUncertainty === undefined
          ? current.ai.onlyOnUncertainty
          : parseBool(input.ai.onlyOnUncertainty),
      alwaysInDebug:
        input?.ai?.alwaysInDebug === undefined ? current.ai.alwaysInDebug : parseBool(input.ai.alwaysInDebug)
    },
    quality: {
      marketCompareAmazonDirectEnabled:
        input?.quality?.marketCompareAmazonDirectEnabled === undefined
          ? current.quality.marketCompareAmazonDirectEnabled
          : parseBool(input.quality.marketCompareAmazonDirectEnabled),
      marketCompareAmazonDirectOnly:
        input?.quality?.marketCompareAmazonDirectOnly === undefined
          ? current.quality.marketCompareAmazonDirectOnly
          : parseBool(input.quality.marketCompareAmazonDirectOnly),
      aiAmazonDirectOnly:
        input?.quality?.aiAmazonDirectOnly === undefined
          ? current.quality.aiAmazonDirectOnly
          : parseBool(input.quality.aiAmazonDirectOnly),
      allowFbaThirdPartyMarketCompare:
        input?.quality?.allowFbaThirdPartyMarketCompare === undefined
          ? current.quality.allowFbaThirdPartyMarketCompare
          : parseBool(input.quality.allowFbaThirdPartyMarketCompare),
      allowFbaThirdPartyAi:
        input?.quality?.allowFbaThirdPartyAi === undefined
          ? current.quality.allowFbaThirdPartyAi
          : parseBool(input.quality.allowFbaThirdPartyAi),
      allowFbmMarketCompare:
        input?.quality?.allowFbmMarketCompare === undefined
          ? current.quality.allowFbmMarketCompare
          : parseBool(input.quality.allowFbmMarketCompare),
      allowFbmAi:
        input?.quality?.allowFbmAi === undefined
          ? current.quality.allowFbmAi
          : parseBool(input.quality.allowFbmAi),
      unknownSellerMode:
        input?.quality?.unknownSellerMode === undefined
          ? current.quality.unknownSellerMode
          : normalizeUnknownSellerMode(input.quality.unknownSellerMode)
    }
  };
  const timestamp = nowIso();

  db.prepare(
    `
      UPDATE deal_engine_settings
      SET amazon_day_min_market_pct = @amazonDay,
          amazon_night_min_market_pct = @amazonNight,
          fbm_day_min_market_pct = @fbmDay,
          fbm_night_min_market_pct = @fbmNight,
          keepa_approve_score = @keepaApproveScore,
          keepa_queue_score = @keepaQueueScore,
          queue_margin_pct = @queueMarginPct,
          queue_enabled = @queueEnabled,
          night_mode_enabled = @nightModeEnabled,
          night_start_hour = @nightStartHour,
          night_end_hour = @nightEndHour,
          cheap_product_limit = @cheapProductLimit,
          require_market_for_cheap = @requireMarketForCheap,
          require_market_for_no_name = @requireMarketForNoName,
          telegram_output_enabled = @telegramOutputEnabled,
          whatsapp_output_enabled = @whatsappOutputEnabled,
          ai_resolver_enabled = @aiResolverEnabled,
          ai_amazon_direct_enabled = @aiAmazonDirectEnabled,
          ai_only_on_uncertainty = @aiOnlyOnUncertainty,
          ai_always_in_debug = @aiAlwaysInDebug,
          market_compare_amazon_direct_enabled = @marketCompareAmazonDirectEnabled,
          market_compare_amazon_direct_only = @marketCompareAmazonDirectOnly,
          ai_amazon_direct_only = @aiAmazonDirectOnly,
          allow_fba_market_compare = @allowFbaMarketCompare,
          allow_fba_ai = @allowFbaAi,
          allow_fbm_market_compare = @allowFbmMarketCompare,
          allow_fbm_ai = @allowFbmAi,
          unknown_seller_mode = @unknownSellerMode,
          updated_at = @updatedAt
      WHERE id = 1
    `
  ).run({
    amazonDay: next.amazon.dayMinMarketAdvantagePct,
    amazonNight: next.amazon.nightMinMarketAdvantagePct,
    fbmDay: next.fbm.dayMinMarketAdvantagePct,
    fbmNight: next.fbm.nightMinMarketAdvantagePct,
    keepaApproveScore: next.global.keepaApproveScore,
    keepaQueueScore: next.global.keepaQueueScore,
    queueMarginPct: next.global.queueMarginPct,
    queueEnabled: next.global.queueEnabled ? 1 : 0,
    nightModeEnabled: next.global.nightModeEnabled ? 1 : 0,
    nightStartHour: next.global.nightStartHour,
    nightEndHour: next.global.nightEndHour,
    cheapProductLimit: next.global.cheapProductLimit,
    requireMarketForCheap: next.global.requireMarketForCheapProducts ? 1 : 0,
    requireMarketForNoName: next.global.requireMarketForNoNameProducts ? 1 : 0,
    telegramOutputEnabled: next.output.telegramEnabled ? 1 : 0,
    whatsappOutputEnabled: next.output.whatsappEnabled ? 1 : 0,
    aiResolverEnabled: next.ai.resolverEnabled ? 1 : 0,
    aiAmazonDirectEnabled: next.ai.amazonDirectEnabled ? 1 : 0,
    aiOnlyOnUncertainty: next.ai.onlyOnUncertainty ? 1 : 0,
    aiAlwaysInDebug: next.ai.alwaysInDebug ? 1 : 0,
    marketCompareAmazonDirectEnabled: next.quality.marketCompareAmazonDirectEnabled ? 1 : 0,
    marketCompareAmazonDirectOnly: next.quality.marketCompareAmazonDirectOnly ? 1 : 0,
    aiAmazonDirectOnly: next.quality.aiAmazonDirectOnly ? 1 : 0,
    allowFbaMarketCompare: next.quality.allowFbaThirdPartyMarketCompare ? 1 : 0,
    allowFbaAi: next.quality.allowFbaThirdPartyAi ? 1 : 0,
    allowFbmMarketCompare: next.quality.allowFbmMarketCompare ? 1 : 0,
    allowFbmAi: next.quality.allowFbmAi ? 1 : 0,
    unknownSellerMode: next.quality.unknownSellerMode,
    updatedAt: timestamp
  });

  return getDealEngineSettings();
}

export function resolveDealEngineDayPart(settings, overrideDayPart = '', date = new Date()) {
  if (overrideDayPart === 'day' || overrideDayPart === 'night') {
    return overrideDayPart;
  }

  if (!settings.global.nightModeEnabled) {
    return 'day';
  }

  const hour = date.getHours();
  const startHour = settings.global.nightStartHour;
  const endHour = settings.global.nightEndHour;

  if (startHour === endHour) {
    return 'night';
  }

  const nightActive =
    startHour < endHour ? hour >= startHour && hour < endHour : hour >= startHour || hour < endHour;

  return nightActive ? 'night' : 'day';
}

export function getRequiredMarketAdvantagePct(settings, sellerArea, dayPart) {
  const scope = sellerArea === 'AMAZON' ? settings.amazon : settings.fbm;
  return dayPart === 'night' ? scope.nightMinMarketAdvantagePct : scope.dayMinMarketAdvantagePct;
}

export { DEFAULT_SETTINGS as DEAL_ENGINE_DEFAULT_SETTINGS };
