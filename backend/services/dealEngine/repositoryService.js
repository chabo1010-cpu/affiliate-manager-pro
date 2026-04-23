import { getDb } from '../../db.js';
import { fromJson, nowIso, parseInteger, parseNumber, round } from './shared.js';

const db = getDb();

function mapRunRow(row = {}) {
  const analysis = fromJson(row.analysis_json, null);
  const marketComparison = fromJson(row.market_comparison_json, null);
  const reasonDetails = fromJson(row.reason_details_json, []);

  return {
    id: Number(row.id),
    source: {
      name: row.source_name || '',
      platform: row.source_platform || '',
      type: row.source_type || ''
    },
    amazonUrl: row.amazon_url || '',
    asin: row.asin || '',
    title: row.title || '',
    sellerArea: row.seller_area || 'FBM',
    amazonPrice: parseNumber(row.amazon_price, null),
    marketPrice: parseNumber(row.market_price, null),
    lowestPrice: parseNumber(row.lowest_price, parseNumber(row.market_price, null)),
    marketAdvantagePct: parseNumber(row.market_advantage_pct, null),
    marketOfferCount: parseInteger(row.market_offer_count, 0),
    keepaScore: parseNumber(row.keepa_score, null),
    keepaDiscount90: parseNumber(row.keepa_discount_avg90, null),
    keepaDiscount180: parseNumber(row.keepa_discount_avg180, null),
    fallbackUsed: row.fallback_used === 1,
    keepaFallbackUsed: row.keepa_fallback_used === 1 || row.fallback_used === 1,
    aiStatus: row.ai_status || 'not_needed',
    aiNeeded: row.ai_needed === 1 || analysis?.aiNeeded === true,
    aiUsed: row.ai_used === 1 || analysis?.aiUsed === true,
    aiEscalation: row.ai_escalation || analysis?.aiEscalation || row.ai_status || 'not_needed',
    fakePatternStatus: row.fake_pattern_status || 'clear',
    dayPart: row.day_part || 'day',
    decision: row.decision || 'REJECT',
    decisionReason: row.decision_reason || '',
    marketComparison: marketComparison || analysis?.marketComparison || null,
    reasonDetails: Array.isArray(reasonDetails) && reasonDetails.length ? reasonDetails : analysis?.reasonDetails || [],
    outputStatus: row.output_status || 'none',
    outputQueueId: row.output_queue_id ?? null,
    outputTargetCount: parseInteger(row.output_target_count, 0),
    payload: fromJson(row.payload_json, null),
    analysis,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

export function createDealEngineRun(input = {}) {
  const timestamp = nowIso();
  const result = db
    .prepare(
      `
        INSERT INTO deal_engine_runs (
          source_name,
          source_platform,
          source_type,
          amazon_url,
          asin,
          title,
          seller_area,
          amazon_price,
          market_price,
          lowest_price,
          market_advantage_pct,
          market_offer_count,
          keepa_score,
          keepa_discount_avg90,
          keepa_discount_avg180,
          fallback_used,
          keepa_fallback_used,
          ai_status,
          ai_needed,
          ai_used,
          ai_escalation,
          fake_pattern_status,
          day_part,
          decision,
          decision_reason,
          market_comparison_json,
          reason_details_json,
          output_status,
          output_queue_id,
          output_target_count,
          payload_json,
          analysis_json,
          created_at,
          updated_at
        ) VALUES (
          @sourceName,
          @sourcePlatform,
          @sourceType,
          @amazonUrl,
          @asin,
          @title,
          @sellerArea,
          @amazonPrice,
          @marketPrice,
          @lowestPrice,
          @marketAdvantagePct,
          @marketOfferCount,
          @keepaScore,
          @keepaDiscount90,
          @keepaDiscount180,
          @fallbackUsed,
          @keepaFallbackUsed,
          @aiStatus,
          @aiNeeded,
          @aiUsed,
          @aiEscalation,
          @fakePatternStatus,
          @dayPart,
          @decision,
          @decisionReason,
          @marketComparisonJson,
          @reasonDetailsJson,
          @outputStatus,
          @outputQueueId,
          @outputTargetCount,
          @payloadJson,
          @analysisJson,
          @createdAt,
          @updatedAt
        )
      `
    )
    .run({
      sourceName: input.source?.name || '',
      sourcePlatform: input.source?.platform || '',
      sourceType: input.source?.type || '',
      amazonUrl: input.amazonUrl || '',
      asin: input.asin || '',
      title: input.title || '',
      sellerArea: input.sellerArea || 'FBM',
      amazonPrice: input.amazonPrice ?? null,
      marketPrice: input.marketPrice ?? null,
      lowestPrice: input.lowestPrice ?? null,
      marketAdvantagePct: input.marketAdvantagePct ?? null,
      marketOfferCount: input.marketOfferCount ?? 0,
      keepaScore: input.keepaScore ?? null,
      keepaDiscount90: input.keepaDiscount90 ?? null,
      keepaDiscount180: input.keepaDiscount180 ?? null,
      fallbackUsed: input.fallbackUsed ? 1 : 0,
      keepaFallbackUsed: input.keepaFallbackUsed ? 1 : 0,
      aiStatus: input.aiStatus || 'not_needed',
      aiNeeded: input.aiNeeded ? 1 : 0,
      aiUsed: input.aiUsed ? 1 : 0,
      aiEscalation: input.aiEscalation || input.aiStatus || 'not_needed',
      fakePatternStatus: input.fakePatternStatus || 'clear',
      dayPart: input.dayPart || 'day',
      decision: input.decision || 'REJECT',
      decisionReason: input.decisionReason || '',
      marketComparisonJson: JSON.stringify(input.marketComparison ?? null),
      reasonDetailsJson: JSON.stringify(input.reasonDetails ?? []),
      outputStatus: input.outputStatus || 'none',
      outputQueueId: input.outputQueueId ?? null,
      outputTargetCount: input.outputTargetCount ?? 0,
      payloadJson: JSON.stringify(input.payload ?? null),
      analysisJson: JSON.stringify(input.analysis ?? null),
      createdAt: timestamp,
      updatedAt: timestamp
    });

  return getDealEngineRunById(result.lastInsertRowid);
}

export function getDealEngineRunById(id) {
  const row = db.prepare(`SELECT * FROM deal_engine_runs WHERE id = ?`).get(id) || null;
  return row ? mapRunRow(row) : null;
}

export function listDealEngineRuns(filters = {}) {
  const limit = Math.min(200, Math.max(1, parseInteger(filters.limit, 25)));
  const decision = String(filters.decision || '').trim().toUpperCase();
  const rows = db
    .prepare(
      `
        SELECT *
        FROM deal_engine_runs
        WHERE (@decision = '' OR decision = @decision)
        ORDER BY created_at DESC
        LIMIT @limit
      `
    )
    .all({
      decision,
      limit
    });

  return {
    items: rows.map(mapRunRow)
  };
}

export function getDealEngineMetrics() {
  const row = db
    .prepare(
      `
        SELECT
          COUNT(*) AS totalRuns,
          COALESCE(SUM(CASE WHEN decision = 'APPROVE' THEN 1 ELSE 0 END), 0) AS approvedRuns,
          COALESCE(SUM(CASE WHEN decision = 'QUEUE' THEN 1 ELSE 0 END), 0) AS queuedRuns,
          COALESCE(SUM(CASE WHEN decision = 'REJECT' THEN 1 ELSE 0 END), 0) AS rejectedRuns,
          COALESCE(SUM(CASE WHEN fallback_used = 1 THEN 1 ELSE 0 END), 0) AS keepaFallbackRuns,
          COALESCE(SUM(CASE WHEN fallback_used = 0 THEN 1 ELSE 0 END), 0) AS marketRuns,
          COALESCE(SUM(CASE WHEN ai_status = 'resolved' THEN 1 ELSE 0 END), 0) AS aiResolvedRuns,
          MAX(created_at) AS lastRunAt
        FROM deal_engine_runs
      `
    )
    .get();

  return {
    totalRuns: parseInteger(row?.totalRuns, 0),
    approvedRuns: parseInteger(row?.approvedRuns, 0),
    queuedRuns: parseInteger(row?.queuedRuns, 0),
    rejectedRuns: parseInteger(row?.rejectedRuns, 0),
    keepaFallbackRuns: parseInteger(row?.keepaFallbackRuns, 0),
    marketRuns: parseInteger(row?.marketRuns, 0),
    aiResolvedRuns: parseInteger(row?.aiResolvedRuns, 0),
    approveRatePct:
      parseInteger(row?.totalRuns, 0) > 0 ? round((parseInteger(row.approvedRuns, 0) / parseInteger(row.totalRuns, 1)) * 100, 2) : 0,
    lastRunAt: row?.lastRunAt || null
  };
}
