import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { once } from 'node:events';
import { Worker } from 'node:worker_threads';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-hardening-'));
const dbPath = path.join(tempRoot, 'deals.db');

process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = dbPath;
process.env.TELEGRAM_BOT_TOKEN = '';
process.env.TELEGRAM_CHAT_ID = '';
process.env.TELEGRAM_TEST_CHAT_ID = '';
process.env.WHATSAPP_DELIVERY_ENABLED = '0';
process.env.WHATSAPP_DELIVERY_ENDPOINT = '';
process.env.WHATSAPP_DELIVERY_TOKEN = '';
process.env.WHATSAPP_DELIVERY_SENDER = '';

const { getDb } = await import('../db.js');
const { analyzeDealWithEngine } = await import('../services/dealEngine/service.js');
const {
  createPublishingEntry,
  getPublishingQueueEntry,
  processPublishingQueueEntry,
  recoverPublishingQueueState
} = await import('../services/publisherService.js');
const { __testablesDirectPublisher } = await import('../services/directPublisher.js');
const { savePostedDeal } = await import('../services/dealHistoryService.js');
const { buildPublishingChannelLabel } = await import('../services/databaseService.js');

const db = getDb();

function buildKeepaSeries(prices = [], { startDaysAgo = 180, stepDays = 30 } = {}) {
  const start = Date.now() - startDaysAgo * 24 * 60 * 60 * 1000;

  return prices.flatMap((price, index) => [start + index * stepDays * 24 * 60 * 60 * 1000, price]);
}

function buildStableKeepaPayload(prices = [122, 118, 114, 109, 104, 99]) {
  return {
    product: {
      csv: {
        AMAZON: buildKeepaSeries(prices)
      }
    }
  };
}

function buildSpikeKeepaPayload() {
  return {
    product: {
      csv: {
        AMAZON: buildKeepaSeries([102, 248, 100, 242, 98, 96], { startDaysAgo: 9, stepDays: 1 })
      }
    },
    markers: ['coupon', 'promotion']
  };
}

function countRows(tableName) {
  return Number(db.prepare(`SELECT COUNT(*) AS count FROM ${tableName}`).get()?.count || 0);
}

function resetState() {
  db.exec(`
    DELETE FROM publishing_logs;
    DELETE FROM publishing_targets;
    DELETE FROM publishing_queue;
    DELETE FROM deals_history;
    DELETE FROM deal_status_registry;
    DELETE FROM deal_engine_runs;
    DELETE FROM keepa_review_labels;
    DELETE FROM keepa_review_items;
    DELETE FROM keepa_fake_drop_scores;
    DELETE FROM keepa_feature_snapshots;
    DELETE FROM keepa_example_library;
    DELETE FROM keepa_results;
    DELETE FROM sqlite_sequence
    WHERE name IN (
      'publishing_logs',
      'publishing_targets',
      'publishing_queue',
      'deals_history',
      'deal_status_registry',
      'deal_engine_runs',
      'keepa_review_labels',
      'keepa_review_items',
      'keepa_fake_drop_scores',
      'keepa_feature_snapshots',
      'keepa_example_library',
      'keepa_results'
    );
  `);

  db.prepare(
    `
      UPDATE deal_engine_settings
      SET telegram_output_enabled = 0,
          whatsapp_output_enabled = 0,
          ai_resolver_enabled = 0
      WHERE id = 1
    `
  ).run();
}

function buildQueuePayload(overrides = {}) {
  return {
    title: overrides.title || 'Queue Test Deal',
    link: overrides.link || 'https://www.amazon.de/dp/B000TEST01',
    normalizedUrl: overrides.normalizedUrl || overrides.link || 'https://www.amazon.de/dp/B000TEST01',
    asin: overrides.asin || 'B000TEST01',
    sellerType: overrides.sellerType || 'AMAZON',
    currentPrice: overrides.currentPrice || '79.99',
    oldPrice: overrides.oldPrice || '99.99',
    couponCode: overrides.couponCode || '',
    textByChannel: {
      telegram: overrides.telegramText || 'Telegram Nachricht',
      whatsapp: overrides.whatsappText || 'WhatsApp Nachricht'
    },
    imageVariants: {},
    targetImageSources: {
      telegram: 'none',
      whatsapp: 'none'
    }
  };
}

function savePostedChannel({ payload, target, channelType, targetLabel }) {
  savePostedDeal({
    asin: payload.asin,
    originalUrl: payload.link,
    normalizedUrl: payload.normalizedUrl || payload.link,
    title: payload.title,
    currentPrice: payload.currentPrice,
    oldPrice: payload.oldPrice,
    sellerType: payload.sellerType,
    postedAt: new Date().toISOString(),
    channel: buildPublishingChannelLabel(channelType, targetLabel),
    couponCode: payload.couponCode || '',
    sourceType: payload.databaseSourceType || 'publisher_queue',
    sourceId: payload.sourceId || null,
    queueId: target.queue_id,
    origin: payload.databaseOrigin || 'automatic',
    decisionReason: `${channelType} Testversand erfolgreich.`,
    meta: {
      targetId: target.id
    }
  });
}

async function waitForLockReady(worker) {
  const [chunk] = await once(worker, 'message');
  assert.equal(String(chunk || '').trim(), 'LOCK_READY');
}

async function run(name, fn) {
  resetState();
  try {
    await fn();
    console.log(`PASS ${name}`);
    return true;
  } catch (error) {
    console.error(`FAIL ${name}`);
    console.error(error);
    return false;
  }
}

const results = [];

results.push(
  await run('Markt-Approve bleibt Hauptentscheidung mit lowestPrice Alias', async () => {
    const result = await analyzeDealWithEngine({
      source: {
        name: 'Test Quelle',
        platform: 'telegram',
        type: 'manual'
      },
      deal: {
        title: 'Makita Akkuschrauber',
        amazonUrl: 'https://www.amazon.de/dp/B0TESTAAA1',
        amazonPrice: 79.99,
        sellerType: 'AMAZON',
        referencePrice: 109.99,
        variantKey: 'solo',
        quantityKey: '1'
      },
      market: {
        offers: [
          { id: 'valid-1', shopName: 'WerkzeugProfi', price: 104.99, shippingPrice: 0, variantKey: 'solo', quantityKey: '1', isRealShop: true },
          { id: 'valid-2', shopName: 'ToolCenter', price: 98.99, shippingPrice: 5.5, variantKey: 'solo', quantityKey: '1', isRealShop: true },
          { id: 'fake-1', shopName: 'FlashMegaDiscount', price: 39.99, shippingPrice: 0, variantKey: 'solo', quantityKey: '1', isRealShop: false }
        ]
      },
      keepa: {
        payload: buildStableKeepaPayload(),
        avg90: 109.99,
        avg180: 114.99,
        min90: 98.99
      },
      meta: {
        overrideDayPart: 'day'
      }
    });

    const item = result.item;
    assert.equal(item.decision, 'APPROVE');
    assert.equal(item.analysis.decisionSource, 'market');
    assert.equal(item.analysis.keepaFallbackUsed, false);
    assert.equal(item.analysis.marketPrice, 104.49);
    assert.equal(item.analysis.lowestPrice, 104.49);
    assert.equal(item.analysis.marketComparison.lowestPrice, 104.49);
    assert.equal(item.analysis.marketComparison.cheapestOffer.id, 'valid-2');
  })
);

results.push(
  await run('Keepa-Fallback greift nur ohne brauchbaren Marktpreis', async () => {
    const result = await analyzeDealWithEngine({
      deal: {
        title: 'Bosch Schleifer',
        amazonUrl: 'https://www.amazon.de/dp/B0TESTAAA2',
        amazonPrice: 69.99,
        sellerType: 'AMAZON',
        referencePrice: 89.99,
        variantKey: 'solo',
        quantityKey: '1'
      },
      market: {
        offers: [{ id: 'wrong-variant', shopName: 'ToolShop', price: 89.99, shippingPrice: 0, variantKey: 'set', quantityKey: '1', isRealShop: true }]
      },
      keepa: {
        payload: buildStableKeepaPayload([110, 101, 93, 85, 78, 69.99]),
        avg90: 108.99,
        avg180: 118.99,
        min90: 69.99,
        isLowest90: true,
        nearLow: true
      },
      meta: {
        overrideDayPart: 'day'
      }
    });

    const item = result.item;
    assert.equal(item.decision, 'APPROVE');
    assert.equal(item.analysis.decisionSource, 'keepa');
    assert.equal(item.analysis.keepaFallbackUsed, true);
    assert.equal(item.analysis.marketComparison.available, false);
    assert.ok((item.analysis.keepaScore || 0) >= 70);
  })
);

results.push(
  await run('Autonome Fake-Pattern-Heuristik sitzt im Hauptpfad und kann APPROVE zu REJECT drehen', async () => {
    const result = await analyzeDealWithEngine({
      deal: {
        title: 'Gaming Headset',
        amazonUrl: 'https://www.amazon.de/dp/B0TESTAAA3',
        amazonPrice: 84.99,
        sellerType: 'FBM',
        referencePrice: 249.99,
        variantKey: 'schwarz',
        quantityKey: '1'
      },
      market: {
        offers: [{ id: 'valid-1', shopName: 'AudioCenter', price: 129.99, shippingPrice: 0, variantKey: 'schwarz', quantityKey: '1', isRealShop: true }]
      },
      keepa: {
        payload: buildSpikeKeepaPayload(),
        avg90: 159.99,
        avg180: 164.99,
        min90: 84.99
      },
      meta: {
        overrideDayPart: 'day'
      }
    });

    const item = result.item;
    assert.equal(item.analysis.decisionSource, 'market');
    assert.equal(item.decision, 'REJECT');
    assert.equal(item.analysis.fakePatternStatus, 'reject');
    assert.equal(item.analysis.fakePatterns.engine.classification, 'wahrscheinlicher_fake_drop');
    assert.ok((item.analysis.fakePatterns.engine.fakeDropRisk || 0) >= 70);
  })
);

results.push(
  await run('Dedup sperrt doppelte aktive Queue-Eintraege atomar ab', async () => {
    const payload = buildQueuePayload({
      asin: 'B000LOCK01',
      link: 'https://www.amazon.de/dp/B000LOCK01',
      normalizedUrl: 'https://www.amazon.de/dp/B000LOCK01'
    });

    const firstQueue = createPublishingEntry({
      sourceType: 'deal_engine',
      payload,
      targets: [{ channelType: 'telegram', isEnabled: true, imageSource: 'none' }]
    });

    assert.ok(firstQueue?.id);
    assert.throws(
      () =>
        createPublishingEntry({
          sourceType: 'deal_engine',
          payload,
          targets: [{ channelType: 'telegram', isEnabled: true, imageSource: 'none' }]
        }),
      /Deal-Lock aktiv/
    );
    assert.equal(countRows('publishing_queue'), 1);
  })
);

results.push(
  await run('Retry, Recovery und next_retry_at Bereinigung bleiben konsistent', async () => {
    const queue = createPublishingEntry({
      sourceType: 'deal_engine',
      payload: buildQueuePayload({
        asin: 'B000RETRY1',
        link: 'https://www.amazon.de/dp/B000RETRY1',
        normalizedUrl: 'https://www.amazon.de/dp/B000RETRY1'
      }),
      targets: [
        { channelType: 'telegram', isEnabled: true, imageSource: 'none', targetLabel: 'Primary Channel' },
        { channelType: 'whatsapp', isEnabled: true, imageSource: 'none', targetLabel: 'Buyer Group' }
      ]
    });

    let whatsappAttempts = 0;
    await processPublishingQueueEntry(queue.id, {
      processors: {
        telegram: async (target, payload) => {
          savePostedChannel({
            payload,
            target,
            channelType: 'telegram',
            targetLabel: target.target_label || 'Primary Channel'
          });
          return {
            targetLabel: target.target_label || 'Primary Channel'
          };
        },
        whatsapp: async (target, payload) => {
          whatsappAttempts += 1;

          if (whatsappAttempts === 1) {
            const error = new Error('WhatsApp Gateway timeout');
            error.retryable = true;
            error.retryLimit = 2;
            throw error;
          }

          savePostedChannel({
            payload,
            target,
            channelType: 'whatsapp',
            targetLabel: target.target_label || 'Buyer Group'
          });
          return {
            targetLabel: target.target_label || 'Buyer Group'
          };
        }
      }
    });

    const retryQueue = getPublishingQueueEntry(queue.id);
    assert.equal(retryQueue.status, 'retry');
    assert.equal(retryQueue.retryCount, 1);
    assert.ok(retryQueue.next_retry_at);

    db.prepare(`UPDATE publishing_queue SET next_retry_at = ? WHERE id = ?`).run(new Date(Date.now() - 1000).toISOString(), queue.id);

    await processPublishingQueueEntry(queue.id, {
      processors: {
        telegram: async (target, payload) => {
          savePostedChannel({
            payload,
            target,
            channelType: 'telegram',
            targetLabel: target.target_label || 'Primary Channel'
          });
          return {
            targetLabel: target.target_label || 'Primary Channel'
          };
        },
        whatsapp: async (target, payload) => {
          savePostedChannel({
            payload,
            target,
            channelType: 'whatsapp',
            targetLabel: target.target_label || 'Buyer Group'
          });
          return {
            targetLabel: target.target_label || 'Buyer Group'
          };
        }
      }
    });

    const finalQueue = getPublishingQueueEntry(queue.id);
    const registry = db.prepare(`SELECT * FROM deal_status_registry WHERE deal_key = ?`).get('asin:B000RETRY1');

    assert.equal(finalQueue.status, 'sent');
    assert.equal(finalQueue.retryCount, 1);
    assert.equal(finalQueue.next_retry_at, null);
    assert.equal(finalQueue.attemptCount, 3);
    assert.equal(countRows('deals_history'), 2);
    assert.ok(String(registry?.posted_channels_json || '').includes('TELEGRAM:Primary Channel'));
    assert.ok(String(registry?.posted_channels_json || '').includes('WHATSAPP:Buyer Group'));

    db.prepare(`UPDATE publishing_queue SET status = ?, next_retry_at = ?, updated_at = ? WHERE id = ?`).run(
      'sending',
      new Date(Date.now() + 60_000).toISOString(),
      new Date().toISOString(),
      queue.id
    );
    db.prepare(`UPDATE publishing_targets SET status = ?, updated_at = ? WHERE queue_id = ?`).run(
      'sending',
      new Date().toISOString(),
      queue.id
    );

    const recovery = recoverPublishingQueueState();
    const recoveredQueue = getPublishingQueueEntry(queue.id);

    assert.ok(recovery.recoveredQueues >= 1);
    assert.ok(recovery.recoveredTargets >= 1);
    assert.equal(recoveredQueue.status, 'retry');
    assert.equal(recoveredQueue.next_retry_at, null);
    assert.ok(recoveredQueue.targets.every((target) => target.status === 'retry'));
  })
);

results.push(
  await run('Nicht-retrybare Fehler erhoehen retry_count nicht und lassen next_retry_at leer', async () => {
    const queue = createPublishingEntry({
      sourceType: 'deal_engine',
      payload: buildQueuePayload({
        asin: 'B000FAIL01',
        link: 'https://www.amazon.de/dp/B000FAIL01',
        normalizedUrl: 'https://www.amazon.de/dp/B000FAIL01'
      }),
      targets: [{ channelType: 'telegram', isEnabled: true, imageSource: 'none', targetLabel: 'Dead Channel' }]
    });

    await processPublishingQueueEntry(queue.id, {
      processors: {
        telegram: async () => {
          const error = new Error('Telegram Bot Client ist deaktiviert.');
          error.retryable = false;
          error.retryLimit = 3;
          throw error;
        }
      }
    });

    const failedQueue = getPublishingQueueEntry(queue.id);
    assert.equal(failedQueue.status, 'failed');
    assert.equal(failedQueue.retryCount, 0);
    assert.equal(failedQueue.next_retry_at, null);
    assert.equal(failedQueue.attemptCount, 1);
  })
);

results.push(
  await run('Kurzer SQLITE_BUSY Lock fuehrt nicht zu Datenverlust bei Queue-Erzeugung', async () => {
    const helperScript = path.join(process.cwd(), 'tests', 'helpers', 'holdSqliteWriteLock.mjs');
    const lockWorker = new Worker(helperScript, {
      workerData: {
        dbPath,
        holdMs: 250
      }
    });

    try {
      await waitForLockReady(lockWorker);
      const startedAt = Date.now();
      const queue = createPublishingEntry({
        sourceType: 'deal_engine',
        payload: buildQueuePayload({
          asin: 'B000BUSY01',
          link: 'https://www.amazon.de/dp/B000BUSY01',
          normalizedUrl: 'https://www.amazon.de/dp/B000BUSY01'
        }),
        targets: [{ channelType: 'telegram', isEnabled: true, imageSource: 'none', targetLabel: 'Busy Channel' }]
      });

      assert.ok(queue?.id);
      assert.ok(Date.now() - startedAt >= 40);
      assert.equal(countRows('publishing_queue'), 1);
      assert.equal(countRows('publishing_targets'), 1);
    } finally {
      await lockWorker.terminate();
    }
  })
);

results.push(
  await run('Generator Direktpost Summary bleibt stabil und deliveries sind immer Arrays', async () => {
    const summary = __testablesDirectPublisher.summarizeQueueResults(
      {
        queue: {
          id: 77,
          status: 'sent',
          targets: [
            {
              channel_type: 'telegram',
              status: 'sent',
              posted_at: '2026-04-23T10:15:00.000Z'
            }
          ]
        },
        results: [
          {
            channelType: 'telegram',
            status: 'sent',
            workerResult: {
              targets: [
                {
                  messageId: 'tg-123',
                  targetChatId: '-1009876543210'
                }
              ]
            }
          }
        ]
      },
      {
        telegramImageSource: 'standard',
        whatsappImageSource: 'none',
        facebookImageSource: 'link_preview'
      },
      99
    );

    assert.equal(summary.queue.id, 77);
    assert.equal(summary.results.telegram.status, 'sent');
    assert.equal(summary.results.telegram.messageId, 'tg-123');
    assert.equal(summary.results.telegram.chatId, '-1009876543210');
    assert.deepEqual(summary.deliveries.whatsapp, []);
    assert.deepEqual(summary.deliveries.facebook, []);
    assert.equal(summary.postedAt, '2026-04-23T10:15:00.000Z');
  })
);

results.push(
  await run('Generator Direktpost liefert klare Fehlermeldung ohne Telegram-Ziel', async () => {
    assert.throws(
      () =>
        __testablesDirectPublisher.assertDirectPublishingTargets(
          {
            enableTelegram: true
          },
          {
            telegramChatIds: []
          }
        ),
      /Keine Telegram-Zielgruppe/
    );
  })
);

try {
  db.close();
} catch {
  // no-op
}

fs.rmSync(tempRoot, { recursive: true, force: true });

if (results.some((item) => item !== true)) {
  process.exit(1);
}

console.log(`OK ${results.length} Regressionstests bestanden`);
