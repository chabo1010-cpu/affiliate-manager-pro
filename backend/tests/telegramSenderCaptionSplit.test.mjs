import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-telegram-sender-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');
process.env.TELEGRAM_BOT_TOKEN = 'test-token';
process.env.TELEGRAM_CHAT_ID = '-1001234567890';

const {
  sendTelegramCouponFollowUp,
  sendTelegramDealPost,
  sendTelegramPost,
  __testablesTelegramSender
} = await import('../services/telegramSenderService.js');

const { splitTelegramPhotoPostText, trimTelegramPhotoCaption } = __testablesTelegramSender;
const tinyPngDataUrl =
  'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+iM3sAAAAASUVORK5CYII=';

async function test(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

function buildLongMainText() {
  const extraLines = Array.from({ length: 24 }, (_, index) => `Info ${index + 1}: ${'A'.repeat(42)}`);
  return [
    '<b>Produktname fuer Caption Trim Test mit langem Generator-Inhalt</b>',
    '',
    '🔥 Jetzt <b>29,99€</b>',
    '➡️ <b>https://amazon.de/dp/B000TEST123?tag=affiliatemanager-21</b>',
    ...extraLines,
    '',
    '',
    '<i>Anzeige/Partnerlink</i>'
  ].join('\n');
}

await test('Hilfsfunktionen trennen Hauptinhalt und Debugblock sauber', async () => {
  const mainText = buildLongMainText();
  const fullText = `${mainText}\n\n🧾 <b>DEAL STATUS</b>\n📌 Entscheidung: REVIEW\n⚠️ Grund: Debug bleibt separat`;
  const splitResult = splitTelegramPhotoPostText(fullText);
  const captionResult = trimTelegramPhotoCaption(splitResult.mainText, 900);

  assert.equal(splitResult.mainText, mainText.trim());
  assert.match(splitResult.extraText, /<b>DEAL STATUS<\/b>/);
  assert.equal(splitResult.splitMarker, 'DEAL STATUS');
  assert.ok(captionResult.trimmed);
  assert.equal(captionResult.beforeLength, mainText.trim().length);
  assert.ok(captionResult.afterLength <= 900);
  assert.ok(captionResult.cutAt > 0);
});

await test('sendTelegramPost sendet Foto mit Hauptcaption und Debug separat', async () => {
  const requests = [];
  const originalFetch = global.fetch;
  const mainText = buildLongMainText();
  const debugText = '🧾 <b>DEAL STATUS</b>\n📌 Entscheidung: REVIEW\n⚠️ Grund: Debug bleibt separat';
  const fullText = `${mainText}\n\n${debugText}`;

  global.fetch = async (url, init = {}) => {
    requests.push({
      url: String(url),
      init
    });

    return {
      ok: true,
      text: async () =>
        JSON.stringify({
          ok: true,
          result: {
            message_id: requests.length,
            chat: {
              id: process.env.TELEGRAM_CHAT_ID
            }
          }
        })
    };
  };

  try {
    const result = await sendTelegramPost({
      text: fullText,
      uploadedImage: tinyPngDataUrl
    });

    assert.equal(requests.length, 2);
    assert.match(requests[0].url, /\/sendPhoto$/);
    assert.match(requests[1].url, /\/sendMessage$/);

    const photoBody = requests[0].init.body;
    const photoCaption = photoBody.get('caption');
    assert.ok(photoCaption.length <= 900);
    assert.match(photoCaption, /Produktname fuer Caption Trim Test/);
    assert.match(photoCaption, /29,99/);
    assert.match(photoCaption, /amazon\.de\/dp\/B000TEST123/);
    assert.doesNotMatch(photoCaption, /<b>DEAL STATUS<\/b>/);

    const debugPayload = JSON.parse(requests[1].init.body);
    assert.match(debugPayload.text, /<b>DEAL STATUS<\/b>/);
    assert.doesNotMatch(debugPayload.text, /Produktname fuer Caption Trim Test/);

    assert.equal(result.extraMessageIds.length, 1);
    assert.ok(result.captionInfo.trimmed);
    assert.equal(result.captionInfo.beforeLength, mainText.trim().length);
    assert.equal(result.captionInfo.afterLength, photoCaption.length);
    assert.equal(result.captionInfo.splitMarker, 'DEAL STATUS');
    assert.ok(result.captionInfo.cutAt > 0);
  } finally {
    global.fetch = originalFetch;
  }
});

await test('sendTelegramDealPost sendet den Hauptdeal als Foto-Caption und Debug separat', async () => {
  const requests = [];
  const originalFetch = global.fetch;

  global.fetch = async (url, init = {}) => {
    requests.push({
      url: String(url),
      init
    });

    return {
      ok: true,
      text: async () =>
        JSON.stringify({
          ok: true,
          result: {
            message_id: requests.length,
            chat: {
              id: process.env.TELEGRAM_CHAT_ID
            }
          }
        })
    };
  };

  try {
    const result = await sendTelegramDealPost({
      title: 'Bosch Professional GSR 18V-55 Akku-Bohrschrauber',
      price: '29,99€',
      affiliateLink: 'https://amzn.to/fremdlink',
      asin: 'B012345678',
      uploadedImage: tinyPngDataUrl,
      debugInfo:
        '⚠️ <b>TESTPOST</b>\n\n🧾 <b>DEAL STATUS</b>\n📌 Entscheidung: REVIEW\n🚀 Live: NEIN\n🧪 Testgruppe: JA\n\n📊 <b>PRÜFUNGEN</b>\n🌍 Markt: success\n🤖 KI: success\n📈 Keepa: verfuegbar',
      testMode: true
    });

    assert.equal(requests.length, 2);
    assert.match(requests[0].url, /\/sendPhoto$/);
    assert.match(requests[1].url, /\/sendMessage$/);

    const photoCaption = requests[0].init.body.get('caption');
    assert.match(photoCaption, /Bosch Professional GSR 18V-55 Akku-Bohrschrauber/);
    assert.match(photoCaption, /29,99€/);
    assert.match(photoCaption, /https:\/\/www\.amazon\.de\/dp\/B012345678\?tag=codeundcoup08-21/);
    assert.match(photoCaption, /Anzeige\/Partnerlink/);
    assert.doesNotMatch(photoCaption, /TESTPOST/);
    assert.doesNotMatch(photoCaption, /PRÜFUNGEN/);

    const debugPayload = JSON.parse(requests[1].init.body);
    assert.match(debugPayload.text, /TESTPOST/);
    assert.match(debugPayload.text, /DEAL STATUS/);
    assert.match(debugPayload.text, /PRÜFUNGEN/);
    assert.doesNotMatch(debugPayload.text, /Bosch Professional GSR 18V-55 Akku-Bohrschrauber/);
    assert.doesNotMatch(debugPayload.text, /29,99€/);
    assert.doesNotMatch(debugPayload.text, /amazon\.de\/dp\/B012345678/);

    assert.equal(result.extraMessageIds.length, 1);
    assert.equal(result.strippedForeignLink, true);
  } finally {
    global.fetch = originalFetch;
  }
});

await test('sendTelegramDealPost baut sicheren Hauptpost aus strukturierten Feldern', async () => {
  const requests = [];
  const originalFetch = global.fetch;
  const renderedGeneratorText = [
    '<b>DeWalt Akkuschrauber Set</b>',
    '',
    'ðŸ”¥ Jetzt <b>119,99â‚¬</b>',
    'âž¡ï¸ <b>https://www.amazon.de/dp/B0GENERATOR1?tag=codeundcoup08-21</b>',
    'âœ… Coupon aktivieren',
    '',
    '',
    '<i>Anzeige/Partnerlink</i>'
  ].join('\n');

  global.fetch = async (url, init = {}) => {
    requests.push({
      url: String(url),
      init
    });

    return {
      ok: true,
      text: async () =>
        JSON.stringify({
          ok: true,
          result: {
            message_id: requests.length,
            chat: {
              id: process.env.TELEGRAM_CHAT_ID
            }
          }
        })
    };
  };

  try {
    await sendTelegramDealPost({
      title: 'DeWalt Akkuschrauber Set',
      price: '119,99â‚¬',
      affiliateLink: 'https://www.amazon.de/dp/B0GENERATOR1?tag=codeundcoup08-21',
      asin: 'B0GENERATOR1',
      uploadedImage: tinyPngDataUrl,
      fallbackText: renderedGeneratorText
    });

    assert.equal(requests.length, 1);
    assert.match(requests[0].url, /\/sendPhoto$/);

    const photoCaption = requests[0].init.body.get('caption');
    assert.match(photoCaption, /<b>DeWalt Akkuschrauber Set<\/b>/);
    assert.match(photoCaption, /119,99/);
    assert.match(photoCaption, /https:\/\/www\.amazon\.de\/dp\/B0GENERATOR1\?tag=codeundcoup08-21/);
    assert.doesNotMatch(photoCaption, /Coupon aktivieren/);
    assert.match(photoCaption, /Anzeige\/Partnerlink/);
  } finally {
    global.fetch = originalFetch;
  }
});

await test('sendTelegramDealPost faellt bei sendPhoto-Fehler auf sendMessage mit Hauptdeal zurueck', async () => {
  const requests = [];
  const originalFetch = global.fetch;

  global.fetch = async (url, init = {}) => {
    requests.push({
      url: String(url),
      init
    });

    if (requests.length === 1) {
      return {
        ok: false,
        text: async () =>
          JSON.stringify({
            ok: false,
            error_code: 400,
            description: 'Bad Request: message caption is too long'
          })
      };
    }

    return {
      ok: true,
      text: async () =>
        JSON.stringify({
          ok: true,
          result: {
            message_id: requests.length,
            chat: {
              id: process.env.TELEGRAM_CHAT_ID
            }
          }
        })
    };
  };

  try {
    const result = await sendTelegramDealPost({
      title: 'Makita Akku-Kompressor',
      price: '79,99€',
      affiliateLink: 'https://www.amazon.de/dp/B0TEST12345?tag=codeundcoup08-21',
      asin: 'B0TEST1234',
      uploadedImage: tinyPngDataUrl
    });

    assert.equal(requests.length, 2);
    assert.match(requests[0].url, /\/sendPhoto$/);
    assert.match(requests[1].url, /\/sendMessage$/);

    const fallbackPayload = JSON.parse(requests[1].init.body);
    assert.match(fallbackPayload.text, /Makita Akku-Kompressor/);
    assert.match(fallbackPayload.text, /79,99€/);
    assert.match(fallbackPayload.text, /https:\/\/www\.amazon\.de\/dp\/B0TEST1234\?tag=codeundcoup08-21/);
    assert.equal(result.method, 'sendMessage');
  } finally {
    global.fetch = originalFetch;
  }
});

await test('sendTelegramCouponFollowUp nutzt denselben Copy-Button wie der Generator', async () => {
  const requests = [];
  const originalFetch = global.fetch;

  global.fetch = async (url, init = {}) => {
    requests.push({
      url: String(url),
      init
    });

    return {
      ok: true,
      text: async () =>
        JSON.stringify({
          ok: true,
          result: {
            message_id: requests.length,
            chat: {
              id: process.env.TELEGRAM_CHAT_ID
            }
          }
        })
    };
  };

  try {
    const result = await sendTelegramCouponFollowUp({
      couponCode: 'LVYF4QEK',
      chatId: process.env.TELEGRAM_CHAT_ID,
      titlePreview: 'Generator Rabattcode'
    });

    assert.equal(requests.length, 1);
    assert.match(requests[0].url, /\/sendMessage$/);

    const payload = JSON.parse(requests[0].init.body);
    assert.equal(payload.text, 'CODE:\nLVYF4QEK');
    assert.deepEqual(payload.reply_markup, {
      inline_keyboard: [
        [
          {
            text: '📋 Zum Kopieren hier klicken',
            copy_text: {
              text: 'LVYF4QEK'
            }
          }
        ]
      ]
    });
    assert.equal(result?.messageId, 1);
  } finally {
    global.fetch = originalFetch;
  }
});

console.log('OK Telegram Caption Split getestet');
