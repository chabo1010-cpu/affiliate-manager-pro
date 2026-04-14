import { type ChangeEvent, useEffect, useMemo, useRef, useState } from 'react';
import Layout from '../components/layout/Layout';
import {
  COUPON_LINE_PREFIX,
  COUPON_OPTION_LABEL,
  MASTER_EXTRA_OPTIONS,
  MASTER_PRIMARY_OPTIONS,
  WITHOUT_OPTIONS_LABEL,
  hasEffectivePostQualifier,
  generatePostText,
  normalizeDealImageUrl
} from '../lib/postGenerator';
import { Toast, useToast } from '../components/Toast';
import './GeneratorPoster.css';

const DEFAULT_FREE_TEXT = 'ℹ️ ';
const NORMALIZED_DEFAULT_FREE_TEXT = 'ℹ️ ';
const SUCCESS_RESET_DELAY_MS = 1100;

const textOptions = MASTER_PRIMARY_OPTIONS.map((option) => ({ value: option, label: option }));
const extraOptions = [...MASTER_EXTRA_OPTIONS];
const imageSourceOptions = [
  { value: 'standard', label: 'Standardbild' },
  { value: 'upload', label: 'eigener Upload' },
  { value: 'none', label: 'kein Bild' }
] as const;
const facebookImageSourceOptions = [
  ...imageSourceOptions,
  { value: 'link_preview', label: 'nur Link Preview' }
] as const;

const oldIconOptions = ['Statt', 'Vorher', 'Alt'];
const newIconOptions = ['Jetzt', 'Deal', 'Neu'];
const amazonScrapeApiUrl = `${import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000'}/api/amazon/scrape`;
const dealsCheckApiUrl = `${import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000'}/api/deals/check`;
const directPublishApiUrl = `${import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000'}/api/posts/direct`;
const COUPON_PREVIEW_PREFIX = COUPON_LINE_PREFIX;
const BLITZANGEBOT_LABEL = '\u26A1\uFE0F Blitzangebot';
const ZEITLICH_BEGRENZT_LABEL = '\u23F0\uFE0F Zeitlich begrenztes Angebot';

function parseEnabledFlag(value: unknown) {
  return value === true || value === 1 || value === '1';
}

function parseBlockedFlag(value: unknown) {
  return value === true || value === 1 || value === '1';
}

function formatDealDateTime(value: string) {
  if (!value) {
    return null;
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  const day = String(parsed.getDate()).padStart(2, '0');
  const month = String(parsed.getMonth() + 1).padStart(2, '0');
  const year = parsed.getFullYear();
  const hours = String(parsed.getHours()).padStart(2, '0');
  const minutes = String(parsed.getMinutes()).padStart(2, '0');

  return `${day}.${month}.${year} ${hours}:${minutes}`;
}

function parseDealPriceValue(value: unknown) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  return null;
}

function formatPriceRangeValue(value: number | null) {
  if (value === null || Number.isNaN(value)) {
    return null;
  }

  return new Intl.NumberFormat('de-DE', {
    style: 'currency',
    currency: 'EUR'
  })
    .format(value)
    .replace(/\s/g, '');
}

function formatRemainingTime(value: number) {
  if (!value || value <= 0) {
    return '1 Minute';
  }

  const totalSeconds = Math.max(0, Math.floor(value));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const safeMinutes = hours === 0 ? Math.max(1, minutes) : minutes;
  const hourLabel = hours === 1 ? 'Stunde' : 'Stunden';
  const minuteLabel = safeMinutes === 1 ? 'Minute' : 'Minuten';

  if (hours > 0 && safeMinutes > 0) {
    return `${hours} ${hourLabel} ${safeMinutes} ${minuteLabel}`;
  }

  if (hours > 0) {
    return `${hours} ${hourLabel}`;
  }

  return `${safeMinutes} ${minuteLabel}`;
}

function GeneratorPosterPage() {
  const [amazonLink, setAmazonLink] = useState('');
  const [advertising, setAdvertising] = useState(false);
  const [selectedPrimaryOptions, setSelectedPrimaryOptions] = useState<string[]>([]);
  const [expandedAdvanced, setExpandedAdvanced] = useState(false);
  const [selectedExtras, setSelectedExtras] = useState<string[]>([]);
  const [showOldPrice, setShowOldPrice] = useState(false);
  const [oldPrice, setOldPrice] = useState('');
  const [currentPrice, setCurrentPrice] = useState('');
  const [oldIcon, setOldIcon] = useState(oldIconOptions[1]);
  const [newIcon, setNewIcon] = useState(newIconOptions[0]);
  const [extraText, setExtraText] = useState(NORMALIZED_DEFAULT_FREE_TEXT);
  const [publishing, setPublishing] = useState(false);
  const [scraping, setScraping] = useState(false);
  const [hasScraped, setHasScraped] = useState(false);
  const [scrapedImageUrl, setScrapedImageUrl] = useState('');
  const [uploadedImageDataUrl, setUploadedImageDataUrl] = useState('');
  const [uploadedImageName, setUploadedImageName] = useState('');
  const [telegramImageSource, setTelegramImageSource] = useState<'standard' | 'upload' | 'none'>('standard');
  const [whatsappImageSource, setWhatsappImageSource] = useState<'standard' | 'upload' | 'none'>('standard');
  const [facebookImageSource, setFacebookImageSource] = useState<'standard' | 'upload' | 'none' | 'link_preview'>(
    'link_preview'
  );
  const [telegramEnabled, setTelegramEnabled] = useState(true);
  const [whatsappEnabled, setWhatsappEnabled] = useState(false);
  const [facebookEnabled, setFacebookEnabled] = useState(false);
  const [scrapedTitle, setScrapedTitle] = useState('');
  const [rabattgutscheinCode, setRabattgutscheinCode] = useState('');
  const [formError, setFormError] = useState('');
  const [dealSnapshot, setDealSnapshot] = useState<{
    asin: string;
    finalUrl: string;
    normalizedUrl: string;
    sellerType: string;
    lastPostedAt: string;
    minPrice: number | null;
    maxPrice: number | null;
    postingCount: number;
    blocked: boolean;
    remainingMs: number;
    cooldownEnabled: boolean;
    cooldownHours: number;
  } | null>(null);
  const resetTimeoutRef = useRef<number | null>(null);
  const { toast, showToast } = useToast();
  const rabattgutscheinAktiv = selectedExtras.includes(COUPON_OPTION_LABEL);

  useEffect(() => {
    return () => {
      if (resetTimeoutRef.current !== null) {
        window.clearTimeout(resetTimeoutRef.current);
      }
    };
  }, []);

  const combinedExtraText = useMemo(() => extraText.trim(), [extraText]);

  const generatedPost = useMemo(
    () =>
      generatePostText({
        productTitle: scrapedTitle,
        freiText: combinedExtraText,
        textBaustein: selectedPrimaryOptions,
        alterPreis: showOldPrice ? oldPrice : '',
        neuerPreis: currentPrice,
        alterPreisLabel: showOldPrice ? oldIcon : '',
        neuerPreisLabel: newIcon,
        amazonLink,
        werbung: advertising,
        extraOptions: selectedExtras,
        rabattgutscheinCode: rabattgutscheinAktiv ? rabattgutscheinCode : ''
      }),
    [
      scrapedTitle,
      combinedExtraText,
      selectedPrimaryOptions,
      showOldPrice,
      oldPrice,
      currentPrice,
      amazonLink,
      advertising,
      selectedExtras,
      rabattgutscheinAktiv,
      rabattgutscheinCode
    ]
  );

  const previewMainPost = useMemo(() => {
    if (!rabattgutscheinAktiv || !rabattgutscheinCode.trim()) {
      return generatedPost.telegramCaption;
    }

    return generatedPost.telegramCaption.replace(`\n🏷️ Rabattgutschein: ${rabattgutscheinCode.trim()}`, '');
  }, [generatedPost, rabattgutscheinAktiv, rabattgutscheinCode]);

  const previewDisplayPost = useMemo(
    () =>
      generatedPost.telegramCaption
        .split('\n')
        .filter((line) => !line.includes(`Rabattgutschein: ${rabattgutscheinCode.trim()}`))
        .join('\n'),
    [generatedPost, rabattgutscheinCode]
  );

  const couponFieldError = rabattgutscheinAktiv && formError === 'Rabattgutschein fehlt.' ? formError : '';

  const validateRabattgutscheinCode = () => {
    if (rabattgutscheinAktiv && !rabattgutscheinCode.trim()) {
      return 'Rabattgutschein fehlt.';
    }

    if (!telegramEnabled && !whatsappEnabled && !facebookEnabled) {
      return 'Mindestens ein Kanal muss aktiviert sein.';
    }

    return '';
  };

  const validateBeforePublish = () => {
    const couponError = validateRabattgutscheinCode();
    if (couponError) {
      return couponError;
    }

    if (!hasEffectivePostQualifier(selectedPrimaryOptions, extraText, rabattgutscheinAktiv, rabattgutscheinCode)) {
      return "Keine Option angewählt. Wähle 'Ohne Optionen' oder schreibe einen Freitext.";
    }

    return '';
  };

  const resetGeneratorState = () => {
    setAmazonLink('');
    setAdvertising(false);
    setSelectedPrimaryOptions([]);
    setExpandedAdvanced(false);
    setSelectedExtras([]);
    setShowOldPrice(false);
    setOldPrice('');
    setCurrentPrice('');
    setOldIcon(oldIconOptions[1]);
    setNewIcon(newIconOptions[0]);
    setExtraText(NORMALIZED_DEFAULT_FREE_TEXT);
    setHasScraped(false);
    setScrapedImageUrl('');
    setUploadedImageDataUrl('');
    setUploadedImageName('');
    setTelegramImageSource('standard');
    setWhatsappImageSource('standard');
    setFacebookImageSource('link_preview');
    setTelegramEnabled(true);
    setWhatsappEnabled(false);
    setFacebookEnabled(false);
    setScrapedTitle('');
    setRabattgutscheinCode('');
    setFormError('');
    setDealSnapshot(null);
  };

  const handleTogglePrimaryOption = (option: string) => {
    setSelectedPrimaryOptions((prev) => {
      if (option === WITHOUT_OPTIONS_LABEL) {
        return prev.includes(WITHOUT_OPTIONS_LABEL) ? [] : [WITHOUT_OPTIONS_LABEL];
      }

      const withoutDefault = prev.filter((item) => item !== WITHOUT_OPTIONS_LABEL);
      const isActive = withoutDefault.includes(option);
      let next = isActive ? withoutDefault.filter((item) => item !== option) : [...withoutDefault, option];

      if (!isActive && option === BLITZANGEBOT_LABEL && !next.includes(ZEITLICH_BEGRENZT_LABEL)) {
        next = [...next, ZEITLICH_BEGRENZT_LABEL];
      }

      return next;
    });
  };

  const handleScrape = async () => {
    if (scraping) {
      console.log('EARLY RETURN REASON', {
        blocked: dealSnapshot?.blocked ?? null,
        lastPostedAt: dealSnapshot?.lastPostedAt ?? null,
        remainingSeconds: dealSnapshot?.remainingMs ? Math.ceil(dealSnapshot.remainingMs / 1000) : 0,
        reason: 'already_scraping'
      });
      return;
    }

    const finalAmazonLink = (amazonLink || '').trim();
    console.log('SCRAPE BUTTON CLICK');
    console.log('SCRAPE START', finalAmazonLink);

    if (!finalAmazonLink) {
      setHasScraped(false);
      setScrapedImageUrl('');
      setScrapedTitle('');
      setFormError('Link vergessen.');
      setDealSnapshot(null);
      showToast('Link vergessen.');
      console.log('EARLY RETURN REASON', {
        blocked: null,
        lastPostedAt: null,
        remainingSeconds: 0,
        reason: 'missing_link'
      });
      return;
    }

    setScraping(true);
    setFormError('');
    setDealSnapshot(null);

    try {
      console.log('SCRAPE REQUEST START');
      const response = await fetch(amazonScrapeApiUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ url: finalAmazonLink })
      });
      console.log('SCRAPE RESPONSE STATUS', response.status);

      const rawResponse = await response.text();
      let data: {
        success?: boolean;
        error?: string;
        code?: string;
        message?: string;
        image?: string;
        title?: string;
        price?: string;
        oldPrice?: string;
        link?: string;
        asin?: string;
        finalUrl?: string;
        normalizedUrl?: string;
        sellerType?: string;
      } = {};

      try {
        data = rawResponse ? JSON.parse(rawResponse) : {};
      } catch {
        data = { error: rawResponse || 'Unbekannte Scrape-Antwort' };
      }

      console.log('SCRAPE RESULT', {
        ok: response.ok,
        status: response.status,
        data
      });
      console.log('SCRAPE RESPONSE', data);
      console.log('SCRAPE RESPONSE DATA', data);

      if (!response.ok) {
        setHasScraped(false);
        setScrapedImageUrl('');
        setScrapedTitle('');
        setFormError('');
        setDealSnapshot(null);
        showToast(
          data.error ||
            data.message ||
            data.code ||
            `Scrape fehlgeschlagen (${response.status}). Bitte Backend pruefen.`
        );
        console.log('EARLY RETURN REASON', {
          blocked: null,
          lastPostedAt: null,
          remainingSeconds: 0,
          reason: 'scrape_request_failed'
        });
        return;
      }

      const checkPayload = {
        asin: data.asin || '',
        url: data.finalUrl || data.normalizedUrl || finalAmazonLink,
        normalizedUrl: data.normalizedUrl || ''
      };
      console.log('CHECK PAYLOAD', checkPayload);

      const checkResponse = await fetch(dealsCheckApiUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(checkPayload)
      });

      const checkRawResponse = await checkResponse.text();
      let checkData: {
        success?: boolean;
        error?: string;
        asin?: string;
        resolvedFinalUrl?: string | null;
        normalizedUrl?: string;
        blocked?: boolean;
        remainingSeconds?: number | null;
        repostCooldownEnabled?: boolean;
        repostCooldownHours?: number;
        lastPostedAt?: string | null;
        minPrice?: number | null;
        maxPrice?: number | null;
        postingCount?: number;
        sellerType?: string | null;
        lastDeal?: {
          postedAt?: string;
          sellerType?: string;
          title?: string;
          price?: string;
        } | null;
      } = {};

      try {
        checkData = checkRawResponse ? JSON.parse(checkRawResponse) : {};
      } catch {
        checkData = { error: checkRawResponse || 'Unbekannte Deal-Check-Antwort' };
      }

      console.log('CHECK RESULT', {
        status: checkResponse.status,
        data: checkData
      });
      console.log('CHECK RESPONSE', checkData);
      console.log('CHECK RESULT', checkData);

      if (!checkResponse.ok) {
        setHasScraped(false);
        setScrapedImageUrl('');
        setScrapedTitle('');
        setFormError('');
        setDealSnapshot(null);
        showToast(checkData.error || 'Deal-Check fehlgeschlagen.');
        console.log('EARLY RETURN REASON', {
          blocked: checkData?.blocked ?? null,
          lastPostedAt: checkData?.lastPostedAt || checkData?.lastDeal?.postedAt || null,
          remainingSeconds: checkData?.remainingSeconds ?? 0,
          reason: 'deal_check_failed'
        });
        return;
      }

      const isKnownDeal = Boolean(checkData.lastPostedAt || checkData.lastDeal?.postedAt);
      console.log('KNOWN DEAL?', isKnownDeal);

      const nextDealSnapshot = {
        asin: checkData.asin || data.asin || '',
        finalUrl: data.finalUrl || checkData.resolvedFinalUrl || data.normalizedUrl || '',
        normalizedUrl: data.normalizedUrl || checkData.normalizedUrl || '',
        sellerType: data.sellerType || checkData.sellerType || checkData.lastDeal?.sellerType || '',
        lastPostedAt: checkData.lastPostedAt || checkData.lastDeal?.postedAt || '',
        minPrice: parseDealPriceValue(checkData.minPrice),
        maxPrice: parseDealPriceValue(checkData.maxPrice),
        postingCount: Number(checkData.postingCount || 0),
        blocked: checkData.blocked === true,
        remainingMs: Number(checkData.remainingSeconds || 0) * 1000,
        cooldownEnabled: parseEnabledFlag(checkData.repostCooldownEnabled),
        cooldownHours: Number(checkData.repostCooldownHours ?? 12)
      };

      setDealSnapshot(nextDealSnapshot);
      console.log('GENERATOR HISTORY META STATE', {
        blocked: nextDealSnapshot.blocked,
        lastPostedAt: nextDealSnapshot.lastPostedAt,
        minPrice: nextDealSnapshot.minPrice,
        maxPrice: nextDealSnapshot.maxPrice,
        repostCooldownEnabled: nextDealSnapshot.cooldownEnabled
      });

      const normalizedImageUrl = normalizeDealImageUrl(data.image || '');
      setScrapedImageUrl(normalizedImageUrl);
      setScrapedTitle(data.title || '');
      setOldPrice(data.oldPrice || '');
      setCurrentPrice(data.price || '');
      setSelectedPrimaryOptions([]);

      if (checkData.blocked === true) {
        const formattedBlockMessage = `Link bereits gepostet. Erneut möglich in ${formatRemainingTime(
          Number(checkData.remainingSeconds || 0)
        )}.`;
        console.log('FORMATTED BLOCK MESSAGE', formattedBlockMessage);
        setFormError(formattedBlockMessage);
        setHasScraped(false);
        console.log('EARLY RETURN REASON', {
          blocked: true,
          lastPostedAt: checkData.lastPostedAt || checkData.lastDeal?.postedAt || null,
          remainingSeconds: checkData.remainingSeconds ?? 0,
          reason: 'deal_blocked'
        });
        return;
      }

      console.log('FORMATTED BLOCK MESSAGE', '');
      setFormError('');
      setHasScraped(true);
      showToast(
        normalizedImageUrl
          ? 'Amazon Link erfolgreich gescrapt und Produktbild geladen'
          : 'Amazon Link erfolgreich gescrapt, aber ohne Produktbild'
      );

      if (!data.title) {
        showToast('Produkttitel konnte nicht gelesen werden. Fallback wird verwendet.', 2600);
      }
    } catch (error) {
      const finalError =
        error instanceof Error ? error.message : 'Unbekannter Scrape-Fehler';
      console.error('final error', error);
      setHasScraped(false);
      setScrapedImageUrl('');
      setScrapedTitle('');
      setFormError(`Scrape fehlgeschlagen: ${finalError}`);
      showToast(`Scrape fehlgeschlagen: ${finalError}`);
    } finally {
      setScraping(false);
    }
  };

  const handlePublish = async () => {
    if (publishing) return;

    if (!hasScraped) {
      showToast('Bitte zuerst erfolgreich den Amazon Link scrapen');
      return;
    }

    const validationError = validateBeforePublish();
    if (validationError) {
      setFormError(validationError);
      showToast(validationError);
      return;
    }

    if (!generatedPost.telegramCaption || generatedPost.telegramCaption.trim().length === 0) {
      showToast('Bitte fuellen Sie das Formular aus');
      return;
    }

    setFormError('');
    setPublishing(true);

    try {
      const response = await fetch(directPublishApiUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          title: scrapedTitle || generatedPost.productTitle,
          link: amazonLink,
          normalizedUrl: dealSnapshot?.normalizedUrl || '',
          asin: dealSnapshot?.asin || '',
          sellerType: dealSnapshot?.sellerType || 'FBM',
          currentPrice,
          oldPrice: showOldPrice ? oldPrice : '',
          couponCode: rabattgutscheinAktiv ? rabattgutscheinCode.trim() : '',
          textByChannel: {
            telegram: generatedPost.telegramCaption,
            whatsapp: generatedPost.whatsappText,
            facebook: previewMainPost
          },
          generatedImagePath: scrapedImageUrl || '',
          uploadedImagePath: uploadedImageDataUrl || '',
          telegramImageSource,
          whatsappImageSource,
          facebookImageSource,
          enableTelegram: telegramEnabled,
          enableWhatsapp: whatsappEnabled,
          enableFacebook: facebookEnabled
        })
      });

      const rawResponse = await response.text();
      let data: { success?: boolean; message?: string; error?: string; code?: string } = {};

      try {
        data = rawResponse ? JSON.parse(rawResponse) : {};
      } catch {
        data = { error: rawResponse || 'Unbekannte Backend-Antwort' };
      }

      if (!response.ok) {
        const backendMessage = data.error || data.message || `Backend-Fehler (${response.status}) beim Direkt-Posting`;
        setFormError(backendMessage);
        showToast(backendMessage);
        return;
      }

      showToast(
        whatsappEnabled || facebookEnabled
          ? 'Generator-Post direkt gesendet. Weitere Kanaele sind fuer spaetere Direkt-Implementierung vorbereitet.'
          : 'Generator-Post sofort zu Telegram gesendet',
        SUCCESS_RESET_DELAY_MS
      );
      resetTimeoutRef.current = window.setTimeout(() => {
        resetGeneratorState();
        resetTimeoutRef.current = null;
      }, SUCCESS_RESET_DELAY_MS);
    } catch (error) {
      console.error('Telegram send error:', error);
      const message =
        error instanceof Error
          ? `Telegram-Verbindungsfehler: ${error.message}`
          : 'Telegram-Verbindungsfehler';

      setFormError(message);
      showToast(message);
    } finally {
      setPublishing(false);
    }
  };

  const handleToggleExtra = (option: string) => {
    setSelectedExtras((prev) => {
      const next = prev.includes(option) ? prev.filter((item) => item !== option) : [...prev, option];
      if (!next.includes(COUPON_OPTION_LABEL)) {
        setRabattgutscheinCode('');
        setFormError('');
      }
      return next;
    });
  };

  const handleToggleOldPrice = (checked: boolean) => {
    setShowOldPrice(checked);
    if (!checked) {
      setOldPrice('');
      setOldIcon(oldIconOptions[1]);
    }
  };

  const handleUploadImage = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) {
      setUploadedImageDataUrl('');
      setUploadedImageName('');
      return;
    }

    const reader = new FileReader();
    reader.onload = () => {
      setUploadedImageDataUrl(typeof reader.result === 'string' ? reader.result : '');
      setUploadedImageName(file.name);
    };
    reader.readAsDataURL(file);
  };

  const formattedLastPostedAt = dealSnapshot ? formatDealDateTime(dealSnapshot.lastPostedAt) : null;
  const formattedMinPrice = dealSnapshot ? formatPriceRangeValue(dealSnapshot.minPrice) : null;
  const formattedMaxPrice = dealSnapshot ? formatPriceRangeValue(dealSnapshot.maxPrice) : null;
  const shouldShowHistoryMeta =
    Boolean(dealSnapshot) &&
    !dealSnapshot?.blocked &&
    Boolean(formattedLastPostedAt) &&
    Boolean(formattedMinPrice) &&
    Boolean(formattedMaxPrice);
  console.log('GENERATOR SHOW HISTORY META', shouldShowHistoryMeta);
  console.log('SHOW HISTORY META?', {
    blocked: dealSnapshot?.blocked ?? null,
    historyLastPostedAt: dealSnapshot?.lastPostedAt ?? null,
    historyMinPrice: dealSnapshot?.minPrice ?? null,
    historyMaxPrice: dealSnapshot?.maxPrice ?? null,
    showHistoryMeta: shouldShowHistoryMeta
  });
  console.log('FORMATTED HISTORY META', {
    formattedDate: formattedLastPostedAt,
    formattedMinPrice,
    formattedMaxPrice
  });
  return (
    <Layout showSidebar>
      <div className="generator-desktop-page">
        <div className="generator-desktop-shell">
          <section className="generator-content-header">
            <p className="generator-eyebrow">AFFILIATE MANAGER</p>
          </section>

          <section className="generator-panel generator-intro-panel">
            <div className="generator-panel-header">
              <h1>Generator Poster</h1>
            </div>

            <div className="generator-base-fields">
              <label className="generator-form-field">
                <span>Amazon Link</span>
                <input
                  type="text"
                  value={amazonLink}
                  onChange={(e) => {
                    setAmazonLink(e.target.value);
                    setHasScraped(false);
                    setScrapedImageUrl('');
                    setScrapedTitle('');
                    setSelectedPrimaryOptions([]);
                    setFormError('');
                    setDealSnapshot(null);
                  }}
                  placeholder="https://amazon.de/..."
                />
              </label>

              <div className="generator-intro-actions">
                <button
                  type="button"
                  className="generator-action-button secondary"
                  onClick={handleScrape}
                  disabled={scraping}
                >
                  {scraping ? 'Amazon Link wird gescrapt...' : 'Scrap Amazon Link'}
                </button>

                <label className="generator-checkbox-row">
                  <input
                    type="checkbox"
                    checked={advertising}
                    onChange={(e) => setAdvertising(e.target.checked)}
                  />
                  <span>Werbung</span>
                </label>
              </div>

              {formError === 'Link vergessen.' && <p className="generator-form-error">{formError}</p>}

              {dealSnapshot?.blocked && (
                <p className="generator-history-alert">
                  Link bereits gepostet. Erneut möglich in {formatRemainingTime(dealSnapshot.remainingMs / 1000)}.
                </p>
              )}

              {shouldShowHistoryMeta && (
                <div className="generator-history-inline">
                  <p>Zuletzt gepostet am: {formattedLastPostedAt}</p>
                  <p>Preisspanne 6 Monate: {formattedMinPrice} - {formattedMaxPrice}</p>
                </div>
              )}
            </div>
          </section>

          {hasScraped && (
            <>
              <section className="generator-panel">
                <div className="generator-panel-header">
                  <h2>Textbausteine</h2>
                </div>

                <div className="generator-vertical-list">
                  <label className="generator-list-row">
                    <input
                      type="checkbox"
                      checked={selectedPrimaryOptions.includes(WITHOUT_OPTIONS_LABEL)}
                      onChange={() => handleTogglePrimaryOption(WITHOUT_OPTIONS_LABEL)}
                    />
                    <span>{WITHOUT_OPTIONS_LABEL}</span>
                  </label>
                  {textOptions.map((option) => (
                    <label key={option.value} className="generator-list-row">
                      <input
                        type="checkbox"
                        checked={selectedPrimaryOptions.includes(option.value)}
                        onChange={() => handleTogglePrimaryOption(option.value)}
                      />
                      <span>{option.label}</span>
                    </label>
                  ))}
                </div>
              </section>

              <section className="generator-panel">
                <div className="generator-panel-header generator-panel-header-inline">
                  <div className="generator-panel-header-inline-title">
                    <h2>Erweiterte Ansicht</h2>
                    <button
                      type="button"
                      className="generator-action-button secondary compact"
                      onClick={() => setExpandedAdvanced((prev) => !prev)}
                    >
                      {expandedAdvanced ? 'Ausblenden' : 'Einblenden'}
                    </button>
                  </div>
                </div>

                {expandedAdvanced && (
                  <>
                    <div className="generator-vertical-list">
                      {extraOptions.map((option) => (
                        <label key={option} className="generator-list-row">
                          <input
                            type="checkbox"
                            checked={selectedExtras.includes(option)}
                            onChange={() => handleToggleExtra(option)}
                          />
                          <span>{option}</span>
                        </label>
                      ))}
                    </div>

                    {rabattgutscheinAktiv && (
                      <div className="generator-inline-field">
                        <label className="generator-form-field">
                          <span>Rabattgutschein eingeben</span>
                          <input
                            className={couponFieldError ? 'generator-coupon-input has-error' : 'generator-coupon-input'}
                            type="text"
                            value={rabattgutscheinCode}
                            onChange={(e) => {
                              setRabattgutscheinCode(e.target.value);
                              setFormError('');
                            }}
                            placeholder="z. B. YE67K4BD"
                          />
                        </label>
                        <p className="generator-field-hint">
                          Pflichtfeld. Wird im Hauptbeitrag angezeigt und danach als zweite Nachricht gesendet.
                        </p>
                        {couponFieldError && <p className="generator-form-error">{couponFieldError}</p>}
                      </div>
                    )}
                  </>
                )}
              </section>

              <section className="generator-panel">
                <div className="generator-panel-header">
                  <h2>Preise</h2>
                </div>

                <div className="generator-price-panel">
                  <label className="generator-checkbox-row">
                    <input
                      type="checkbox"
                      checked={showOldPrice}
                      onChange={(e) => handleToggleOldPrice(e.target.checked)}
                    />
                    <span>Stattpreis anzeigen</span>
                  </label>

                  <div className={`generator-price-grid${showOldPrice ? ' has-old-price' : ' single-price'}`}>
                    {showOldPrice && (
                      <div className="generator-column-fields">
                        <label className="generator-form-field">
                          <span>Icon Alt</span>
                          <select value={oldIcon} onChange={(e) => setOldIcon(e.target.value)}>
                            {oldIconOptions.map((option) => (
                              <option key={option} value={option}>
                                {option}
                              </option>
                            ))}
                          </select>
                        </label>

                        <label className="generator-form-field">
                          <span>Alter Preis</span>
                          <input
                            type="text"
                            value={oldPrice}
                            onChange={(e) => setOldPrice(e.target.value)}
                            placeholder="39,99 EUR"
                          />
                        </label>
                      </div>
                    )}

                    <div className="generator-column-fields">
                      <label className="generator-form-field">
                        <span>Icon Neu</span>
                        <select value={newIcon} onChange={(e) => setNewIcon(e.target.value)}>
                          {newIconOptions.map((option) => (
                            <option key={option} value={option}>
                              {option}
                            </option>
                          ))}
                        </select>
                      </label>

                      <label className="generator-form-field">
                        <span>Aktueller Preis</span>
                        <input
                          type="text"
                          value={currentPrice}
                          onChange={(e) => setCurrentPrice(e.target.value)}
                          placeholder="24,90 EUR"
                        />
                      </label>
                    </div>
                  </div>

                  {!showOldPrice && <p className="generator-field-hint">Standardmaessig wird nur der Jetzt-Preis verwendet.</p>}
                </div>
              </section>

              <section className="generator-panel">
                <div className="generator-panel-header">
                  <h2>Freitext</h2>
                </div>

                <label className="generator-form-field">
                  <span>Zusatztext</span>
                  <textarea
                    value={extraText}
                    onChange={(e) => {
                      setExtraText(e.target.value);
                      setFormError('');
                    }}
                    rows={7}
                  />
                </label>
              </section>

              <section className="generator-panel">
                <div className="generator-panel-header">
                  <h2>Bildquellen pro Kanal</h2>
                </div>

                <div style={{ display: 'grid', gap: '1rem' }}>
                  <label className="generator-form-field">
                    <span>Eigener Upload / Screenshot</span>
                    <input type="file" accept="image/*" onChange={handleUploadImage} />
                  </label>
                  <p className="generator-field-hint">
                    Keine automatische Screenshot-Erstellung. Generator-Posts werden direkt versendet; nur Copybot-Deals laufen ueber Queue und Worker.
                  </p>
                  {(scrapedImageUrl || uploadedImageDataUrl) && (
                    <div className="split-row">
                      <div className="card" style={{ padding: '0.85rem' }}>
                        <p className="section-title">Standardbild</p>
                        <strong>{scrapedImageUrl ? 'vorhanden' : 'nicht vorhanden'}</strong>
                      </div>
                      <div className="card" style={{ padding: '0.85rem' }}>
                        <p className="section-title">Eigener Upload</p>
                        <strong>{uploadedImageName || 'nicht vorhanden'}</strong>
                      </div>
                    </div>
                  )}
                  <div className="form-row">
                    <label className="checkbox-card">
                      <span>Telegram aktiv</span>
                      <input type="checkbox" checked={telegramEnabled} onChange={(e) => setTelegramEnabled(e.target.checked)} />
                    </label>
                    <label className="checkbox-card">
                      <span>WhatsApp aktiv</span>
                      <input type="checkbox" checked={whatsappEnabled} onChange={(e) => setWhatsappEnabled(e.target.checked)} />
                    </label>
                    <label className="checkbox-card">
                      <span>Facebook aktiv</span>
                      <input type="checkbox" checked={facebookEnabled} onChange={(e) => setFacebookEnabled(e.target.checked)} />
                    </label>
                  </div>
                  <div className="form-row">
                    <label className="generator-form-field">
                      <span>Telegram Bildquelle</span>
                      <select value={telegramImageSource} onChange={(e) => setTelegramImageSource(e.target.value as 'standard' | 'upload' | 'none')}>
                        {imageSourceOptions.map((option) => (
                          <option key={option.value} value={option.value}>{option.label}</option>
                        ))}
                      </select>
                    </label>
                    <label className="generator-form-field">
                      <span>WhatsApp Bildquelle</span>
                      <select value={whatsappImageSource} onChange={(e) => setWhatsappImageSource(e.target.value as 'standard' | 'upload' | 'none')}>
                        {imageSourceOptions.map((option) => (
                          <option key={option.value} value={option.value}>{option.label}</option>
                        ))}
                      </select>
                    </label>
                    <label className="generator-form-field">
                      <span>Facebook Bildquelle</span>
                      <select value={facebookImageSource} onChange={(e) => setFacebookImageSource(e.target.value as 'standard' | 'upload' | 'none' | 'link_preview')}>
                        {facebookImageSourceOptions.map((option) => (
                          <option key={option.value} value={option.value}>{option.label}</option>
                        ))}
                      </select>
                    </label>
                  </div>
                </div>
              </section>

              <section className="generator-panel">
                <div className="generator-panel-header">
                  <h2>Hauptbeitrag</h2>
                </div>

                <div className="generator-post-preview">
                  <pre>{previewDisplayPost}</pre>
                  {rabattgutscheinAktiv && (
                    <div className={`generator-coupon-preview${!rabattgutscheinCode.trim() ? ' is-empty' : ''}`}>
                      <span>Rabattgutschein</span>
                      <strong>{`${COUPON_PREVIEW_PREFIX} ${rabattgutscheinCode.trim() || 'Rabattgutschein fehlt.'}`}</strong>
                      {!rabattgutscheinCode.trim() && <em>Pflichtfeld vor dem Versand</em>}
                    </div>
                  )}
                </div>
              </section>

              <section className="generator-panel generator-submit-panel">
                {formError && formError !== 'Rabattgutschein fehlt.' && <p className="generator-form-error">{formError}</p>}
                <button
                  type="button"
                  className="generator-action-button primary"
                  onClick={handlePublish}
                  disabled={publishing}
                >
                  {publishing ? 'Wird direkt gesendet...' : 'Sofort posten'}
                </button>
              </section>
            </>
          )}
        </div>

        {toast && <Toast message={toast.message} duration={toast.duration} />}
      </div>
    </Layout>
  );
}

export default GeneratorPosterPage;
