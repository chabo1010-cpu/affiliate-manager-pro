import { useEffect, useMemo, useState } from 'react';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';
import './DealEngine.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';
const DEAL_ENGINE_LOAD_TIMEOUT_MS = 2200;
const DEAL_ENGINE_REQUEST_TIMEOUT_MS = 5000;
const UI_ROUTE_SLOW_THRESHOLD_MS = 800;

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatDateTime(value) {
  if (!value) {
    return '-';
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat('de-DE', {
    dateStyle: 'short',
    timeStyle: 'short'
  }).format(date);
}

function prettyJson(value) {
  return JSON.stringify(value ?? null, null, 2);
}

function toSellerFlagValue(value) {
  if (value === true) {
    return 'yes';
  }

  if (value === false) {
    return 'no';
  }

  return 'unknown';
}

function fromSellerFlagValue(value) {
  if (value === 'yes') {
    return true;
  }

  if (value === 'no') {
    return false;
  }

  return null;
}

function buildFormFromSample(sample = {}) {
  return {
    sourceName: sample.source?.name || 'Demo Quelle',
    sourcePlatform: sample.source?.platform || 'telegram',
    sourceType: sample.source?.type || 'manual',
    title: sample.deal?.title || '',
    amazonUrl: sample.deal?.amazonUrl || '',
    amazonPrice: String(sample.deal?.amazonPrice ?? ''),
    sellerType: sample.deal?.sellerType || 'AMAZON',
    sellerClass: sample.deal?.sellerClass || 'AMAZON_DIRECT',
    soldByAmazon: toSellerFlagValue(sample.deal?.soldByAmazon ?? true),
    shippedByAmazon: toSellerFlagValue(sample.deal?.shippedByAmazon ?? true),
    brand: sample.deal?.brand || '',
    category: sample.deal?.category || '',
    rating: String(sample.deal?.rating ?? ''),
    reviewCount: String(sample.deal?.reviewCount ?? ''),
    variantKey: sample.deal?.variantKey || '',
    quantityKey: sample.deal?.quantityKey || '',
    isBrandProduct: sample.deal?.isBrandProduct === true,
    isNoName: sample.deal?.isNoName === true,
    isChinaProduct: sample.deal?.isChinaProduct === true,
    overrideDayPart: sample.meta?.overrideDayPart || 'day',
    marketOffersJson: prettyJson(sample.market?.offers || []),
    keepaJson: prettyJson(sample.keepa || {}),
    aiJson: prettyJson(sample.ai || {})
  };
}

function buildAnalyzePayload(form) {
  const marketOffers = JSON.parse(form.marketOffersJson || '[]');
  const keepa = JSON.parse(form.keepaJson || '{}');
  const ai = JSON.parse(form.aiJson || '{}');

  if (!Array.isArray(marketOffers)) {
    throw new Error('Marktangebote muessen als JSON-Array angegeben werden.');
  }

  return {
    source: {
      name: form.sourceName,
      platform: form.sourcePlatform,
      type: form.sourceType
    },
    deal: {
      title: form.title,
      amazonUrl: form.amazonUrl,
      amazonPrice: form.amazonPrice,
      sellerType: form.sellerType,
      sellerClass: form.sellerClass,
      soldByAmazon: fromSellerFlagValue(form.soldByAmazon),
      shippedByAmazon: fromSellerFlagValue(form.shippedByAmazon),
      brand: form.brand,
      category: form.category,
      rating: form.rating,
      reviewCount: form.reviewCount,
      variantKey: form.variantKey,
      quantityKey: form.quantityKey,
      isBrandProduct: form.isBrandProduct,
      isNoName: form.isNoName,
      isChinaProduct: form.isChinaProduct
    },
    market: {
      offers: marketOffers
    },
    keepa,
    ai,
    meta: {
      overrideDayPart: form.overrideDayPart
    }
  };
}

function buildProductRuleForm(rule = {}) {
  return {
    id: rule?.id || null,
    name: rule?.name || '',
    keywords: Array.isArray(rule?.keywords) ? rule.keywords.join(', ') : '',
    brandType: rule?.brandType || 'ANY',
    maxPrice: rule?.maxPrice ?? '',
    minReviews: rule?.minReviews ?? 0,
    minRating: rule?.minRating ?? 0,
    marketCompareRequired: rule?.marketCompareRequired === true,
    capacityMin: rule?.capacityMin ?? '',
    capacityMax: rule?.capacityMax ?? '',
    active: rule?.active !== false
  };
}

function getToneClass(value) {
  const normalized = String(value || '').toLowerCase();
  if (normalized.includes('reject') || normalized.includes('danger') || normalized.includes('failed') || normalized.includes('attention')) {
    return 'danger';
  }

  if (normalized.includes('queue') || normalized.includes('warning') || normalized.includes('pending')) {
    return 'warning';
  }

  if (normalized.includes('approve') || normalized.includes('success') || normalized.includes('ready') || normalized.includes('active')) {
    return 'success';
  }

  return 'info';
}

const DEAL_ENGINE_DEFAULTS = {
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

function mergeDealEngineSettings(current, patch) {
  if (!current) {
    return current;
  }

  return {
    ...current,
    amazon: {
      ...current.amazon,
      ...(patch.amazon || {})
    },
    fbm: {
      ...current.fbm,
      ...(patch.fbm || {})
    },
    global: {
      ...current.global,
      ...(patch.global || {})
    },
    output: {
      ...current.output,
      ...(patch.output || {})
    },
    ai: {
      ...current.ai,
      ...(patch.ai || {})
    },
    quality: {
      ...current.quality,
      ...(patch.quality || {})
    }
  };
}

function applyQualityPreset(current, preset) {
  const patches = {
    locker: {
      global: {
        keepaApproveScore: 60,
        keepaQueueScore: 45,
        queueMarginPct: 6
      },
      ai: {
        resolverEnabled: true
      },
      quality: {
        marketCompareAmazonDirectOnly: false,
        aiAmazonDirectOnly: false,
        allowFbaThirdPartyMarketCompare: true,
        allowFbaThirdPartyAi: true,
        allowFbmMarketCompare: true,
        allowFbmAi: true,
        unknownSellerMode: 'review'
      }
    },
    normal: DEAL_ENGINE_DEFAULTS,
    streng: {
      global: {
        keepaApproveScore: 75,
        keepaQueueScore: 55,
        queueMarginPct: 2.5,
        cheapProductLimit: 25,
        requireMarketForCheapProducts: true,
        requireMarketForNoNameProducts: true
      },
      quality: {
        marketCompareAmazonDirectOnly: true,
        aiAmazonDirectOnly: true,
        allowFbaThirdPartyMarketCompare: false,
        allowFbaThirdPartyAi: false,
        allowFbmMarketCompare: false,
        allowFbmAi: false,
        unknownSellerMode: 'review'
      }
    },
    profi: {
      global: {
        keepaApproveScore: 80,
        keepaQueueScore: 60,
        queueMarginPct: 1.5,
        cheapProductLimit: 30,
        requireMarketForCheapProducts: true,
        requireMarketForNoNameProducts: true
      },
      ai: {
        resolverEnabled: true,
        onlyOnUncertainty: true,
        alwaysInDebug: false
      },
      quality: {
        marketCompareAmazonDirectEnabled: true,
        marketCompareAmazonDirectOnly: true,
        aiAmazonDirectOnly: true,
        allowFbaThirdPartyMarketCompare: false,
        allowFbaThirdPartyAi: false,
        allowFbmMarketCompare: false,
        allowFbmAi: false,
        unknownSellerMode: 'block'
      }
    }
  };

  return mergeDealEngineSettings(current, patches[preset] || DEAL_ENGINE_DEFAULTS);
}

function detectQualityPreset(settings) {
  if (!settings) {
    return 'normal';
  }

  if (settings.quality.unknownSellerMode === 'block' && settings.global.keepaApproveScore >= 80) {
    return 'profi';
  }

  if (settings.global.keepaApproveScore >= 75 || settings.global.queueMarginPct <= 2.5) {
    return 'streng';
  }

  if (settings.quality.allowFbmMarketCompare || settings.quality.allowFbaThirdPartyMarketCompare || settings.global.keepaApproveScore < 70) {
    return 'locker';
  }

  return 'normal';
}

function applyProductFilterPreset(current, preset) {
  const patches = {
    standard: {
      global: {
        cheapProductLimit: 20,
        requireMarketForCheapProducts: true,
        requireMarketForNoNameProducts: true
      }
    },
    streng: {
      global: {
        cheapProductLimit: 25,
        requireMarketForCheapProducts: true,
        requireMarketForNoNameProducts: true
      },
      quality: {
        allowFbaThirdPartyMarketCompare: false,
        allowFbmMarketCompare: false
      }
    },
    china_filter: {
      global: {
        cheapProductLimit: 30,
        requireMarketForCheapProducts: true,
        requireMarketForNoNameProducts: true
      },
      quality: {
        allowFbaThirdPartyMarketCompare: false,
        allowFbaThirdPartyAi: false,
        allowFbmMarketCompare: false,
        allowFbmAi: false
      }
    }
  };

  return mergeDealEngineSettings(current, patches[preset] || patches.standard);
}

function detectProductFilterPreset(settings) {
  if (!settings) {
    return 'standard';
  }

  if (settings.global.cheapProductLimit >= 30 && settings.quality.allowFbmAi === false && settings.quality.allowFbaThirdPartyAi === false) {
    return 'china_filter';
  }

  if (settings.global.cheapProductLimit >= 25) {
    return 'streng';
  }

  return 'standard';
}

function applyAutomationPreset(current, preset) {
  const patches = {
    testmodus: {
      output: {
        telegramEnabled: false,
        whatsappEnabled: false
      },
      ai: {
        alwaysInDebug: true
      }
    },
    review_sammeln: {
      output: {
        telegramEnabled: false,
        whatsappEnabled: false
      },
      global: {
        queueEnabled: true
      }
    },
    auto_posten: {
      output: {
        telegramEnabled: true,
        whatsappEnabled: true
      },
      global: {
        queueEnabled: true
      }
    }
  };

  return mergeDealEngineSettings(current, patches[preset] || patches.review_sammeln);
}

function detectAutomationPreset(settings) {
  if (!settings) {
    return 'review_sammeln';
  }

  if (settings.output.telegramEnabled && settings.output.whatsappEnabled) {
    return 'auto_posten';
  }

  if (!settings.output.telegramEnabled && settings.ai.alwaysInDebug) {
    return 'testmodus';
  }

  return 'review_sammeln';
}

function isWithinLastMinutes(value, minutes) {
  if (!value) {
    return false;
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return false;
  }

  return Date.now() - date.getTime() <= minutes * 60 * 1000;
}

function openInternalRoute(path) {
  window.location.assign(path);
}

function getTimedRequestMeta(startedAt, path) {
  const durationMs = Math.round(performance.now() - startedAt);
  const meta = { route: '/deal-engine', path, durationMs };
  console.info('[UI_ROUTE_DONE]', meta);
  if (durationMs >= UI_ROUTE_SLOW_THRESHOLD_MS) {
    console.warn('[UI_ROUTE_SLOW]', meta);
  }
  return meta;
}

const DEAL_ENGINE_TABS = [
  { id: 'overview', label: 'Uebersicht' },
  { id: 'rules', label: 'Produktregeln' },
  { id: 'variants', label: 'Variantencheck' },
  { id: 'safety', label: 'Seller & Sicherheit' },
  { id: 'routing', label: 'Routing' },
  { id: 'queue', label: 'Queue' },
  { id: 'logs', label: 'Logs' },
  { id: 'experts', label: 'Expertenmodus' }
];

function DealEnginePage() {
  const { user } = useAuth();
  const [dashboard, setDashboard] = useState(null);
  const [botDashboard, setBotDashboard] = useState(null);
  const [settings, setSettings] = useState(null);
  const [productRulesList, setProductRulesList] = useState([]);
  const [productRuleForm, setProductRuleForm] = useState(() => buildProductRuleForm({}));
  const [samplePayload, setSamplePayload] = useState(null);
  const [form, setForm] = useState(() => buildFormFromSample({}));
  const [result, setResult] = useState(null);
  const [status, setStatus] = useState('');
  const [loading, setLoading] = useState(true);
  const [secondaryLoading, setSecondaryLoading] = useState(false);
  const [activeTab, setActiveTab] = useState('overview');
  const [logFilter, setLogFilter] = useState('all');
  const [showAllRules, setShowAllRules] = useState(false);
  const [showAllLogs, setShowAllLogs] = useState(false);
  const [saving, setSaving] = useState(false);
  const [savingProductRule, setSavingProductRule] = useState(false);
  const [busyRuleId, setBusyRuleId] = useState(null);
  const [analyzing, setAnalyzing] = useState(false);
  const [expertMode, setExpertMode] = useState(false);

  async function apiFetch(path, options = {}, config = {}) {
    const timeoutMs = Number(config.timeoutMs || DEAL_ENGINE_REQUEST_TIMEOUT_MS);
    const controller = new AbortController();
    const startedAt = performance.now();
    const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);

    console.info('[UI_ROUTE_START]', {
      route: '/deal-engine',
      path,
      timeoutMs
    });

    try {
      const response = await fetch(`${API_BASE_URL}${path}`, {
        headers: {
          'Content-Type': 'application/json',
          'X-User-Role': user?.role || ''
        },
        signal: controller.signal,
        ...options
      });
      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(data?.error || `Request fehlgeschlagen (${response.status}).`);
      }

      getTimedRequestMeta(startedAt, path);
      return data;
    } catch (error) {
      const durationMs = Math.round(performance.now() - startedAt);
      const timedOut = error?.name === 'AbortError';
      if (timedOut) {
        console.warn('[DEAL_ENGINE_LOAD_TIMEOUT]', {
          route: '/deal-engine',
          path,
          timeoutMs,
          durationMs
        });
        throw new Error(`Timeout nach ${timeoutMs}ms: ${path}`);
      }

      console.error('[UI_ROUTE_ERROR]', {
        route: '/deal-engine',
        path,
        durationMs,
        errorMessage: error instanceof Error ? error.message : `Request fehlgeschlagen: ${path}`
      });
      throw error;
    } finally {
      window.clearTimeout(timeoutId);
    }
  }

  useEffect(() => {
    let cancelled = false;
    let timeoutId = 0;

    async function loadSupplementalData() {
      setSecondaryLoading(true);
      const [botResult, productRulesResult] = await Promise.allSettled([
        apiFetch('/api/bot'),
        apiFetch('/api/deal-engine/product-rules')
      ]);

      if (cancelled) {
        return;
      }

      if (botResult.status === 'fulfilled') {
        setBotDashboard(botResult.value);
      }

      if (productRulesResult.status === 'fulfilled') {
        const items = productRulesResult.value.items || [];
        setProductRulesList(items);
        setProductRuleForm((current) => {
          if (current.id || current.name || current.keywords) {
            return current;
          }

          return buildProductRuleForm(items[0] || {});
        });
      }

      if (botResult.status === 'rejected' || productRulesResult.status === 'rejected') {
        const fallbackMessage =
          botResult.status === 'rejected'
            ? botResult.reason?.message || 'Bot-Status konnte nicht geladen werden.'
            : productRulesResult.reason?.message || 'Produkt-Regeln konnten nicht geladen werden.';
        setStatus((current) => current || fallbackMessage);
      }

      setSecondaryLoading(false);
    }

    async function loadPage() {
      const startedAt = performance.now();
      try {
        setLoading(true);
        setStatus('');
        console.info('[DEAL_ENGINE_LOAD_START]', {
          route: '/deal-engine',
          strategy: 'core_first_then_secondary',
          timeoutMs: DEAL_ENGINE_LOAD_TIMEOUT_MS
        });

        timeoutId = window.setTimeout(() => {
          if (!cancelled) {
            console.warn('[DEAL_ENGINE_LOAD_TIMEOUT]', {
              route: '/deal-engine',
              timeoutMs: DEAL_ENGINE_LOAD_TIMEOUT_MS
            });
            setStatus((current) => current || 'Deal Engine reagiert gerade langsam. Basisdaten werden weiter geladen.');
          }
        }, DEAL_ENGINE_LOAD_TIMEOUT_MS);

        const [dashboardResult, settingsResult, sampleResult] = await Promise.allSettled([
          apiFetch('/api/deal-engine/dashboard'),
          apiFetch('/api/deal-engine/settings'),
          apiFetch('/api/deal-engine/sample')
        ]);

        if (cancelled) {
          return;
        }

        if (dashboardResult.status === 'fulfilled') {
          setDashboard(dashboardResult.value);
        }

        if (settingsResult.status === 'fulfilled') {
          setSettings(settingsResult.value.item);
        }

        if (sampleResult.status === 'fulfilled') {
          setSamplePayload(sampleResult.value.item);
        }

        setForm((current) => {
          const isStillDefault = !current.title && !current.amazonUrl && !current.amazonPrice;

          return isStillDefault && sampleResult.status === 'fulfilled' ? buildFormFromSample(sampleResult.value.item) : current;
        });

        const coreErrors = [dashboardResult, settingsResult, sampleResult]
          .filter((entry) => entry.status === 'rejected')
          .map((entry) => entry.reason?.message || 'Deal Engine konnte nicht geladen werden.');

        if (coreErrors.length === 3) {
          throw new Error(coreErrors[0]);
        }

        if (coreErrors.length > 0) {
          setStatus(coreErrors[0]);
        }

        setLoading(false);
        console.info('[DEAL_ENGINE_LOAD_DONE]', {
          route: '/deal-engine',
          durationMs: Math.round(performance.now() - startedAt),
          coreLoaded: 3 - coreErrors.length,
          secondaryPending: true
        });

        void loadSupplementalData();
      } catch (error) {
        if (!cancelled) {
          console.error('[UI_ROUTE_ERROR]', {
            route: '/deal-engine',
            errorMessage: error instanceof Error ? error.message : 'Deal Engine konnte nicht geladen werden.'
          });
          setStatus(error instanceof Error ? error.message : 'Deal Engine konnte nicht geladen werden.');
        }
      } finally {
        window.clearTimeout(timeoutId);
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadPage();

    return () => {
      cancelled = true;
      window.clearTimeout(timeoutId);
    };
  }, [user?.role]);

  function updateSettings(path, value) {
    setSettings((current) => {
      if (!current) {
        return current;
      }

      return {
        ...current,
        [path]: {
          ...current[path],
          ...value
        }
      };
    });
  }

  function updateForm(field, value) {
    setForm((current) => ({
      ...current,
      [field]: value
    }));
  }

  function updateProductRuleForm(field, value) {
    setProductRuleForm((current) => ({
      ...current,
      [field]: value
    }));
  }

  async function refreshProductRules() {
    const data = await apiFetch('/api/deal-engine/product-rules');
    setProductRulesList(data.items || []);
    return data.items || [];
  }

  async function refreshDashboard() {
    const [dashboardResult, botResult] = await Promise.allSettled([apiFetch('/api/deal-engine/dashboard'), apiFetch('/api/bot')]);

    if (dashboardResult.status === 'fulfilled') {
      setDashboard(dashboardResult.value);
    }

    if (botResult.status === 'fulfilled') {
      setBotDashboard(botResult.value);
    }
  }

  async function handleSaveSettings() {
    if (!settings) {
      return;
    }

    try {
      setSaving(true);
      setStatus('');
      const data = await apiFetch('/api/deal-engine/settings', {
        method: 'PUT',
        body: JSON.stringify(settings)
      });

      setSettings(data.item);
      await refreshDashboard();
      setStatus('Regler gespeichert.');
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Regler konnten nicht gespeichert werden.');
    } finally {
      setSaving(false);
    }
  }

  async function handleAnalyze() {
    try {
      setAnalyzing(true);
      setStatus('');
      const payload = buildAnalyzePayload(form);
      const data = await apiFetch('/api/deal-engine/analyze', {
        method: 'POST',
        body: JSON.stringify(payload)
      });

      setResult(data.item);
      await refreshDashboard();
      setStatus(`Analyse abgeschlossen: ${data.item?.decision || 'unbekannt'}.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Analyse fehlgeschlagen.');
    } finally {
      setAnalyzing(false);
    }
  }

  async function handleSaveProductRule() {
    try {
      setSavingProductRule(true);
      setStatus('');
      const method = productRuleForm.id ? 'PUT' : 'POST';
      const path = productRuleForm.id
        ? `/api/deal-engine/product-rules/${productRuleForm.id}`
        : '/api/deal-engine/product-rules';
      const data = await apiFetch(path, {
        method,
        body: JSON.stringify(productRuleForm)
      });

      const nextRules = await refreshProductRules();
      setProductRuleForm(buildProductRuleForm(data.item || nextRules[0] || {}));
      setStatus(productRuleForm.id ? 'Produkt-Regel gespeichert.' : 'Produkt-Regel angelegt.');
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Produkt-Regel konnte nicht gespeichert werden.');
    } finally {
      setSavingProductRule(false);
    }
  }

  async function handleToggleProductRule(rule, nextActive) {
    try {
      setBusyRuleId(rule.id);
      setStatus('');
      const data = await apiFetch(`/api/deal-engine/product-rules/${rule.id}/active`, {
        method: 'PATCH',
        body: JSON.stringify({ active: nextActive })
      });

      const nextRules = await refreshProductRules();
      if (productRuleForm.id === rule.id) {
        setProductRuleForm(buildProductRuleForm(data.item || nextRules.find((item) => item.id === rule.id) || {}));
      }
      setStatus(nextActive ? 'Produkt-Regel aktiviert.' : 'Produkt-Regel deaktiviert.');
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Produkt-Regel konnte nicht umgeschaltet werden.');
    } finally {
      setBusyRuleId(null);
    }
  }

  async function handleDeleteProductRule(rule) {
    try {
      setBusyRuleId(rule.id);
      setStatus('');
      await apiFetch(`/api/deal-engine/product-rules/${rule.id}`, {
        method: 'DELETE'
      });

      const nextRules = await refreshProductRules();
      if (productRuleForm.id === rule.id) {
        setProductRuleForm(buildProductRuleForm(nextRules[0] || {}));
      }
      setStatus('Produkt-Regel geloescht.');
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Produkt-Regel konnte nicht geloescht werden.');
    } finally {
      setBusyRuleId(null);
    }
  }

  const currentResult = result || dashboard?.runs?.[0] || null;
  const productRules = currentResult?.analysis?.productRules || currentResult?.productRules || null;
  const qualityPreset = detectQualityPreset(settings);
  const productFilterPreset = detectProductFilterPreset(settings);
  const automationPreset = detectAutomationPreset(settings);

  const recentDealCount = useMemo(
    () => (dashboard?.timeline || []).filter((entry) => isWithinLastMinutes(entry.createdAt, 5)).length,
    [dashboard]
  );

  const systemStatusCards = useMemo(
    () => [
      {
        title: 'Backend',
        value: loading ? 'Laedt' : 'Online',
        detail: dashboard?.systemStatus?.detail || 'API erreichbar',
        tone: loading ? 'info' : 'success'
      },
      {
        title: 'Telegram Reader',
        value: botDashboard?.operationalStatus?.telegramReader?.label || 'unbekannt',
        detail: botDashboard?.operationalStatus?.telegramReader?.detail || 'Reader-Status wird geladen.',
        tone: getToneClass(botDashboard?.operationalStatus?.telegramReader?.status || botDashboard?.operationalStatus?.telegramReader?.label)
      },
      {
        title: 'Queue',
        value: botDashboard?.operationalStatus?.scheduler?.label || 'bereit',
        detail:
          botDashboard?.operationalStatus?.scheduler?.detail ||
          `${dashboard?.outputs?.openQueueCount || 0} offene Queue-Eintraege fuer die Deal Engine`,
        tone: getToneClass(botDashboard?.operationalStatus?.scheduler?.status || botDashboard?.operationalStatus?.scheduler?.label)
      },
      {
        title: 'Fehlerstatus',
        value: dashboard?.errors?.length ? `${dashboard.errors.length} offen` : 'keine',
        detail: dashboard?.errors?.[0]?.detail || 'Keine aktuellen Blocker.',
        tone: dashboard?.errors?.length ? 'danger' : 'success'
      }
    ],
    [botDashboard, dashboard, loading]
  );

  const liveMetricCards = useMemo(
    () => [
      {
        title: 'Neue Deals',
        value: `${recentDealCount} / 5 min`,
        detail: `${dashboard?.metrics?.totalRuns || 0} Gesamt-Runs`,
        tone: recentDealCount > 0 ? 'success' : 'info'
      },
      {
        title: 'Approve Count',
        value: toNumber(dashboard?.metrics?.approvedRuns),
        detail: 'Nur APPROVE geht weiter in die Publisher-Strecke.',
        tone: 'success'
      },
      {
        title: 'Review Count',
        value: toNumber(dashboard?.metrics?.queuedRuns),
        detail: 'QUEUE wird hier als Review-Sammelstrecke behandelt.',
        tone: toNumber(dashboard?.metrics?.queuedRuns) > 0 ? 'warning' : 'info'
      },
      {
        title: 'Block Count',
        value: toNumber(dashboard?.metrics?.rejectedRuns),
        detail: 'REJECT durch Validierung, Fake-Pattern oder Produktregeln.',
        tone: toNumber(dashboard?.metrics?.rejectedRuns) > 0 ? 'danger' : 'success'
      }
    ],
    [dashboard, recentDealCount]
  );

  const outputStatusCards = useMemo(() => {
    const telegramReady = dashboard?.outputs?.snapshot?.telegram?.configured === true;
    const baseTone = telegramReady ? 'success' : 'warning';
    const baseDetail = telegramReady ? 'Telegram Bot ist bereit. Approved/Rejected laufen ueber ENV-Routen.' : 'Telegram Bot oder Zielgruppe pruefen.';

    return [
      {
        title: 'Testgruppe',
        value: telegramReady ? 'aktiv' : 'pruefen',
        detail: 'Bestehender Generator-Post plus Analyse bleiben aktiv.',
        tone: baseTone
      },
      {
        title: 'Approved Gruppe',
        value: telegramReady ? 'bereit' : 'inaktiv',
        detail: baseDetail,
        tone: baseTone
      },
      {
        title: 'Rejected Gruppe',
        value: telegramReady ? 'bereit' : 'inaktiv',
        detail: baseDetail,
        tone: baseTone
      }
    ];
  }, [dashboard]);

  const flowCards = useMemo(
    () => [
      {
        title: 'Reader',
        value: botDashboard?.operationalStatus?.telegramReader?.label || 'vorbereitet',
        detail: botDashboard?.operationalStatus?.telegramReader?.detail || 'Quelle nimmt Deals entgegen.',
        tone: getToneClass(botDashboard?.operationalStatus?.telegramReader?.status || botDashboard?.operationalStatus?.telegramReader?.label)
      },
      {
        title: 'Analyse',
        value: productRules?.status === 'matched' ? 'Produktregel aktiv' : 'Produktregeln + Keepa',
        detail:
          productRules?.summary ||
          'Marktvergleich zuerst, Keepa nur Fallback, Produktregeln greifen zusaetzlich.',
        tone: productRules?.status === 'matched' ? 'warning' : 'success'
      },
      {
        title: 'Decision',
        value: currentResult?.decision || 'wartet',
        detail: currentResult?.decisionReason || 'Noch keine finale Entscheidung.',
        tone: getToneClass(currentResult?.decision || dashboard?.systemStatus?.label)
      },
      {
        title: 'Output',
        value: dashboard?.outputs?.openQueueCount ? `${dashboard.outputs.openQueueCount} offen` : 'bereit',
        detail: 'Test / Approved / Rejected werden getrennt bedient.',
        tone: dashboard?.outputs?.openQueueCount ? 'warning' : 'success'
      }
    ],
    [botDashboard, currentResult, dashboard, productRules]
  );

  const moduleCards = [
    { title: 'Scrapper', path: '/scraper', detail: 'Quellen und Importstrecke.' },
    { title: 'Copybot', path: '/copybot', detail: 'Review Queue und Quellenmanagement.' },
    { title: 'Templates', path: '/templates', detail: 'Bausteine und Generator-Vorlagen.' },
    { title: 'Autobot', path: '/autobot', detail: 'Automatische Prozesse und Jobs.' },
    { title: 'Logik-Zentrale', path: '/learning', detail: 'Keepa, Fake-Drop und Lernlogik.' },
    { title: 'Sperrzeiten', path: '/sperrzeiten', detail: 'Deal-Lock und Cooldowns.' },
    { title: 'Logs', path: '/logs', detail: 'Fehler, Queue und Laufzeit-Logs.' }
  ];

  const quickActions = [
    { title: 'Generator', path: '/generator', detail: 'Direkt zum Hauptpost.' },
    { title: 'Publishing', path: '/publishing/telegram', detail: 'Telegram-Ziele und Testgruppe.' },
    { title: 'Logs', path: '/logs', detail: 'Schnell zu Fehlern und Events.' },
    { title: 'Settings', path: '/settings', detail: 'Systemweite Konfiguration.' }
  ];

  const safetyChecks = [
    'Hauptpost nutzt keine Telegram-Titel aus Fremdquellen.',
    'Hauptpost nutzt keine Telegram-Bilder oder fremde Collagen.',
    'Hauptpost nutzt keine fremden Links.',
    'Hauptpost darf nur mit PAAPI-, Amazon- und verifizierten Daten gebaut werden.'
  ];

  const routingExamples = [
    {
      title: 'Testgruppe',
      body: 'Nachricht 1: bestehender Generator-Post (unveraendert)\nNachricht 2: komplette Analyse'
    },
    {
      title: 'Approved Gruppe',
      body: 'Nur APPROVE\nNur Nachricht 1\nKeine Analyse'
    },
    {
      title: 'Rejected Gruppe',
      body: '\u26A0\uFE0F NICHT VEROEFFENTLICHT\n\u{1F4E2} Quelle: Beispielgruppe\n\u{1F4CC} Grund: Produktregel blockiert.\n\u{1F6E0} Loesung: Preislimit oder Daten pruefen.'
    }
  ];

  const safetyCards = [
    {
      title: 'Seller-Regeln',
      tone: 'danger',
      badge: 'hart',
      body: 'Veroeffentlicht erlaubt nur AMAZON_DIRECT und FBA. FBM und UNKNOWN bleiben global draussen. Optimierte Deals duerfen FBA_UNKNOWN nur separat nutzen.'
    },
    {
      title: 'Varianten-Schutz',
      tone: 'success',
      badge: 'aktiv',
      body: 'Nur Farbe, Groesse, Laenge und gleiche Style-Familie. Es wird immer komplett geprueft und erst danach die guenstigste erlaubte Variante genommen.'
    },
    {
      title: 'Packgroessen-Schutz',
      tone: 'warning',
      badge: 'match',
      body: '5er gegen 30er Pack, 250g gegen 1kg oder Einzelteil gegen Multipack werden blockiert, wenn Mengen nicht sauber vergleichbar sind.'
    },
    {
      title: 'Produktrollen-Schutz',
      tone: 'warning',
      badge: 'rolle',
      body: 'Kochset gegen Deckel, Kamera gegen Halterung oder Fernseher gegen Fernbedienung bleiben gesperrt. Zubehoer ersetzt nie das Hauptprodukt.'
    },
    {
      title: 'Hauptpost-Schutz',
      tone: 'info',
      badge: 'clean',
      body: 'Titel, Bilder und Links muessen aus verifizierten Amazon-/PAAPI-Daten kommen. Fremde Telegram-Inhalte bleiben aus dem Hauptpost draussen.'
    }
  ];
  const variantCards = [
    {
      title: 'Guenstigste Variante',
      tone: 'success',
      badge: 'scan',
      body: 'Zuerst wird dieselbe Amazon-Familie geprueft. Nicht der erste Treffer, sondern die guenstigste erlaubte Variante gewinnt.'
    },
    {
      title: 'Erlaubte Wechsel',
      tone: 'info',
      badge: 'same',
      body: 'Farbe, Groesse, Laenge und gleiche Style-Familie sind erlaubt, solange Produktart und Rolle identisch bleiben.'
    },
    {
      title: 'Packgroessen-Schutz',
      tone: 'warning',
      badge: 'pack',
      body: '5er gegen 30er Pack, 250g gegen 1kg oder Einzelteil gegen Multipack werden als PACK_SIZE_MISMATCH gestoppt.'
    },
    {
      title: 'Produktrollen-Schutz',
      tone: 'warning',
      badge: 'role',
      body: 'Kochset gegen Deckel, Kamera gegen Halterung oder Fernseher gegen Fernbedienung bleiben PRODUCT_ROLE_MISMATCH.'
    }
  ];

  const presetOptions = {
    quality: [
      ['locker', 'Locker'],
      ['normal', 'Normal'],
      ['streng', 'Streng'],
      ['profi', 'Profi']
    ],
    productFilter: [
      ['standard', 'Standard'],
      ['streng', 'Streng'],
      ['china_filter', 'China-Filter aktiv']
    ],
    automation: [
      ['testmodus', 'Testmodus'],
      ['review_sammeln', 'Review sammeln'],
      ['auto_posten', 'Auto posten']
    ]
  };

  const latestOverviewActivity = useMemo(() => (dashboard?.timeline || []).slice(0, 5), [dashboard]);
  const activeRuleCount = useMemo(() => productRulesList.filter((rule) => rule.active).length, [productRulesList]);
  const visibleProductRules = useMemo(
    () => (showAllRules ? productRulesList : productRulesList.slice(0, 6)),
    [productRulesList, showAllRules]
  );
  const logEntries = useMemo(() => {
    const timelineEntries = (dashboard?.timeline || []).map((entry) => ({
      id: `timeline-${entry.id}`,
      type: 'activity',
      tone: entry.tone || 'info',
      title: entry.title || `Run ${entry.id}`,
      detail: entry.detail || 'Keine Details',
      time: entry.createdAt
    }));
    const errorItems = (dashboard?.errors || []).map((entry) => ({
      id: `error-${entry.id}`,
      type: 'error',
      tone: entry.tone || 'danger',
      title: entry.title || 'Fehler',
      detail: entry.detail || 'Keine Details',
      time: entry.createdAt
    }));
    const reasonItems = (currentResult?.analysis?.reasons || []).map((entry, index) => ({
      id: `reason-${currentResult?.id || 'latest'}-${index}`,
      type: 'reason',
      tone: getToneClass(currentResult?.decision || 'info'),
      title: `Analysegrund ${index + 1}`,
      detail: entry,
      time: currentResult?.createdAt
    }));

    return [...errorItems, ...timelineEntries, ...reasonItems]
      .filter((entry) => entry.title || entry.detail)
      .sort((left, right) => new Date(right.time || 0).getTime() - new Date(left.time || 0).getTime());
  }, [currentResult, dashboard]);
  const filteredLogEntries = useMemo(() => {
    if (logFilter === 'errors') {
      return logEntries.filter((entry) => entry.type === 'error');
    }
    if (logFilter === 'activity') {
      return logEntries.filter((entry) => entry.type === 'activity');
    }
    if (logFilter === 'reasons') {
      return logEntries.filter((entry) => entry.type === 'reason');
    }
    return logEntries;
  }, [logEntries, logFilter]);
  const visibleLogEntries = useMemo(
    () => (showAllLogs ? filteredLogEntries : filteredLogEntries.slice(0, 8)),
    [filteredLogEntries, showAllLogs]
  );
  const tabMetadata = useMemo(
    () => ({
      overview: `${systemStatusCards.length + liveMetricCards.length} Karten`,
      rules: activeRuleCount ? `${activeRuleCount} aktiv` : 'Keine Regeln',
      variants: `${variantCards.length} Checks`,
      routing: `${outputStatusCards.length} Ziele`,
      queue: dashboard?.outputs?.openQueueCount ? `${dashboard.outputs.openQueueCount} offen` : 'leer',
      safety: `${safetyCards.length} Schutzregeln`,
      logs: `${logEntries.length} Eintraege`,
      experts: expertMode ? 'offen' : 'eingeklappt'
    }),
    [activeRuleCount, dashboard, expertMode, liveMetricCards.length, logEntries.length, outputStatusCards.length, safetyCards.length, systemStatusCards.length, variantCards.length]
  );
  const summaryCards = useMemo(
    () => [...systemStatusCards.slice(0, 2), ...liveMetricCards.slice(0, 2), ...outputStatusCards.slice(0, 1)],
    [liveMetricCards, outputStatusCards, systemStatusCards]
  );
  const queueSnapshotCards = useMemo(
    () => [
      {
        title: 'Offene Queue',
        value: dashboard?.outputs?.openQueueCount ? `${dashboard.outputs.openQueueCount}` : '0',
        detail: dashboard?.outputs?.openQueueCount ? 'Eintraege warten auf Publishing oder Review.' : 'Aktuell keine offenen Queue-Eintraege.',
        tone: dashboard?.outputs?.openQueueCount ? 'warning' : 'success'
      },
      {
        title: 'Review Sammelstrecke',
        value: `${toNumber(dashboard?.metrics?.queuedRuns)}`,
        detail: 'QUEUE bleibt die kontrollierte Zwischenstufe fuer nicht finale Entscheidungen.',
        tone: toNumber(dashboard?.metrics?.queuedRuns) > 0 ? 'warning' : 'info'
      },
      {
        title: 'Queue Modus',
        value: settings?.global?.queueEnabled ? 'Aktiv' : 'Pausiert',
        detail: settings?.global?.queueEnabled ? 'Review- und Publishing-Pfade sind aktiviert.' : 'Queue wurde in den globalen Deal-Engine-Regeln deaktiviert.',
        tone: settings?.global?.queueEnabled ? 'success' : 'danger'
      },
      {
        title: 'Letzte Aktivitaet',
        value: latestOverviewActivity[0] ? formatDateTime(latestOverviewActivity[0].createdAt) : '-',
        detail: latestOverviewActivity[0]?.title || 'Noch keine Queue-nahe Aktivitaet vorhanden.',
        tone: latestOverviewActivity[0] ? latestOverviewActivity[0].tone || 'info' : 'info'
      }
    ],
    [dashboard, latestOverviewActivity, settings]
  );
  const activeTabConfig = useMemo(
    () => DEAL_ENGINE_TABS.find((tab) => tab.id === activeTab) || DEAL_ENGINE_TABS[0],
    [activeTab]
  );

  function renderOverviewTab() {
    return (
      <div className="engine-tab-stack">
        <section className="engine-tab-grid engine-tab-grid-overview">
          <section className="card engine-panel engine-panel-compact">
            <div className="engine-panel-header">
              <div>
                <p className="section-title">Statusuebersicht</p>
                <h2 className="page-title">Backend, Reader und Queue</h2>
              </div>
              <span className="engine-header-note">{dashboard?.systemStatus?.detail || 'Keine Daten vorhanden'}</span>
            </div>
            <div className="engine-card-grid">
              {systemStatusCards.map((card) => (
                <article key={card.title} className={`engine-card engine-card-status engine-tone-${card.tone}`}>
                  <div className="engine-card-head">
                    <p className="section-title">{card.title}</p>
                    <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                  </div>
                  <h3>{card.value}</h3>
                  <p>{card.detail}</p>
                </article>
              ))}
            </div>
          </section>

          <section className="card engine-panel engine-panel-compact">
            <div className="engine-panel-header">
              <div>
                <p className="section-title">Wichtigste Kennzahlen</p>
                <h2 className="page-title">Approve, Review und Block</h2>
              </div>
              <span className="engine-header-note">{recentDealCount} neue Deals in 5 Minuten</span>
            </div>
            <div className="engine-card-grid">
              {liveMetricCards.map((card) => (
                <article key={card.title} className={`engine-card engine-card-status engine-tone-${card.tone}`}>
                  <div className="engine-card-head">
                    <p className="section-title">{card.title}</p>
                    <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                  </div>
                  <h3>{card.value}</h3>
                  <p>{card.detail}</p>
                </article>
              ))}
            </div>
          </section>
        </section>

        <section className="engine-tab-grid engine-tab-grid-overview">
          <section className="card engine-panel engine-panel-compact">
            <div className="engine-panel-header">
              <div>
                <p className="section-title">Quick Actions</p>
                <h2 className="page-title">Wichtige Wege ohne Suchen</h2>
              </div>
              <span className="engine-header-note">Direkt zu den haeufigsten Bereichen</span>
            </div>
            <div className="engine-action-grid">
              {quickActions.map((item) => (
                <article key={item.title} className="engine-card engine-card-action engine-tone-info">
                  <div className="engine-card-head">
                    <p className="section-title">{item.title}</p>
                    <span className="status-chip info">go</span>
                  </div>
                  <p>{item.detail}</p>
                  <button type="button" className="secondary" onClick={() => openInternalRoute(item.path)}>
                    Oeffnen
                  </button>
                </article>
              ))}
            </div>
          </section>

          <section className="card engine-panel engine-panel-compact">
            <div className="engine-panel-header">
              <div>
                <p className="section-title">Letzte Aktivitaet</p>
                <h2 className="page-title">Was zuletzt passiert ist</h2>
              </div>
              <span className="engine-header-note">{latestOverviewActivity.length} Eintraege sichtbar</span>
            </div>
            {latestOverviewActivity.length ? (
              <div className="engine-list engine-list-compact">
                {latestOverviewActivity.map((entry) => (
                  <article key={entry.id} className="engine-list-item engine-log-item">
                    <div className="engine-card-head">
                      <strong>{entry.title}</strong>
                      <span className={`status-chip ${entry.tone}`}>{formatDateTime(entry.createdAt)}</span>
                    </div>
                    <p>{entry.detail}</p>
                  </article>
                ))}
              </div>
            ) : (
              <p className="engine-empty">Keine Daten vorhanden</p>
            )}
          </section>
        </section>

        <section className="card engine-panel">
          <div className="engine-panel-header">
            <div>
              <p className="section-title">Schnellmodi</p>
              <h2 className="page-title">Hauefige Steuerung ohne Expertenmodus</h2>
            </div>
            <span className="engine-header-note">{user?.role === 'admin' ? 'Direkt aenderbar' : 'Nur Lesemodus'}</span>
          </div>
          {settings ? (
            <>
              <div className="engine-card-grid">
                <article className="engine-card engine-card-rule engine-tone-info">
                  <div className="engine-card-head">
                    <p className="section-title">Qualitaet</p>
                    <span className="status-chip info">{qualityPreset}</span>
                  </div>
                  <div className="engine-chip-row">
                    {presetOptions.quality.map(([value, label]) => (
                      <button
                        key={value}
                        type="button"
                        className={qualityPreset === value ? 'primary' : 'secondary'}
                        disabled={user?.role !== 'admin'}
                        onClick={() => setSettings((current) => applyQualityPreset(current, value))}
                      >
                        {label}
                      </button>
                    ))}
                  </div>
                </article>

                <article className="engine-card engine-card-rule engine-tone-info">
                  <div className="engine-card-head">
                    <p className="section-title">Produktfilter</p>
                    <span className="status-chip info">{productFilterPreset}</span>
                  </div>
                  <div className="engine-chip-row">
                    {presetOptions.productFilter.map(([value, label]) => (
                      <button
                        key={value}
                        type="button"
                        className={productFilterPreset === value ? 'primary' : 'secondary'}
                        disabled={user?.role !== 'admin'}
                        onClick={() => setSettings((current) => applyProductFilterPreset(current, value))}
                      >
                        {label}
                      </button>
                    ))}
                  </div>
                </article>

                <article className="engine-card engine-card-rule engine-tone-info">
                  <div className="engine-card-head">
                    <p className="section-title">Automatik</p>
                    <span className="status-chip info">{automationPreset}</span>
                  </div>
                  <div className="engine-chip-row">
                    {presetOptions.automation.map(([value, label]) => (
                      <button
                        key={value}
                        type="button"
                        className={automationPreset === value ? 'primary' : 'secondary'}
                        disabled={user?.role !== 'admin'}
                        onClick={() => setSettings((current) => applyAutomationPreset(current, value))}
                      >
                        {label}
                      </button>
                    ))}
                  </div>
                </article>
              </div>

              <div className="engine-actions">
                <button type="button" className="secondary" disabled={user?.role !== 'admin' || saving} onClick={handleSaveSettings}>
                  {saving ? 'Speichert...' : 'Regler speichern'}
                </button>
                <button type="button" className="secondary" onClick={() => setActiveTab('experts')}>
                  Expertenmodus oeffnen
                </button>
              </div>
            </>
          ) : (
            <p className="engine-empty">Keine Daten vorhanden</p>
          )}
        </section>

        <section className="card engine-panel">
          <div className="engine-panel-header">
            <div>
              <p className="section-title">Flow & Module</p>
              <h2 className="page-title">Reader, Analyse, Entscheidung und Output</h2>
            </div>
            <span className="engine-header-note">{dashboard?.sources?.activeCount || 0} aktive Quellen</span>
          </div>
          <div className="engine-card-grid">
            {flowCards.map((card) => (
              <article key={card.title} className={`engine-card engine-card-status engine-tone-${card.tone}`}>
                <div className="engine-card-head">
                  <p className="section-title">{card.title}</p>
                  <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                </div>
                <h3>{card.value}</h3>
                <p>{card.detail}</p>
              </article>
            ))}
          </div>
          <div className="engine-flow-grid">
            {moduleCards.map((item) => (
              <article key={item.title} className="engine-flow-card engine-card-action engine-tone-info">
                <div className="engine-card-head">
                  <strong>{item.title}</strong>
                  <span className="status-chip info">modul</span>
                </div>
                <p>{item.detail}</p>
                <button type="button" className="secondary" onClick={() => openInternalRoute(item.path)}>
                  Oeffnen
                </button>
              </article>
            ))}
          </div>
        </section>
      </div>
    );
  }

  function renderProductRulesTab() {
    return (
      <div className="engine-tab-stack">
        <section className="card engine-panel">
          <div className="engine-panel-header">
            <div>
              <p className="section-title">Produkt-Regeln</p>
              <h2 className="page-title">Preisgrenzen und Produktgruppen</h2>
            </div>
            <span className="engine-header-note">{activeRuleCount} aktiv / {productRulesList.length} gesamt</span>
          </div>

          <div className="engine-tab-grid engine-tab-grid-rules">
            <article className="engine-card engine-card-rule engine-tone-info">
              <div className="engine-card-head">
                <div>
                  <p className="section-title">Regel bearbeiten</p>
                  <h3 style={{ margin: 0 }}>{productRuleForm.id ? productRuleForm.name || 'Produkt-Regel' : 'Neue Produkt-Regel'}</h3>
                </div>
                <span className={`status-chip ${productRuleForm.active ? 'success' : 'danger'}`}>
                  {productRuleForm.active ? 'aktiv' : 'deaktiviert'}
                </span>
              </div>

              <div className="engine-form-grid">
                <label className="engine-span-2">
                  <span>Regelname</span>
                  <input
                    value={productRuleForm.name}
                    onChange={(event) => updateProductRuleForm('name', event.target.value)}
                    placeholder="China Kopfhoerer"
                    disabled={user?.role !== 'admin'}
                  />
                </label>
                <label className="engine-span-2">
                  <span>Keywords / Produkttyp</span>
                  <input
                    value={productRuleForm.keywords}
                    onChange={(event) => updateProductRuleForm('keywords', event.target.value)}
                    placeholder="kopfhoerer, bluetooth kopfhoerer, earbuds"
                    disabled={user?.role !== 'admin'}
                  />
                </label>
                <label>
                  <span>Markenart</span>
                  <select
                    value={productRuleForm.brandType}
                    onChange={(event) => updateProductRuleForm('brandType', event.target.value)}
                    disabled={user?.role !== 'admin'}
                  >
                    <option value="ANY">Egal</option>
                    <option value="NONAME">NoName</option>
                    <option value="BRAND">Marke</option>
                  </select>
                </label>
                <label>
                  <span>Maximalpreis</span>
                  <input
                    type="number"
                    step="0.01"
                    value={productRuleForm.maxPrice}
                    onChange={(event) => updateProductRuleForm('maxPrice', event.target.value)}
                    placeholder="12"
                    disabled={user?.role !== 'admin'}
                  />
                </label>
                <label>
                  <span>Mindestbewertungen</span>
                  <input
                    type="number"
                    value={productRuleForm.minReviews}
                    onChange={(event) => updateProductRuleForm('minReviews', event.target.value)}
                    placeholder="50"
                    disabled={user?.role !== 'admin'}
                  />
                </label>
                <label>
                  <span>Mindeststerne</span>
                  <input
                    type="number"
                    step="0.1"
                    value={productRuleForm.minRating}
                    onChange={(event) => updateProductRuleForm('minRating', event.target.value)}
                    placeholder="4.0"
                    disabled={user?.role !== 'admin'}
                  />
                </label>
                <label>
                  <span>Kapazitaet Min mAh</span>
                  <input
                    type="number"
                    value={productRuleForm.capacityMin}
                    onChange={(event) => updateProductRuleForm('capacityMin', event.target.value)}
                    placeholder="19000"
                    disabled={user?.role !== 'admin'}
                  />
                </label>
                <label>
                  <span>Kapazitaet Max mAh</span>
                  <input
                    type="number"
                    value={productRuleForm.capacityMax}
                    onChange={(event) => updateProductRuleForm('capacityMax', event.target.value)}
                    placeholder="30000"
                    disabled={user?.role !== 'admin'}
                  />
                </label>
                <label className="engine-checkbox">
                  <span>Marktvergleich noetig</span>
                  <input
                    type="checkbox"
                    checked={productRuleForm.marketCompareRequired}
                    onChange={(event) => updateProductRuleForm('marketCompareRequired', event.target.checked)}
                    disabled={user?.role !== 'admin'}
                  />
                </label>
                <label className="engine-checkbox">
                  <span>Aktiv</span>
                  <input
                    type="checkbox"
                    checked={productRuleForm.active}
                    onChange={(event) => updateProductRuleForm('active', event.target.checked)}
                    disabled={user?.role !== 'admin'}
                  />
                </label>
              </div>

              <div className="engine-actions">
                <button
                  type="button"
                  className="secondary"
                  onClick={() => setProductRuleForm(buildProductRuleForm({ active: true }))}
                  disabled={user?.role !== 'admin'}
                >
                  Regel hinzufuegen
                </button>
                <button type="button" className="primary" onClick={handleSaveProductRule} disabled={user?.role !== 'admin' || savingProductRule}>
                  {savingProductRule ? 'Speichert...' : productRuleForm.id ? 'Update / Speichern' : 'Regel speichern'}
                </button>
              </div>
            </article>

            <section className="engine-list-section">
              {visibleProductRules.length ? (
                <div className="engine-rule-list">
                  {visibleProductRules.map((rule) => (
                    <article
                      key={rule.id}
                      className={`engine-card engine-card-rule engine-tone-${rule.active ? 'success' : 'danger'}`}
                      style={{ minHeight: 0 }}
                    >
                      <div className="engine-card-head">
                        <div>
                          <p className="section-title">Regel</p>
                          <h3>{rule.name}</h3>
                        </div>
                        <span className={`status-chip ${rule.active ? 'success' : 'danger'}`}>{rule.active ? 'aktiv' : 'aus'}</span>
                      </div>
                      <p>{(rule.keywords || []).join(', ') || 'Keine Daten vorhanden'}</p>
                      <div className="engine-rule-meta">
                        <span>Max {rule.maxPrice ?? '-'} EUR</span>
                        <span>Min Reviews {rule.minReviews ?? 0}</span>
                        <span>Min Sterne {rule.minRating ?? 0}</span>
                      </div>
                      <div className="engine-rule-meta">
                        <span>{rule.brandTypeLabel}</span>
                        <span>Marktvergleich {rule.marketCompareRequired ? 'ja' : 'nein'}</span>
                        <span>Kapazitaet {rule.capacityMin ?? '-'} bis {rule.capacityMax ?? 'offen'}</span>
                      </div>
                      <div className="engine-actions">
                        <button type="button" className="secondary" onClick={() => setProductRuleForm(buildProductRuleForm(rule))}>
                          Bearbeiten
                        </button>
                        <button
                          type="button"
                          className="secondary"
                          onClick={() => void handleToggleProductRule(rule, !rule.active)}
                          disabled={user?.role !== 'admin' || busyRuleId === rule.id}
                        >
                          {rule.active ? 'Deaktivieren' : 'Aktivieren'}
                        </button>
                        <button
                          type="button"
                          className="secondary"
                          onClick={() => void handleDeleteProductRule(rule)}
                          disabled={user?.role !== 'admin' || busyRuleId === rule.id}
                        >
                          Loeschen
                        </button>
                      </div>
                    </article>
                  ))}
                </div>
              ) : (
                <p className="engine-empty">Keine Daten vorhanden</p>
              )}

              {productRulesList.length > 6 ? (
                <button type="button" className="secondary" onClick={() => setShowAllRules((current) => !current)}>
                  {showAllRules ? 'Weniger anzeigen' : `Mehr anzeigen (${productRulesList.length - visibleProductRules.length})`}
                </button>
              ) : null}
            </section>
          </div>
        </section>
      </div>
    );
  }

  function renderRoutingTab() {
    return (
      <div className="engine-tab-stack">
        <section className="card engine-panel">
          <div className="engine-panel-header">
            <div>
              <p className="section-title">Routing</p>
              <h2 className="page-title">Wohin welcher Deal geht</h2>
            </div>
            <span className="engine-header-note">Testgruppe, Veroeffentlicht, Geblockt und Optimierte Deals getrennt</span>
          </div>
          <div className="engine-card-grid">
            {outputStatusCards.map((card) => (
              <article key={card.title} className={`engine-card engine-card-status engine-tone-${card.tone}`}>
                <div className="engine-card-head">
                  <p className="section-title">{card.title}</p>
                  <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                </div>
                <h3>{card.value}</h3>
                <p>{card.detail}</p>
              </article>
            ))}
            <article className="engine-card engine-card-info engine-tone-info">
              <div className="engine-card-head">
                <p className="section-title">Optimierte Deals</p>
                <span className="status-chip info">final</span>
              </div>
              <h3>Finaler Variantencheck</h3>
              <p>Vor jedem Optimized-Post wird erst die guenstigste erlaubte Variante derselben Produktfamilie bestimmt.</p>
            </article>
          </div>
        </section>

        <section className="card engine-panel">
          <div className="engine-panel-header">
            <div>
              <p className="section-title">Routenbeispiele</p>
              <h2 className="page-title">Kompakt erklaert</h2>
            </div>
            <span className="engine-header-note">Ein Hauptpost, klare Trennung der Gruppen</span>
          </div>
          <div className="engine-card-grid">
            {routingExamples.map((example) => (
              <article key={example.title} className="engine-card engine-card-info engine-tone-info">
                <div className="engine-card-head">
                  <p className="section-title">{example.title}</p>
                  <span className="status-chip info">route</span>
                </div>
                <pre className="engine-code engine-code-compact">{example.body}</pre>
              </article>
            ))}
          </div>
        </section>
      </div>
    );
  }

  function renderVariantTab() {
    return (
      <div className="engine-tab-stack">
        <section className="card engine-panel">
          <div className="engine-panel-header">
            <div>
              <p className="section-title">Variantencheck</p>
              <h2 className="page-title">Familie zuerst, Similar Search erst danach</h2>
            </div>
            <span className="engine-header-note">Creator API zuerst, sichtbare Varianten danach, PAAPI nur sparsam</span>
          </div>
          <div className="engine-card-grid">
            {variantCards.map((card) => (
              <article key={card.title} className={`engine-card engine-card-info engine-tone-${card.tone}`}>
                <div className="engine-card-head">
                  <p className="section-title">{card.title}</p>
                  <span className={`status-chip ${card.tone}`}>{card.badge}</span>
                </div>
                <p>{card.body}</p>
              </article>
            ))}
          </div>
        </section>

        <section className="card engine-panel">
          <div className="engine-panel-header">
            <div>
              <p className="section-title">Finaler Ablauf</p>
              <h2 className="page-title">Reader, Familie, Vergleich, Posting</h2>
            </div>
          </div>
          <div className="engine-card-grid">
            <article className="engine-card engine-card-status engine-tone-info">
              <div className="engine-card-head">
                <p className="section-title">1. Quelle</p>
                <span className="status-chip info">reader</span>
              </div>
              <p>Deal kommt rein und wird zuerst sauber auf ASIN, Familie und Produkttyp abgebildet.</p>
            </article>
            <article className="engine-card engine-card-status engine-tone-success">
              <div className="engine-card-head">
                <p className="section-title">2. Variantenfamilie</p>
                <span className="status-chip success">family</span>
              </div>
              <p>Innerhalb derselben Amazon-Familie werden alle erlaubten Varianten verglichen und die billigste gewaehlt.</p>
            </article>
            <article className="engine-card engine-card-status engine-tone-warning">
              <div className="engine-card-head">
                <p className="section-title">3. Schutzregeln</p>
                <span className="status-chip warning">guard</span>
              </div>
              <p>Packgroessen-, Rollen- und Seller-Schutz pruefen, ob die guenstige Variante wirklich gleichwertig ist.</p>
            </article>
            <article className="engine-card engine-card-status engine-tone-info">
              <div className="engine-card-head">
                <p className="section-title">4. Similar Search</p>
                <span className="status-chip info">optional</span>
              </div>
              <p>Erst wenn die Familie nichts Besseres liefert, kommt die breitere Similar Search ins Spiel.</p>
            </article>
          </div>
        </section>
      </div>
    );
  }

  function renderSafetyTab() {
    return (
      <div className="engine-tab-stack">
        <section className="card engine-panel">
          <div className="engine-panel-header">
            <div>
              <p className="section-title">Seller & Sicherheit</p>
              <h2 className="page-title">Seller-Whitelist und Hauptpost-Schutz</h2>
            </div>
            <span className="engine-header-note">Lieber blockieren als falsch posten</span>
          </div>
          <div className="engine-card-grid">
            {safetyCards.map((card) => (
              <article key={card.title} className={`engine-card engine-card-info engine-tone-${card.tone}`}>
                <div className="engine-card-head">
                  <p className="section-title">{card.title}</p>
                  <span className={`status-chip ${card.tone}`}>{card.badge}</span>
                </div>
                <p>{card.body}</p>
              </article>
            ))}
          </div>
        </section>

        <section className="card engine-panel">
          <div className="engine-panel-header">
            <div>
              <p className="section-title">Hauptpost-Schutz</p>
              <h2 className="page-title">Saubere Ausgabe ohne Fremddaten</h2>
            </div>
            <span className="engine-header-note">Nur verifizierte Daten fuer Titel, Bild und Link</span>
          </div>
          <div className="engine-list">
            {safetyChecks.map((line) => (
              <article key={line} className="engine-list-item">
                <strong>Check</strong>
                <p>{line}</p>
              </article>
            ))}
          </div>
        </section>
      </div>
    );
  }

  function renderLogsTab() {
    return (
      <div className="engine-tab-stack">
        <section className="card engine-panel">
          <div className="engine-panel-header">
            <div>
              <p className="section-title">Logs</p>
              <h2 className="page-title">Letzte Aktivitaet, Fehler und Gruende</h2>
            </div>
            <span className="engine-header-note">{filteredLogEntries.length} Eintraege im aktuellen Filter</span>
          </div>

          <div className="engine-log-toolbar">
            <label>
              <span>Filter</span>
              <select value={logFilter} onChange={(event) => setLogFilter(event.target.value)}>
                <option value="all">Alles</option>
                <option value="errors">Nur Fehler</option>
                <option value="activity">Nur Aktivitaet</option>
                <option value="reasons">Nur Analysegruende</option>
              </select>
            </label>
          </div>

          {visibleLogEntries.length ? (
            <div className="engine-log-list">
              {visibleLogEntries.map((entry) => (
                <article key={entry.id} className={`engine-card engine-card-log engine-tone-${entry.tone}`}>
                  <div className="engine-card-head">
                    <div>
                      <p className="section-title">{entry.type}</p>
                      <h3>{entry.title}</h3>
                    </div>
                    <span className={`status-chip ${entry.tone}`}>{formatDateTime(entry.time)}</span>
                  </div>
                  <pre className="engine-log-body">{entry.detail}</pre>
                </article>
              ))}
            </div>
          ) : (
            <p className="engine-empty">Keine Daten vorhanden</p>
          )}

          {filteredLogEntries.length > 8 ? (
            <button type="button" className="secondary" onClick={() => setShowAllLogs((current) => !current)}>
              {showAllLogs ? 'Weniger anzeigen' : `Mehr anzeigen (${filteredLogEntries.length - visibleLogEntries.length})`}
            </button>
          ) : null}
        </section>
      </div>
    );
  }

  function renderQueueTab() {
    return (
      <div className="engine-tab-stack">
        <section className="card engine-panel">
          <div className="engine-panel-header">
            <div>
              <p className="section-title">Queue</p>
              <h2 className="page-title">Warten, pruefen und kontrolliert veroeffentlichen</h2>
            </div>
            <span className="engine-header-note">
              {dashboard?.outputs?.openQueueCount || 0} offene Eintraege | Queue {settings?.global?.queueEnabled ? 'aktiv' : 'pausiert'}
            </span>
          </div>
          <div className="engine-card-grid">
            {queueSnapshotCards.map((card) => (
              <article key={card.title} className={`engine-card engine-card-status engine-tone-${card.tone}`}>
                <div className="engine-card-head">
                  <p className="section-title">{card.title}</p>
                  <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                </div>
                <h3>{card.value}</h3>
                <p>{card.detail}</p>
              </article>
            ))}
          </div>
        </section>

        <section className="card engine-panel">
          <div className="engine-panel-header">
            <div>
              <p className="section-title">Letzte Queue-nahe Aktivitaet</p>
              <h2 className="page-title">Timeline statt langer Rohlisten</h2>
            </div>
            <span className="engine-header-note">Kompakter Blick auf die letzten relevanten Schritte</span>
          </div>
          {latestOverviewActivity.length ? (
            <div className="engine-log-list">
              {latestOverviewActivity.map((entry) => (
                <article key={entry.id} className={`engine-card engine-card-log engine-tone-${entry.tone || 'info'}`}>
                  <div className="engine-card-head">
                    <div>
                      <p className="section-title">Timeline</p>
                      <h3>{entry.title || `Eintrag ${entry.id}`}</h3>
                    </div>
                    <span className={`status-chip ${entry.tone || 'info'}`}>{formatDateTime(entry.createdAt)}</span>
                  </div>
                  <p>{entry.detail || entry.reason || 'Keine Details vorhanden.'}</p>
                </article>
              ))}
            </div>
          ) : (
            <p className="engine-empty">Keine Daten vorhanden</p>
          )}
        </section>
      </div>
    );
  }

  function renderExpertsTab() {
    return (
      <div className="engine-tab-stack">
        <section className="card engine-panel">
          <div className="engine-panel-header">
            <div>
              <p className="section-title">Expertenmodus</p>
              <h2 className="page-title">Technische Regler und Analyse-Playground</h2>
            </div>
            <span className="engine-header-note">Standardmaessig eingeklappt, nur bei Bedarf oeffnen</span>
          </div>

          <div className="engine-detail-list">
            <details className="engine-detail-card" open={expertMode} onToggle={(event) => setExpertMode(event.currentTarget.open)}>
              <summary>Erweiterte Regler</summary>
              {settings ? (
                <div className="engine-detail-content">
                  <div className="engine-settings-grid">
                    <label>
                      <span>Amazon Tag %</span>
                      <input
                        type="number"
                        value={settings.amazon.dayMinMarketAdvantagePct}
                        onChange={(event) =>
                          updateSettings('amazon', {
                            dayMinMarketAdvantagePct: Number(event.target.value)
                          })
                        }
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label>
                      <span>Amazon Nacht %</span>
                      <input
                        type="number"
                        value={settings.amazon.nightMinMarketAdvantagePct}
                        onChange={(event) =>
                          updateSettings('amazon', {
                            nightMinMarketAdvantagePct: Number(event.target.value)
                          })
                        }
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label>
                      <span>FBM Tag %</span>
                      <input
                        type="number"
                        value={settings.fbm.dayMinMarketAdvantagePct}
                        onChange={(event) =>
                          updateSettings('fbm', {
                            dayMinMarketAdvantagePct: Number(event.target.value)
                          })
                        }
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label>
                      <span>FBM Nacht %</span>
                      <input
                        type="number"
                        value={settings.fbm.nightMinMarketAdvantagePct}
                        onChange={(event) =>
                          updateSettings('fbm', {
                            nightMinMarketAdvantagePct: Number(event.target.value)
                          })
                        }
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label>
                      <span>Keepa Approve Score</span>
                      <input
                        type="number"
                        value={settings.global.keepaApproveScore}
                        onChange={(event) =>
                          updateSettings('global', {
                            keepaApproveScore: Number(event.target.value)
                          })
                        }
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label>
                      <span>Keepa Queue Score</span>
                      <input
                        type="number"
                        value={settings.global.keepaQueueScore}
                        onChange={(event) =>
                          updateSettings('global', {
                            keepaQueueScore: Number(event.target.value)
                          })
                        }
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label>
                      <span>Queue Margin %</span>
                      <input
                        type="number"
                        value={settings.global.queueMarginPct}
                        onChange={(event) =>
                          updateSettings('global', {
                            queueMarginPct: Number(event.target.value)
                          })
                        }
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label>
                      <span>Cheap Product Limit</span>
                      <input
                        type="number"
                        value={settings.global.cheapProductLimit}
                        onChange={(event) =>
                          updateSettings('global', {
                            cheapProductLimit: Number(event.target.value)
                          })
                        }
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label className="engine-checkbox"><span>Queue aktiv</span><input type="checkbox" checked={settings.global.queueEnabled} onChange={(event) => updateSettings('global', { queueEnabled: event.target.checked })} disabled={user?.role !== 'admin'} /></label>
                    <label className="engine-checkbox"><span>Nachtmodus aktiv</span><input type="checkbox" checked={settings.global.nightModeEnabled} onChange={(event) => updateSettings('global', { nightModeEnabled: event.target.checked })} disabled={user?.role !== 'admin'} /></label>
                    <label className="engine-checkbox"><span>Telegram Output</span><input type="checkbox" checked={settings.output.telegramEnabled} onChange={(event) => updateSettings('output', { telegramEnabled: event.target.checked })} disabled={user?.role !== 'admin'} /></label>
                    <label className="engine-checkbox"><span>WhatsApp Output</span><input type="checkbox" checked={settings.output.whatsappEnabled} onChange={(event) => updateSettings('output', { whatsappEnabled: event.target.checked })} disabled={user?.role !== 'admin'} /></label>
                    <label className="engine-checkbox"><span>AI Resolver optional aktiv</span><input type="checkbox" checked={settings.ai.resolverEnabled} onChange={(event) => updateSettings('ai', { resolverEnabled: event.target.checked })} disabled={user?.role !== 'admin'} /></label>
                    <label className="engine-checkbox"><span>KI fuer Amazon Direct</span><input type="checkbox" checked={settings.ai.amazonDirectEnabled} onChange={(event) => updateSettings('ai', { amazonDirectEnabled: event.target.checked })} disabled={user?.role !== 'admin'} /></label>
                    <label className="engine-checkbox"><span>KI nur bei Unsicherheit</span><input type="checkbox" checked={settings.ai.onlyOnUncertainty} onChange={(event) => updateSettings('ai', { onlyOnUncertainty: event.target.checked })} disabled={user?.role !== 'admin'} /></label>
                    <label className="engine-checkbox"><span>KI immer im Debugmodus</span><input type="checkbox" checked={settings.ai.alwaysInDebug} onChange={(event) => updateSettings('ai', { alwaysInDebug: event.target.checked })} disabled={user?.role !== 'admin'} /></label>
                    <label className="engine-checkbox"><span>Marktvergleich fuer Amazon Direct</span><input type="checkbox" checked={settings.quality.marketCompareAmazonDirectEnabled} onChange={(event) => updateSettings('quality', { marketCompareAmazonDirectEnabled: event.target.checked })} disabled={user?.role !== 'admin'} /></label>
                    <label className="engine-checkbox"><span>Nur Amazon Direct fuer Marktvergleich</span><input type="checkbox" checked={settings.quality.marketCompareAmazonDirectOnly} onChange={(event) => updateSettings('quality', { marketCompareAmazonDirectOnly: event.target.checked })} disabled={user?.role !== 'admin'} /></label>
                    <label className="engine-checkbox"><span>AI nur Amazon Direct</span><input type="checkbox" checked={settings.quality.aiAmazonDirectOnly} onChange={(event) => updateSettings('quality', { aiAmazonDirectOnly: event.target.checked })} disabled={user?.role !== 'admin'} /></label>
                    <label className="engine-checkbox"><span>FBA fuer Marktvergleich erlauben</span><input type="checkbox" checked={settings.quality.allowFbaThirdPartyMarketCompare} onChange={(event) => updateSettings('quality', { allowFbaThirdPartyMarketCompare: event.target.checked })} disabled={user?.role !== 'admin'} /></label>
                    <label className="engine-checkbox"><span>FBA fuer KI erlauben</span><input type="checkbox" checked={settings.quality.allowFbaThirdPartyAi} onChange={(event) => updateSettings('quality', { allowFbaThirdPartyAi: event.target.checked })} disabled={user?.role !== 'admin'} /></label>
                    <label className="engine-checkbox"><span>FBM fuer Marktvergleich erlauben</span><input type="checkbox" checked={settings.quality.allowFbmMarketCompare} onChange={(event) => updateSettings('quality', { allowFbmMarketCompare: event.target.checked })} disabled={user?.role !== 'admin'} /></label>
                    <label className="engine-checkbox"><span>FBM fuer KI erlauben</span><input type="checkbox" checked={settings.quality.allowFbmAi} onChange={(event) => updateSettings('quality', { allowFbmAi: event.target.checked })} disabled={user?.role !== 'admin'} /></label>
                    <label>
                      <span>Unbekannte Verkaeufer</span>
                      <select
                        value={settings.quality.unknownSellerMode}
                        onChange={(event) =>
                          updateSettings('quality', {
                            unknownSellerMode: event.target.value
                          })
                        }
                        disabled={user?.role !== 'admin'}
                      >
                        <option value="review">REVIEW</option>
                        <option value="block">BLOCK</option>
                      </select>
                    </label>
                  </div>
                  <div className="engine-actions">
                    <button type="button" className="secondary" disabled={user?.role !== 'admin' || saving} onClick={handleSaveSettings}>
                      {saving ? 'Speichert...' : 'Regler speichern'}
                    </button>
                  </div>
                </div>
              ) : (
                <div className="engine-detail-content">
                  <p className="engine-empty">Keine Daten vorhanden</p>
                </div>
              )}
            </details>

            <details className="engine-detail-card">
              <summary>Analyse Playground</summary>
              <div className="engine-detail-content">
                <div className="engine-form-grid">
                  <label>
                    <span>Quelle</span>
                    <input value={form.sourceName} onChange={(event) => updateForm('sourceName', event.target.value)} />
                  </label>
                  <label>
                    <span>Plattform</span>
                    <select value={form.sourcePlatform} onChange={(event) => updateForm('sourcePlatform', event.target.value)}>
                      <option value="telegram">telegram</option>
                      <option value="whatsapp">whatsapp</option>
                      <option value="internal">internal</option>
                    </select>
                  </label>
                  <label>
                    <span>Source Type</span>
                    <input value={form.sourceType} onChange={(event) => updateForm('sourceType', event.target.value)} />
                  </label>
                  <label>
                    <span>Seller</span>
                    <select value={form.sellerType} onChange={(event) => updateForm('sellerType', event.target.value)}>
                      <option value="AMAZON">AMAZON</option>
                      <option value="FBA">FBA</option>
                      <option value="FBM">FBM</option>
                      <option value="UNKNOWN">UNKNOWN</option>
                    </select>
                  </label>
                  <label>
                    <span>Seller Klasse</span>
                    <select value={form.sellerClass} onChange={(event) => updateForm('sellerClass', event.target.value)}>
                      <option value="AMAZON_DIRECT">AMAZON_DIRECT</option>
                      <option value="FBA_THIRDPARTY">FBA_THIRDPARTY</option>
                      <option value="FBM_THIRDPARTY">FBM_THIRDPARTY</option>
                      <option value="UNKNOWN">UNKNOWN</option>
                    </select>
                  </label>
                  <label>
                    <span>Verkauf durch Amazon</span>
                    <select value={form.soldByAmazon} onChange={(event) => updateForm('soldByAmazon', event.target.value)}>
                      <option value="yes">ja</option>
                      <option value="no">nein</option>
                      <option value="unknown">unbekannt</option>
                    </select>
                  </label>
                  <label>
                    <span>Versand durch Amazon</span>
                    <select value={form.shippedByAmazon} onChange={(event) => updateForm('shippedByAmazon', event.target.value)}>
                      <option value="yes">ja</option>
                      <option value="no">nein</option>
                      <option value="unknown">unbekannt</option>
                    </select>
                  </label>
                  <label className="engine-span-2">
                    <span>Titel</span>
                    <input value={form.title} onChange={(event) => updateForm('title', event.target.value)} />
                  </label>
                  <label className="engine-span-2">
                    <span>Amazon URL</span>
                    <input value={form.amazonUrl} onChange={(event) => updateForm('amazonUrl', event.target.value)} />
                  </label>
                  <label>
                    <span>Amazon Preis</span>
                    <input value={form.amazonPrice} onChange={(event) => updateForm('amazonPrice', event.target.value)} />
                  </label>
                  <label>
                    <span>Brand</span>
                    <input value={form.brand} onChange={(event) => updateForm('brand', event.target.value)} />
                  </label>
                  <label>
                    <span>Kategorie</span>
                    <input value={form.category} onChange={(event) => updateForm('category', event.target.value)} />
                  </label>
                  <label>
                    <span>Rating</span>
                    <input value={form.rating} onChange={(event) => updateForm('rating', event.target.value)} placeholder="4.2" />
                  </label>
                  <label>
                    <span>Rezensionen</span>
                    <input value={form.reviewCount} onChange={(event) => updateForm('reviewCount', event.target.value)} placeholder="100" />
                  </label>
                  <label>
                    <span>Variante</span>
                    <input value={form.variantKey} onChange={(event) => updateForm('variantKey', event.target.value)} />
                  </label>
                  <label>
                    <span>Menge / Set</span>
                    <input value={form.quantityKey} onChange={(event) => updateForm('quantityKey', event.target.value)} />
                  </label>
                  <label>
                    <span>Tag/Nacht Override</span>
                    <select value={form.overrideDayPart} onChange={(event) => updateForm('overrideDayPart', event.target.value)}>
                      <option value="day">day</option>
                      <option value="night">night</option>
                    </select>
                  </label>
                  <label className="engine-checkbox"><span>Markenprodukt</span><input type="checkbox" checked={form.isBrandProduct} onChange={(event) => updateForm('isBrandProduct', event.target.checked)} /></label>
                  <label className="engine-checkbox"><span>No-Name</span><input type="checkbox" checked={form.isNoName} onChange={(event) => updateForm('isNoName', event.target.checked)} /></label>
                  <label className="engine-checkbox"><span>China Produkt</span><input type="checkbox" checked={form.isChinaProduct} onChange={(event) => updateForm('isChinaProduct', event.target.checked)} /></label>
                  <label className="engine-span-2">
                    <span>Marktangebote JSON</span>
                    <textarea value={form.marketOffersJson} onChange={(event) => updateForm('marketOffersJson', event.target.value)} rows={10} />
                  </label>
                  <label>
                    <span>Keepa JSON</span>
                    <textarea value={form.keepaJson} onChange={(event) => updateForm('keepaJson', event.target.value)} rows={10} />
                  </label>
                  <label>
                    <span>AI JSON</span>
                    <textarea value={form.aiJson} onChange={(event) => updateForm('aiJson', event.target.value)} rows={10} />
                  </label>
                </div>
                <div className="engine-actions">
                  <button type="button" className="secondary" onClick={() => setForm(buildFormFromSample(samplePayload || {}))}>
                    Sample laden
                  </button>
                  <button type="button" className="primary" disabled={analyzing} onClick={handleAnalyze}>
                    {analyzing ? 'Analysiert...' : 'Analyse starten'}
                  </button>
                </div>
              </div>
            </details>

            <details className="engine-detail-card">
              <summary>Letztes Analyseergebnis</summary>
              <div className="engine-detail-content">
                {currentResult ? (
                  <>
                    <div className="engine-card-grid">
                      <article className={`engine-card engine-card-status engine-tone-${getToneClass(currentResult.decision)}`}>
                        <div className="engine-card-head">
                          <p className="section-title">Entscheidung</p>
                          <span className={`status-chip ${getToneClass(currentResult.decision)}`}>{currentResult.decision}</span>
                        </div>
                        <h3>{currentResult.analysis?.decisionSourceLabel || currentResult.analysis?.decisionSource || '-'}</h3>
                        <p>{currentResult.decisionReason}</p>
                      </article>
                      <article className={`engine-card engine-card-status engine-tone-${productRules?.status === 'matched' ? 'warning' : 'info'}`}>
                        <div className="engine-card-head">
                          <p className="section-title">Produktregeln</p>
                          <span className={`status-chip ${productRules?.status === 'matched' ? 'warning' : 'info'}`}>{productRules?.status || 'clear'}</span>
                        </div>
                        <h3>{productRules?.matchedRuleName || productRules?.action || 'none'}</h3>
                        <p>{productRules?.summary || 'Keine Produktregel ausgelost.'}</p>
                      </article>
                      <article className="engine-card engine-card-status engine-tone-info">
                        <div className="engine-card-head">
                          <p className="section-title">Markt</p>
                          <span className="status-chip info">{currentResult.marketOfferCount || 0} gueltig</span>
                        </div>
                        <h3>{currentResult.marketAdvantagePct ?? '-'}%</h3>
                        <p>Marktpreis {currentResult.marketPrice ?? '-'} | Schwelle {currentResult.analysis?.thresholdPct ?? '-'}</p>
                      </article>
                      <article className="engine-card engine-card-status engine-tone-info">
                        <div className="engine-card-head">
                          <p className="section-title">Keepa</p>
                          <span className="status-chip info">{currentResult.analysis?.fallbackUsed ? 'fallback' : 'idle'}</span>
                        </div>
                        <h3>{currentResult.keepaScore ?? '-'}</h3>
                        <p>avg90 {currentResult.keepaDiscount90 ?? '-'}% | avg180 {currentResult.keepaDiscount180 ?? '-'}%</p>
                      </article>
                    </div>
                    <div className="engine-code-grid">
                      <div>
                        <p className="section-title">Flow</p>
                        <pre className="engine-code">{prettyJson(currentResult.analysis?.flow || [])}</pre>
                      </div>
                      <div>
                        <p className="section-title">Analyse JSON</p>
                        <pre className="engine-code">{prettyJson(currentResult.analysis || {})}</pre>
                      </div>
                    </div>
                  </>
                ) : (
                  <p className="engine-empty">Keine Daten vorhanden</p>
                )}
              </div>
            </details>
          </div>
        </section>
      </div>
    );
  }

  function renderActiveTab() {
    if (activeTab === 'rules') {
      return renderProductRulesTab();
    }
    if (activeTab === 'variants') {
      return renderVariantTab();
    }
    if (activeTab === 'routing') {
      return renderRoutingTab();
    }
    if (activeTab === 'queue') {
      return renderQueueTab();
    }
    if (activeTab === 'safety') {
      return renderSafetyTab();
    }
    if (activeTab === 'logs') {
      return renderLogsTab();
    }
    if (activeTab === 'experts') {
      return renderExpertsTab();
    }
    return renderOverviewTab();
  }

  return (
    <Layout>
      <div className="engine-page">
        <section className="card engine-hero">
          <div className="engine-hero-grid">
            <div className="engine-hero-copy">
              <p className="section-title">Deal Engine</p>
              <h1 className="page-title">Produktregeln, sauberes Routing und ein Dashboard ohne Scroll-Chaos</h1>
              <p className="page-subtitle">
                Die Seite zeigt jetzt zuerst Status, Metriken, Output und Schnellaktionen. Darunter kommen Presets,
                Deal-Flow, Routing-Beispiele, Sicherheitscheck und erst dann die tieferen Expert-Regler.
              </p>
            </div>
            <div className="engine-hero-side">
              <span className="badge">Hauptpost bleibt unveraendert</span>
              <span className={`status-chip ${dashboard?.systemStatus?.tone || 'info'}`}>{dashboard?.systemStatus?.label || 'loading'}</span>
              <span className="badge">Keepa und Queue bleiben erhalten</span>
            </div>
          </div>
          {status ? (
            <div className="engine-inline-alert">
              <span className={`status-chip ${getToneClass(status)}`}>{getToneClass(status)}</span>
              <p>{status}</p>
            </div>
          ) : null}
        </section>

        {loading ? (
          <section className="card engine-panel">
            <p style={{ margin: 0 }}>Deal Engine wird geladen...</p>
          </section>
        ) : (
          <>
            <section className="card engine-panel engine-panel-compact">
              <div className="engine-panel-header">
                <div>
                  <p className="section-title">Sofortansicht</p>
                  <h2 className="page-title">Status, Kernzahlen und Schnellzugriffe</h2>
                </div>
                <span className="engine-header-note">
                  {secondaryLoading ? 'Erweitere Statusdaten werden nachgeladen...' : dashboard?.feasibility?.detail || '-'}
                </span>
              </div>

              <div className="engine-summary-grid">
                {summaryCards.map((card) => (
                  <article key={card.title} className={`engine-card engine-card-status engine-card-compact engine-tone-${card.tone}`}>
                    <div className="engine-card-head">
                      <p className="section-title">{card.title}</p>
                      <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                    </div>
                    <h3>{card.value}</h3>
                    <p>{card.detail}</p>
                  </article>
                ))}

                <article className="engine-card engine-card-action engine-card-compact engine-tone-info">
                  <div className="engine-card-head">
                    <p className="section-title">Quick Actions</p>
                    <span className="status-chip info">go</span>
                  </div>
                  <div className="engine-action-stack">
                    {quickActions.map((item) => (
                      <button key={item.title} type="button" className="secondary" onClick={() => openInternalRoute(item.path)}>
                        {item.title}
                      </button>
                    ))}
                  </div>
                </article>
              </div>
            </section>

            <section className="card engine-panel engine-tab-shell">
              <div className="engine-panel-header">
                <div>
                  <p className="section-title">Bereiche</p>
                  <h2 className="page-title">Deal Engine klar gegliedert</h2>
                </div>
                <span className="engine-header-note">
                  {activeTabConfig.label} - {tabMetadata[activeTabConfig.id]}
                </span>
              </div>

              <div className="engine-tab-bar" role="tablist" aria-label="Deal Engine Bereiche">
                {DEAL_ENGINE_TABS.map((tab) => (
                  <button
                    key={tab.id}
                    type="button"
                    role="tab"
                    aria-selected={activeTab === tab.id}
                    className={`engine-tab-button ${activeTab === tab.id ? 'active' : ''}`}
                    onClick={() => setActiveTab(tab.id)}
                  >
                    <span>{tab.label}</span>
                    <small>{tabMetadata[tab.id]}</small>
                  </button>
                ))}
              </div>

              <div className="engine-tab-content" role="tabpanel" aria-label={activeTabConfig.label}>
                {renderActiveTab()}
              </div>
            </section>

            {false ? (
              <>
            <section className="card engine-panel">
              <div className="engine-panel-header">
                <div>
                  <p className="section-title">Sofortansicht</p>
                  <h2 className="page-title">Alles Wichtige oben</h2>
                </div>
                <span className="engine-header-note">
                  {secondaryLoading ? 'Erweitere Statusdaten werden nachgeladen...' : dashboard?.feasibility?.detail || '-'}
                </span>
              </div>

              <div className="engine-card-grid">
                {systemStatusCards.map((card) => (
                  <article key={card.title} className={`engine-card engine-tone-${card.tone}`}>
                    <div className="engine-card-head">
                      <p className="section-title">{card.title}</p>
                      <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                    </div>
                    <h3>{card.value}</h3>
                    <p>{card.detail}</p>
                  </article>
                ))}
              </div>

              <div className="engine-card-grid">
                {liveMetricCards.map((card) => (
                  <article key={card.title} className={`engine-card engine-tone-${card.tone}`}>
                    <div className="engine-card-head">
                      <p className="section-title">{card.title}</p>
                      <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                    </div>
                    <h3>{card.value}</h3>
                    <p>{card.detail}</p>
                  </article>
                ))}
              </div>

              <div className="engine-card-grid">
                {outputStatusCards.map((card) => (
                  <article key={card.title} className={`engine-card engine-tone-${card.tone}`}>
                    <div className="engine-card-head">
                      <p className="section-title">{card.title}</p>
                      <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                    </div>
                    <h3>{card.value}</h3>
                    <p>{card.detail}</p>
                  </article>
                ))}
                <article className="engine-card engine-tone-info">
                  <div className="engine-card-head">
                    <p className="section-title">Quick Actions</p>
                    <span className="status-chip info">go</span>
                  </div>
                  <div style={{ display: 'grid', gap: 10 }}>
                    <button type="button" className="secondary" onClick={() => openInternalRoute('/generator')}>
                      Generator oeffnen
                    </button>
                    <button type="button" className="secondary" onClick={() => openInternalRoute('/publishing/telegram')}>
                      Testgruppe oeffnen
                    </button>
                    <button type="button" className="secondary" onClick={() => openInternalRoute('/logs')}>
                      Logs oeffnen
                    </button>
                    <button type="button" className="secondary" onClick={() => openInternalRoute('/settings')}>
                      Settings oeffnen
                    </button>
                  </div>
                </article>
              </div>
            </section>

            <section className="engine-mandatory-grid">
              <section className="card engine-panel">
                <div className="engine-panel-header">
                  <div>
                    <p className="section-title">Vereinfachte UI</p>
                    <h2 className="page-title">3 Hauptmodi statt Regler-Flut</h2>
                  </div>
                  <span className="engine-header-note">{user?.role === 'admin' ? 'Admin darf speichern' : 'Nur Lesemodus'}</span>
                </div>

                {settings ? (
                  <div style={{ display: 'grid', gap: 18 }}>
                    <article className="engine-card engine-tone-info">
                      <div className="engine-card-head">
                        <p className="section-title">Qualitaet</p>
                        <span className="status-chip info">{qualityPreset}</span>
                      </div>
                      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
                        {presetOptions.quality.map(([value, label]) => (
                          <button
                            key={value}
                            type="button"
                            className={qualityPreset === value ? 'primary' : 'secondary'}
                            disabled={user?.role !== 'admin'}
                            onClick={() => setSettings((current) => applyQualityPreset(current, value))}
                          >
                            {label}
                          </button>
                        ))}
                      </div>
                    </article>

                    <article className="engine-card engine-tone-info">
                      <div className="engine-card-head">
                        <p className="section-title">Produktfilter</p>
                        <span className="status-chip info">{productFilterPreset}</span>
                      </div>
                      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
                        {presetOptions.productFilter.map(([value, label]) => (
                          <button
                            key={value}
                            type="button"
                            className={productFilterPreset === value ? 'primary' : 'secondary'}
                            disabled={user?.role !== 'admin'}
                            onClick={() => setSettings((current) => applyProductFilterPreset(current, value))}
                          >
                            {label}
                          </button>
                        ))}
                      </div>
                      <p style={{ margin: '12px 0 0' }}>Beispielregeln aktiv: Powerbank-Limits und China-Kopfhoerer-Block ueber 25 EUR.</p>
                    </article>

                    <article className="engine-card engine-tone-info">
                      <div className="engine-card-head">
                        <p className="section-title">Automatik</p>
                        <span className="status-chip info">{automationPreset}</span>
                      </div>
                      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
                        {presetOptions.automation.map(([value, label]) => (
                          <button
                            key={value}
                            type="button"
                            className={automationPreset === value ? 'primary' : 'secondary'}
                            disabled={user?.role !== 'admin'}
                            onClick={() => setSettings((current) => applyAutomationPreset(current, value))}
                          >
                            {label}
                          </button>
                        ))}
                      </div>
                      <p style={{ margin: '12px 0 0' }}>Testgruppenrouting laeuft separat im Publisher. Dieses Preset steuert die Deal-Engine-Outputs.</p>
                    </article>

                    <div className="engine-actions">
                      <button type="button" className="secondary" onClick={() => setExpertMode((current) => !current)}>
                        {expertMode ? 'Expert Mode ausblenden' : 'Expert Mode einblenden'}
                      </button>
                      <button type="button" className="secondary" disabled={user?.role !== 'admin' || saving} onClick={handleSaveSettings}>
                        {saving ? 'Speichert...' : 'Regler speichern'}
                      </button>
                    </div>

                    {expertMode ? (
                      <div className="engine-settings-grid">
                        <label>
                          <span>Amazon Tag %</span>
                          <input
                            type="number"
                            value={settings.amazon.dayMinMarketAdvantagePct}
                            onChange={(event) =>
                              updateSettings('amazon', {
                                dayMinMarketAdvantagePct: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>Amazon Nacht %</span>
                          <input
                            type="number"
                            value={settings.amazon.nightMinMarketAdvantagePct}
                            onChange={(event) =>
                              updateSettings('amazon', {
                                nightMinMarketAdvantagePct: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>FBM Tag %</span>
                          <input
                            type="number"
                            value={settings.fbm.dayMinMarketAdvantagePct}
                            onChange={(event) =>
                              updateSettings('fbm', {
                                dayMinMarketAdvantagePct: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>FBM Nacht %</span>
                          <input
                            type="number"
                            value={settings.fbm.nightMinMarketAdvantagePct}
                            onChange={(event) =>
                              updateSettings('fbm', {
                                nightMinMarketAdvantagePct: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>Keepa Approve Score</span>
                          <input
                            type="number"
                            value={settings.global.keepaApproveScore}
                            onChange={(event) =>
                              updateSettings('global', {
                                keepaApproveScore: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>Keepa Queue Score</span>
                          <input
                            type="number"
                            value={settings.global.keepaQueueScore}
                            onChange={(event) =>
                              updateSettings('global', {
                                keepaQueueScore: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>Queue Margin %</span>
                          <input
                            type="number"
                            value={settings.global.queueMarginPct}
                            onChange={(event) =>
                              updateSettings('global', {
                                queueMarginPct: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>Cheap Product Limit</span>
                          <input
                            type="number"
                            value={settings.global.cheapProductLimit}
                            onChange={(event) =>
                              updateSettings('global', {
                                cheapProductLimit: Number(event.target.value)
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>Queue aktiv</span>
                          <input
                            type="checkbox"
                            checked={settings.global.queueEnabled}
                            onChange={(event) =>
                              updateSettings('global', {
                                queueEnabled: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>Nachtmodus aktiv</span>
                          <input
                            type="checkbox"
                            checked={settings.global.nightModeEnabled}
                            onChange={(event) =>
                              updateSettings('global', {
                                nightModeEnabled: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>Telegram Output</span>
                          <input
                            type="checkbox"
                            checked={settings.output.telegramEnabled}
                            onChange={(event) =>
                              updateSettings('output', {
                                telegramEnabled: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>WhatsApp Output</span>
                          <input
                            type="checkbox"
                            checked={settings.output.whatsappEnabled}
                            onChange={(event) =>
                              updateSettings('output', {
                                whatsappEnabled: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>AI Resolver optional aktiv</span>
                          <input
                            type="checkbox"
                            checked={settings.ai.resolverEnabled}
                            onChange={(event) =>
                              updateSettings('ai', {
                                resolverEnabled: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>KI fuer Amazon Direct</span>
                          <input
                            type="checkbox"
                            checked={settings.ai.amazonDirectEnabled}
                            onChange={(event) =>
                              updateSettings('ai', {
                                amazonDirectEnabled: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>KI nur bei Unsicherheit</span>
                          <input
                            type="checkbox"
                            checked={settings.ai.onlyOnUncertainty}
                            onChange={(event) =>
                              updateSettings('ai', {
                                onlyOnUncertainty: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>KI immer im Debugmodus</span>
                          <input
                            type="checkbox"
                            checked={settings.ai.alwaysInDebug}
                            onChange={(event) =>
                              updateSettings('ai', {
                                alwaysInDebug: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>Marktvergleich fuer Amazon Direct</span>
                          <input
                            type="checkbox"
                            checked={settings.quality.marketCompareAmazonDirectEnabled}
                            onChange={(event) =>
                              updateSettings('quality', {
                                marketCompareAmazonDirectEnabled: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>Nur Amazon Direct fuer Marktvergleich</span>
                          <input
                            type="checkbox"
                            checked={settings.quality.marketCompareAmazonDirectOnly}
                            onChange={(event) =>
                              updateSettings('quality', {
                                marketCompareAmazonDirectOnly: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>AI nur Amazon Direct</span>
                          <input
                            type="checkbox"
                            checked={settings.quality.aiAmazonDirectOnly}
                            onChange={(event) =>
                              updateSettings('quality', {
                                aiAmazonDirectOnly: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>FBA fuer Marktvergleich erlauben</span>
                          <input
                            type="checkbox"
                            checked={settings.quality.allowFbaThirdPartyMarketCompare}
                            onChange={(event) =>
                              updateSettings('quality', {
                                allowFbaThirdPartyMarketCompare: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>FBA fuer KI erlauben</span>
                          <input
                            type="checkbox"
                            checked={settings.quality.allowFbaThirdPartyAi}
                            onChange={(event) =>
                              updateSettings('quality', {
                                allowFbaThirdPartyAi: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>FBM fuer Marktvergleich erlauben</span>
                          <input
                            type="checkbox"
                            checked={settings.quality.allowFbmMarketCompare}
                            onChange={(event) =>
                              updateSettings('quality', {
                                allowFbmMarketCompare: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label className="engine-checkbox">
                          <span>FBM fuer KI erlauben</span>
                          <input
                            type="checkbox"
                            checked={settings.quality.allowFbmAi}
                            onChange={(event) =>
                              updateSettings('quality', {
                                allowFbmAi: event.target.checked
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          />
                        </label>
                        <label>
                          <span>Unbekannte Verkaeufer</span>
                          <select
                            value={settings.quality.unknownSellerMode}
                            onChange={(event) =>
                              updateSettings('quality', {
                                unknownSellerMode: event.target.value
                              })
                            }
                            disabled={user?.role !== 'admin'}
                          >
                            <option value="review">REVIEW</option>
                            <option value="block">BLOCK</option>
                          </select>
                        </label>
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </section>

              <section className="card engine-panel">
                <div className="engine-panel-header">
                  <div>
                    <p className="section-title">Deal Flow</p>
                    <h2 className="page-title">{'Reader -> Analyse -> Decision -> Output'}</h2>
                  </div>
                  <span className="engine-header-note">{dashboard?.sources?.activeCount || 0} aktive Quellen</span>
                </div>

                <div className="engine-card-grid">
                  {flowCards.map((card) => (
                    <article key={card.title} className={`engine-card engine-tone-${card.tone}`}>
                      <div className="engine-card-head">
                        <p className="section-title">{card.title}</p>
                        <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                      </div>
                      <h3>{card.value}</h3>
                      <p>{card.detail}</p>
                    </article>
                  ))}
                </div>

                <div className="engine-flow-grid">
                  {moduleCards.map((item) => (
                    <article key={item.title} className="engine-flow-card engine-tone-info">
                      <div className="engine-card-head">
                        <strong>{item.title}</strong>
                        <span className="status-chip info">modul</span>
                      </div>
                      <p>{item.detail}</p>
                      <button type="button" className="secondary" onClick={() => openInternalRoute(item.path)}>
                        Oeffnen
                      </button>
                    </article>
                  ))}
                </div>
              </section>
            </section>

            <section className="engine-mandatory-grid">
              <section className="card engine-panel">
                <div className="engine-panel-header">
                  <div>
                    <p className="section-title">Telegram Routing</p>
                    <h2 className="page-title">3 Gruppen klar getrennt</h2>
                  </div>
                  <span className="engine-header-note">Test / Approved / Rejected</span>
                </div>
                <div className="engine-card-grid">
                  {routingExamples.map((example) => (
                    <article key={example.title} className="engine-card engine-tone-info">
                      <div className="engine-card-head">
                        <p className="section-title">{example.title}</p>
                        <span className="status-chip info">beispiel</span>
                      </div>
                      <pre className="engine-code" style={{ margin: 0 }}>
                        {example.body}
                      </pre>
                    </article>
                  ))}
                </div>
              </section>

              <section className="card engine-panel">
                <div className="engine-panel-header">
                  <div>
                    <p className="section-title">Sicherheitscheck</p>
                    <h2 className="page-title">Hauptpost bleibt sauber</h2>
                  </div>
                  <span className="engine-header-note">PAAPI / Amazon / verifizierte Daten only</span>
                </div>
                <div className="engine-list">
                  {safetyChecks.map((line) => (
                    <article key={line} className="engine-list-item">
                      <strong>Check</strong>
                      <p>{line}</p>
                    </article>
                  ))}
                </div>
              </section>
            </section>

            <section className="card engine-panel">
              <div className="engine-panel-header">
                <div>
                  <p className="section-title">Produkt-Regeln</p>
                  <h2 className="page-title">Preisgrenzen und Produktgruppen direkt ueber die UI pflegen</h2>
                </div>
                <span className="engine-header-note">
                  {productRulesList.filter((rule) => rule.active).length} aktiv / {productRulesList.length} gesamt
                </span>
              </div>

              <p className="text-muted" style={{ margin: 0 }}>
                Diese Regeln greifen zusaetzlich vor `Veroeffentlicht` und vor jedem Optimized-Deal-Post. Bei unsicheren
                Daten bleibt der Deal draussen oder geht auf Review.
              </p>

              <div className="engine-result-grid">
                <article className="engine-card engine-tone-info">
                  <div className="engine-card-head">
                    <div>
                      <p className="section-title">Regel bearbeiten</p>
                      <h3 style={{ margin: 0 }}>{productRuleForm.id ? productRuleForm.name || 'Produkt-Regel' : 'Neue Produkt-Regel'}</h3>
                    </div>
                    <span className={`status-chip ${productRuleForm.active ? 'success' : 'danger'}`}>
                      {productRuleForm.active ? 'aktiv' : 'deaktiviert'}
                    </span>
                  </div>

                  <div className="engine-form-grid">
                    <label className="engine-span-2">
                      <span>Regelname</span>
                      <input
                        value={productRuleForm.name}
                        onChange={(event) => updateProductRuleForm('name', event.target.value)}
                        placeholder="China Kopfhoerer"
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label className="engine-span-2">
                      <span>Keywords / Produkttyp</span>
                      <input
                        value={productRuleForm.keywords}
                        onChange={(event) => updateProductRuleForm('keywords', event.target.value)}
                        placeholder="kopfhoerer, bluetooth kopfhoerer, earbuds"
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label>
                      <span>Markenart</span>
                      <select
                        value={productRuleForm.brandType}
                        onChange={(event) => updateProductRuleForm('brandType', event.target.value)}
                        disabled={user?.role !== 'admin'}
                      >
                        <option value="ANY">Egal</option>
                        <option value="NONAME">NoName</option>
                        <option value="BRAND">Marke</option>
                      </select>
                    </label>
                    <label>
                      <span>Maximalpreis</span>
                      <input
                        type="number"
                        step="0.01"
                        value={productRuleForm.maxPrice}
                        onChange={(event) => updateProductRuleForm('maxPrice', event.target.value)}
                        placeholder="12"
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label>
                      <span>Mindestbewertungen</span>
                      <input
                        type="number"
                        value={productRuleForm.minReviews}
                        onChange={(event) => updateProductRuleForm('minReviews', event.target.value)}
                        placeholder="50"
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label>
                      <span>Mindeststerne</span>
                      <input
                        type="number"
                        step="0.1"
                        value={productRuleForm.minRating}
                        onChange={(event) => updateProductRuleForm('minRating', event.target.value)}
                        placeholder="4.0"
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label>
                      <span>Kapazitaet Min mAh</span>
                      <input
                        type="number"
                        value={productRuleForm.capacityMin}
                        onChange={(event) => updateProductRuleForm('capacityMin', event.target.value)}
                        placeholder="19000"
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label>
                      <span>Kapazitaet Max mAh</span>
                      <input
                        type="number"
                        value={productRuleForm.capacityMax}
                        onChange={(event) => updateProductRuleForm('capacityMax', event.target.value)}
                        placeholder="30000"
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label className="engine-checkbox">
                      <span>Marktvergleich noetig</span>
                      <input
                        type="checkbox"
                        checked={productRuleForm.marketCompareRequired}
                        onChange={(event) => updateProductRuleForm('marketCompareRequired', event.target.checked)}
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                    <label className="engine-checkbox">
                      <span>Aktiv</span>
                      <input
                        type="checkbox"
                        checked={productRuleForm.active}
                        onChange={(event) => updateProductRuleForm('active', event.target.checked)}
                        disabled={user?.role !== 'admin'}
                      />
                    </label>
                  </div>

                  <div className="engine-actions">
                    <button
                      type="button"
                      className="secondary"
                      onClick={() => setProductRuleForm(buildProductRuleForm({ active: true }))}
                      disabled={user?.role !== 'admin'}
                    >
                      Regel hinzufuegen
                    </button>
                    <button type="button" className="primary" onClick={handleSaveProductRule} disabled={user?.role !== 'admin' || savingProductRule}>
                      {savingProductRule ? 'Speichert...' : productRuleForm.id ? 'Update / Speichern' : 'Regel speichern'}
                    </button>
                  </div>
                </article>

                <div className="engine-card-grid">
                  {productRulesList.map((rule) => (
                    <article
                      key={rule.id}
                      className={`engine-card engine-tone-${rule.active ? 'success' : 'danger'}`}
                      style={{ minHeight: 0 }}
                    >
                      <div className="engine-card-head">
                        <div>
                          <p className="section-title">Regel</p>
                          <h3>{rule.name}</h3>
                        </div>
                        <span className={`status-chip ${rule.active ? 'success' : 'danger'}`}>{rule.active ? 'aktiv' : 'aus'}</span>
                      </div>
                      <p>{(rule.keywords || []).join(', ') || 'Ohne Keywords'}</p>
                      <p>
                        Max {rule.maxPrice ?? '-'} EUR · Min Reviews {rule.minReviews ?? 0} · Min Sterne {rule.minRating ?? 0}
                      </p>
                      <p>
                        {rule.brandTypeLabel} · Marktvergleich {rule.marketCompareRequired ? 'ja' : 'nein'} · Kapazitaet{' '}
                        {rule.capacityMin ?? '-'} bis {rule.capacityMax ?? 'offen'}
                      </p>
                      <div className="engine-actions">
                        <button type="button" className="secondary" onClick={() => setProductRuleForm(buildProductRuleForm(rule))}>
                          Bearbeiten
                        </button>
                        <button
                          type="button"
                          className="secondary"
                          onClick={() => void handleToggleProductRule(rule, !rule.active)}
                          disabled={user?.role !== 'admin' || busyRuleId === rule.id}
                        >
                          {rule.active ? 'Regel deaktivieren' : 'Regel aktivieren'}
                        </button>
                        <button
                          type="button"
                          className="secondary"
                          onClick={() => void handleDeleteProductRule(rule)}
                          disabled={user?.role !== 'admin' || busyRuleId === rule.id}
                        >
                          Regel loeschen
                        </button>
                      </div>
                    </article>
                  ))}
                  {!productRulesList.length ? <p className="engine-empty">Noch keine Produkt-Regeln vorhanden.</p> : null}
                </div>
              </div>
            </section>

            <section className="card engine-panel">
              <div className="engine-panel-header">
                <div>
                  <p className="section-title">Analyse Engine</p>
                  <h2 className="page-title">Kompletter Deal-Durchlauf</h2>
                </div>
                <span className="engine-header-note">Produktregeln greifen zusaetzlich zu Marktvergleich und Keepa.</span>
              </div>

              <div className="engine-form-grid">
                <label>
                  <span>Quelle</span>
                  <input value={form.sourceName} onChange={(event) => updateForm('sourceName', event.target.value)} />
                </label>
                <label>
                  <span>Plattform</span>
                  <select value={form.sourcePlatform} onChange={(event) => updateForm('sourcePlatform', event.target.value)}>
                    <option value="telegram">telegram</option>
                    <option value="whatsapp">whatsapp</option>
                    <option value="internal">internal</option>
                  </select>
                </label>
                <label>
                  <span>Source Type</span>
                  <input value={form.sourceType} onChange={(event) => updateForm('sourceType', event.target.value)} />
                </label>
                <label>
                  <span>Seller</span>
                  <select value={form.sellerType} onChange={(event) => updateForm('sellerType', event.target.value)}>
                    <option value="AMAZON">AMAZON</option>
                    <option value="FBA">FBA</option>
                    <option value="FBM">FBM</option>
                    <option value="UNKNOWN">UNKNOWN</option>
                  </select>
                </label>
                <label>
                  <span>Seller Klasse</span>
                  <select value={form.sellerClass} onChange={(event) => updateForm('sellerClass', event.target.value)}>
                    <option value="AMAZON_DIRECT">AMAZON_DIRECT</option>
                    <option value="FBA_THIRDPARTY">FBA_THIRDPARTY</option>
                    <option value="FBM_THIRDPARTY">FBM_THIRDPARTY</option>
                    <option value="UNKNOWN">UNKNOWN</option>
                  </select>
                </label>
                <label>
                  <span>Verkauf durch Amazon</span>
                  <select value={form.soldByAmazon} onChange={(event) => updateForm('soldByAmazon', event.target.value)}>
                    <option value="yes">ja</option>
                    <option value="no">nein</option>
                    <option value="unknown">unbekannt</option>
                  </select>
                </label>
                <label>
                  <span>Versand durch Amazon</span>
                  <select value={form.shippedByAmazon} onChange={(event) => updateForm('shippedByAmazon', event.target.value)}>
                    <option value="yes">ja</option>
                    <option value="no">nein</option>
                    <option value="unknown">unbekannt</option>
                  </select>
                </label>
                <label className="engine-span-2">
                  <span>Titel</span>
                  <input value={form.title} onChange={(event) => updateForm('title', event.target.value)} />
                </label>
                <label className="engine-span-2">
                  <span>Amazon URL</span>
                  <input value={form.amazonUrl} onChange={(event) => updateForm('amazonUrl', event.target.value)} />
                </label>
                <label>
                  <span>Amazon Preis</span>
                  <input value={form.amazonPrice} onChange={(event) => updateForm('amazonPrice', event.target.value)} />
                </label>
                <label>
                  <span>Brand</span>
                  <input value={form.brand} onChange={(event) => updateForm('brand', event.target.value)} />
                </label>
                <label>
                  <span>Kategorie</span>
                  <input value={form.category} onChange={(event) => updateForm('category', event.target.value)} />
                </label>
                <label>
                  <span>Rating</span>
                  <input value={form.rating} onChange={(event) => updateForm('rating', event.target.value)} placeholder="4.2" />
                </label>
                <label>
                  <span>Rezensionen</span>
                  <input value={form.reviewCount} onChange={(event) => updateForm('reviewCount', event.target.value)} placeholder="100" />
                </label>
                <label>
                  <span>Variante</span>
                  <input value={form.variantKey} onChange={(event) => updateForm('variantKey', event.target.value)} />
                </label>
                <label>
                  <span>Menge / Set</span>
                  <input value={form.quantityKey} onChange={(event) => updateForm('quantityKey', event.target.value)} />
                </label>
                <label>
                  <span>Tag/Nacht Override</span>
                  <select value={form.overrideDayPart} onChange={(event) => updateForm('overrideDayPart', event.target.value)}>
                    <option value="day">day</option>
                    <option value="night">night</option>
                  </select>
                </label>
                <label className="engine-checkbox">
                  <span>Markenprodukt</span>
                  <input type="checkbox" checked={form.isBrandProduct} onChange={(event) => updateForm('isBrandProduct', event.target.checked)} />
                </label>
                <label className="engine-checkbox">
                  <span>No-Name</span>
                  <input type="checkbox" checked={form.isNoName} onChange={(event) => updateForm('isNoName', event.target.checked)} />
                </label>
                <label className="engine-checkbox">
                  <span>China Produkt</span>
                  <input type="checkbox" checked={form.isChinaProduct} onChange={(event) => updateForm('isChinaProduct', event.target.checked)} />
                </label>
                <label className="engine-span-2">
                  <span>Marktangebote JSON</span>
                  <textarea value={form.marketOffersJson} onChange={(event) => updateForm('marketOffersJson', event.target.value)} rows={12} />
                </label>
                <label>
                  <span>Keepa JSON</span>
                  <textarea value={form.keepaJson} onChange={(event) => updateForm('keepaJson', event.target.value)} rows={12} />
                </label>
                <label>
                  <span>AI JSON</span>
                  <textarea value={form.aiJson} onChange={(event) => updateForm('aiJson', event.target.value)} rows={12} />
                </label>
              </div>

              <div className="engine-actions">
                <button type="button" className="secondary" onClick={() => setForm(buildFormFromSample(samplePayload || {}))}>
                  Sample laden
                </button>
                <button type="button" className="primary" disabled={analyzing} onClick={handleAnalyze}>
                  {analyzing ? 'Analysiert...' : 'Analyse starten'}
                </button>
              </div>
            </section>

            <section className="engine-result-grid">
              <section className="card engine-panel">
                <div className="engine-panel-header">
                  <div>
                    <p className="section-title">Analyse Output</p>
                    <h2 className="page-title">Finale Entscheidung</h2>
                  </div>
                  <span className="engine-header-note">Letztes Ergebnis {currentResult ? formatDateTime(currentResult.createdAt) : '-'}</span>
                </div>
                {currentResult ? (
                  <>
                    <div className="engine-card-grid">
                      <article className={`engine-card engine-tone-${getToneClass(currentResult.decision)}`}>
                        <div className="engine-card-head">
                          <p className="section-title">Entscheidung</p>
                          <span className={`status-chip ${getToneClass(currentResult.decision)}`}>{currentResult.decision}</span>
                        </div>
                        <h3>{currentResult.analysis?.decisionSourceLabel || currentResult.analysis?.decisionSource || '-'}</h3>
                        <p>{currentResult.decisionReason}</p>
                      </article>
                      <article className={`engine-card engine-tone-${productRules?.status === 'matched' ? 'warning' : 'info'}`}>
                        <div className="engine-card-head">
                          <p className="section-title">Produktregeln</p>
                          <span className={`status-chip ${productRules?.status === 'matched' ? 'warning' : 'info'}`}>{productRules?.status || 'clear'}</span>
                        </div>
                        <h3>{productRules?.matchedRuleName || productRules?.action || 'none'}</h3>
                        <p>{productRules?.summary || 'Keine Produktregel ausgelost.'}</p>
                      </article>
                      <article className="engine-card engine-tone-info">
                        <div className="engine-card-head">
                          <p className="section-title">Markt</p>
                          <span className="status-chip info">{currentResult.marketOfferCount || 0} gueltig</span>
                        </div>
                        <h3>{currentResult.marketAdvantagePct ?? '-'}%</h3>
                        <p>Marktpreis {currentResult.marketPrice ?? '-'} | Schwelle {currentResult.analysis?.thresholdPct ?? '-'}</p>
                      </article>
                      <article className="engine-card engine-tone-info">
                        <div className="engine-card-head">
                          <p className="section-title">Keepa</p>
                          <span className="status-chip info">{currentResult.analysis?.fallbackUsed ? 'fallback' : 'idle'}</span>
                        </div>
                        <h3>{currentResult.keepaScore ?? '-'}</h3>
                        <p>avg90 {currentResult.keepaDiscount90 ?? '-'}% | avg180 {currentResult.keepaDiscount180 ?? '-'}%</p>
                      </article>
                    </div>
                    <div className="engine-list">
                      {(currentResult.analysis?.reasons || []).map((reason, index) => (
                        <article key={`${currentResult.id}-reason-${index}`} className="engine-list-item">
                          <strong>Reason {index + 1}</strong>
                          <p>{reason}</p>
                        </article>
                      ))}
                    </div>
                    <div className="engine-code-grid">
                      <div>
                        <p className="section-title">Flow</p>
                        <pre className="engine-code">{prettyJson(currentResult.analysis?.flow || [])}</pre>
                      </div>
                      <div>
                        <p className="section-title">Analyse JSON</p>
                        <pre className="engine-code">{prettyJson(currentResult.analysis || {})}</pre>
                      </div>
                    </div>
                  </>
                ) : (
                  <p className="engine-empty">Noch kein Analyseergebnis vorhanden.</p>
                )}
              </section>

              <section className="card engine-panel">
                <div className="engine-panel-header">
                  <div>
                    <p className="section-title">Timeline und Fehler</p>
                    <h2 className="page-title">Letzte Runs</h2>
                  </div>
                  <span className="engine-header-note">{dashboard?.metrics?.totalRuns || 0} Gesamt-Runs</span>
                </div>
                <div className="engine-list">
                  {(dashboard?.timeline || []).length ? (
                    dashboard.timeline.map((entry) => (
                      <article key={entry.id} className="engine-list-item">
                        <div className="engine-card-head">
                          <strong>{entry.title}</strong>
                          <span className={`status-chip ${entry.tone}`}>{formatDateTime(entry.createdAt)}</span>
                        </div>
                        <p>{entry.detail}</p>
                      </article>
                    ))
                  ) : (
                    <p className="engine-empty">Noch keine Runs vorhanden.</p>
                  )}
                </div>
                <div className="engine-divider" />
                <div className="engine-list">
                  {(dashboard?.errors || []).length ? (
                    dashboard.errors.map((entry) => (
                      <article key={entry.id} className="engine-list-item">
                        <div className="engine-card-head">
                          <strong>{entry.title}</strong>
                          <span className={`status-chip ${entry.tone}`}>{entry.tone}</span>
                        </div>
                        <p>{entry.detail}</p>
                      </article>
                    ))
                  ) : (
                    <p className="engine-empty">Keine aktuellen Fehlerhinweise.</p>
                  )}
                </div>
              </section>
            </section>
              </>
            ) : null}
          </>
        )}
      </div>
    </Layout>
  );
}

export default DealEnginePage;
