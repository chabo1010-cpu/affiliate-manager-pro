import { useEffect, useMemo, useRef, useState } from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';
import './Keepa.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';
const FALLBACK_IMAGE =
  'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="72" height="72"><rect width="72" height="72" rx="18" fill="%230f172a"/><text x="50%" y="52%" dominant-baseline="middle" text-anchor="middle" fill="%2394a3b8" font-family="Arial" font-size="14">AM</text></svg>';

const keepaTabs = [
  { label: 'Flow Dashboard', path: '/keepa' },
  { label: 'Manuelle Suche', path: '/keepa/manual-search' },
  { label: 'Automatik', path: '/keepa/automatik' },
  { label: 'Ergebnisse', path: '/keepa/ergebnisse' },
  { label: 'Benachrichtigungen', path: '/keepa/benachrichtigungen' },
  { label: 'Verbrauch & Logs', path: '/keepa/verbrauch-logs' },
  { label: 'Fake-Drop Analyse', path: '/keepa/fake-drop-analyse' },
  { label: 'Review Queue', path: '/keepa/review-queue' },
  { label: 'Lern-Datenbank', path: '/keepa/lern-datenbank' },
  { label: 'Einstellungen', path: '/keepa/einstellungen' }
];

function normalizeLearningTabPath(pathname) {
  if (typeof pathname !== 'string' || !pathname.trim()) {
    return '/keepa';
  }

  if (pathname.startsWith('/learning')) {
    const nextPath = pathname.replace('/learning', '/keepa');
    return nextPath || '/keepa';
  }

  return pathname;
}

function buildLearningTabPath(basePath, canonicalPath) {
  if (basePath === '/keepa') {
    return canonicalPath;
  }

  return canonicalPath.replace('/keepa', '/learning');
}

const sellerTypeOptions = [
  { value: 'ALL', label: 'Alle' },
  { value: 'AMAZON', label: 'Amazon' },
  { value: 'FBA', label: 'FBA' },
  { value: 'FBM', label: 'FBM' }
];

const keepaDrawerCatalog = [
  { key: 'AMAZON', label: 'Amazon', description: 'Verkauf und Versand durch Amazon.' },
  { key: 'FBA', label: 'FBA', description: 'Verkauf durch Haendler, Versand durch Amazon.' },
  { key: 'FBM', label: 'FBM', description: 'Verkauf und Versand durch Haendler.' }
];

const keepaTrendIntervalOptions = [
  { value: 'day', label: 'Tag' },
  { value: 'week', label: 'Woche' },
  { value: 'month', label: 'Monat' },
  { value: 'three_months', label: '3 Monate' },
  { value: 'all', label: 'Alle' }
];

const keepaSortOptions = [
  { value: 'percent', label: 'Prozent' },
  { value: 'price_drop', label: 'Preissturz' },
  { value: 'price', label: 'Preis' },
  { value: 'newest', label: 'Neueste' },
  { value: 'sales_rank', label: 'Sales Rank' }
];

const keepaAmazonOfferOptions = [
  { value: 'all', label: 'egal' },
  { value: 'require', label: 'nur mit Amazon-Angebot' },
  { value: 'exclude', label: 'kein Amazon-Angebot' }
];

const defaultDrawerConfigs = {
  AMAZON: {
    active: true,
    sellerType: 'AMAZON',
    patternSupportEnabled: true,
    trendInterval: 'week',
    minDiscount: 20,
    minPrice: '',
    maxPrice: '',
    categories: [],
    onlyPrime: false,
    onlyInStock: true,
    onlyGoodRating: false,
    onlyWithReviews: true,
    amazonOfferMode: 'require',
    singleVariantOnly: false,
    recentPriceChangeOnly: false,
    sortBy: 'percent',
    autoModeAllowed: true,
    testGroupPostingAllowed: true
  },
  FBA: {
    active: true,
    sellerType: 'FBA',
    patternSupportEnabled: true,
    trendInterval: 'week',
    minDiscount: 25,
    minPrice: '',
    maxPrice: '',
    categories: [],
    onlyPrime: false,
    onlyInStock: true,
    onlyGoodRating: false,
    onlyWithReviews: true,
    amazonOfferMode: 'exclude',
    singleVariantOnly: false,
    recentPriceChangeOnly: false,
    sortBy: 'percent',
    autoModeAllowed: true,
    testGroupPostingAllowed: true
  },
  FBM: {
    active: true,
    sellerType: 'FBM',
    patternSupportEnabled: true,
    trendInterval: 'month',
    minDiscount: 35,
    minPrice: '',
    maxPrice: '',
    categories: [],
    onlyPrime: false,
    onlyInStock: true,
    onlyGoodRating: false,
    onlyWithReviews: true,
    amazonOfferMode: 'exclude',
    singleVariantOnly: true,
    recentPriceChangeOnly: false,
    sortBy: 'percent',
    autoModeAllowed: true,
    testGroupPostingAllowed: true
  }
};

const workflowStatusOptions = [
  { value: '', label: 'Alle Stati' },
  { value: 'neu', label: 'Neu' },
  { value: 'geprueft', label: 'Geprueft' },
  { value: 'alert_gesendet', label: 'Alert gesendet' },
  { value: 'verworfen', label: 'Verworfen' }
];

const usageRangeOptions = [
  { value: 'today', label: 'Heute' },
  { value: 'week', label: 'Woche' },
  { value: 'month', label: 'Monat' }
];

const strengthLabels = {
  pruefenswert: 'Pruefenswert',
  stark: 'Stark',
  verwerfen: 'Verwerfen'
};

const workflowLabels = {
  neu: 'Neu',
  geprueft: 'Geprueft',
  alert_gesendet: 'Alert gesendet',
  verworfen: 'Verworfen'
};

const reviewLabelOptions = [
  { value: 'good', label: 'Good' },
  { value: 'fake', label: 'Fake' },
  { value: 'weak', label: 'Weak' },
  { value: 'review', label: 'Review' }
];

const reviewLabelAliases = {
  approved: 'good',
  strong_deal: 'good',
  ja: 'good',
  fake_drop: 'fake',
  rejected: 'fake',
  nein: 'fake',
  weak_deal: 'weak',
  eventuell_gut: 'review',
  ueberspringen: 'review'
};

const exampleBucketOptions = [
  { value: '', label: 'Alle Buckets' },
  { value: 'positive', label: 'Positive Beispiele' },
  { value: 'negative', label: 'Negative Beispiele' },
  { value: 'unsicher', label: 'Unsichere Beispiele' }
];

const reviewTagOptions = [
  { value: 'echter_deal', label: 'echter Deal' },
  { value: 'fake_drop', label: 'Fake-Drop' },
  { value: 'coupon_verdacht', label: 'Coupon-Verdacht' },
  { value: 'fba_fbm_trick', label: 'FBA/FBM-Trick' },
  { value: 'amazon_sauber', label: 'Amazon sauber' },
  { value: 'unsicher', label: 'unsicher' }
];

const fakeDropFilterOptions = [
  { value: '', label: 'Alle Bewertungen' },
  { value: 'verdaechtig', label: 'nur verdaechtig' },
  { value: 'manuelle_pruefung', label: 'nur unsicher' },
  { value: 'wahrscheinlicher_fake_drop', label: 'nur Fake-Drop-Verdacht' },
  { value: 'amazon_stabil', label: 'nur Amazon stabil' },
  { value: 'echter_deal', label: 'nur echte Deals' }
];

function normalizeReviewUiValue(value) {
  if (!value) {
    return 'review';
  }

  return reviewLabelAliases[value] || value;
}

function getReviewOptionMeta(value) {
  const normalized = normalizeReviewUiValue(value);
  return reviewLabelOptions.find((item) => item.value === normalized) || reviewLabelOptions[3];
}

function isNegativeReviewValue(value) {
  return ['fake', 'weak'].includes(normalizeReviewUiValue(value));
}

function formatDateTime(value) {
  if (!value) {
    return '-';
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat('de-DE', {
    dateStyle: 'short',
    timeStyle: 'short'
  }).format(parsed);
}

function formatCurrency(value) {
  if (value === null || value === undefined || value === '') {
    return '-';
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return '-';
  }

  return new Intl.NumberFormat('de-DE', {
    style: 'currency',
    currency: 'EUR'
  }).format(parsed);
}

function formatPercent(value) {
  if (value === null || value === undefined || value === '') {
    return '-';
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return '-';
  }

  return `${parsed.toFixed(1)}%`;
}

function formatUsage(value) {
  if (value === null || value === undefined || value === '') {
    return '-';
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return '-';
  }

  return `${parsed.toFixed(1)} Credits`;
}

function formatDuration(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return '-';
  }

  if (parsed < 1000) {
    return `${Math.round(parsed)} ms`;
  }

  if (parsed < 60000) {
    return `${(parsed / 1000).toFixed(1)} s`;
  }

  return `${(parsed / 60000).toFixed(1)} Min.`;
}

function formatKeepaModeLabel(value) {
  const normalized = String(value || '').toLowerCase();
  return (
    {
      manual: 'manuell',
      auto: 'auto',
      test: 'test'
    }[normalized] || '-'
  );
}

function shortenText(value, maxLength = 110) {
  if (!value) {
    return '-';
  }

  const text = String(value).replace(/\s+/g, ' ').trim();
  if (text.length <= maxLength) {
    return text;
  }

  return `${text.slice(0, Math.max(0, maxLength - 1)).trimEnd()}...`;
}

function normalizeDrawerKey(value) {
  return keepaDrawerCatalog.some((item) => item.key === value) ? value : 'AMAZON';
}

function buildDrawerConfigs(settings) {
  return Object.fromEntries(
    keepaDrawerCatalog.map((drawer) => [
      drawer.key,
      {
        ...defaultDrawerConfigs[drawer.key],
        ...(settings?.drawerConfigs?.[drawer.key] || {})
      }
    ])
  );
}

function buildManualFilters(settings, drawerKey = 'AMAZON') {
  const resolvedDrawerKey = normalizeDrawerKey(drawerKey);
  const drawerConfig = buildDrawerConfigs(settings)[resolvedDrawerKey];

  return {
    drawerKey: resolvedDrawerKey,
    page: 1,
    limit: settings?.defaultPageSize || 24,
    minDiscount: drawerConfig.minDiscount ?? (settings?.defaultDiscount || 40),
    sellerType: drawerConfig.sellerType || settings?.defaultSellerType || 'ALL',
    categories: [...(drawerConfig.categories || settings?.defaultCategories || [])],
    minPrice: drawerConfig.minPrice ?? settings?.defaultMinPrice ?? '',
    maxPrice: drawerConfig.maxPrice ?? settings?.defaultMaxPrice ?? '',
    trendInterval: drawerConfig.trendInterval || 'week',
    sortBy: drawerConfig.sortBy || 'percent',
    onlyPrime: Boolean(drawerConfig.onlyPrime),
    onlyInStock: drawerConfig.onlyInStock !== false,
    onlyGoodRating: Boolean(drawerConfig.onlyGoodRating),
    onlyWithReviews: Boolean(drawerConfig.onlyWithReviews),
    amazonOfferMode: drawerConfig.amazonOfferMode || 'all',
    singleVariantOnly: Boolean(drawerConfig.singleVariantOnly),
    recentPriceChangeOnly: Boolean(drawerConfig.recentPriceChangeOnly)
  };
}

function buildRuleForm(settings, drawerKey = 'AMAZON') {
  const resolvedDrawerKey = normalizeDrawerKey(drawerKey);
  const drawerConfig = buildDrawerConfigs(settings)[resolvedDrawerKey];

  return {
    id: 0,
    name: '',
    drawerKey: resolvedDrawerKey,
    minDiscount: drawerConfig.minDiscount ?? (settings?.defaultDiscount || 40),
    sellerType: drawerConfig.sellerType || settings?.defaultSellerType || 'ALL',
    categories: [...(drawerConfig.categories || settings?.defaultCategories || [])],
    minPrice: drawerConfig.minPrice ?? settings?.defaultMinPrice ?? '',
    maxPrice: drawerConfig.maxPrice ?? settings?.defaultMaxPrice ?? '',
    minDealScore: 70,
    intervalMinutes: settings?.defaultIntervalMinutes || 60,
    trendInterval: drawerConfig.trendInterval || 'week',
    sortBy: drawerConfig.sortBy || 'percent',
    onlyPrime: Boolean(drawerConfig.onlyPrime),
    onlyInStock: drawerConfig.onlyInStock !== false,
    onlyGoodRating: Boolean(drawerConfig.onlyGoodRating),
    onlyWithReviews: Boolean(drawerConfig.onlyWithReviews),
    amazonOfferMode: drawerConfig.amazonOfferMode || 'all',
    singleVariantOnly: Boolean(drawerConfig.singleVariantOnly),
    recentPriceChangeOnly: Boolean(drawerConfig.recentPriceChangeOnly),
    comparisonSources: ['manual-source'],
    isActive: true
  };
}

function buildSettingsForm(settings) {
  return {
    keepaEnabled: Boolean(settings?.keepaEnabled),
    schedulerEnabled: Boolean(settings?.schedulerEnabled),
    domainId: settings?.domainId || 3,
    defaultCategories: settings?.defaultCategories || [],
    defaultDiscount: settings?.defaultDiscount || 40,
    defaultSellerType: settings?.defaultSellerType || 'ALL',
    defaultMinPrice: settings?.defaultMinPrice ?? '',
    defaultMaxPrice: settings?.defaultMaxPrice ?? '',
    defaultPageSize: settings?.defaultPageSize || 24,
    defaultIntervalMinutes: settings?.defaultIntervalMinutes || 60,
    strongDealMinDiscount: settings?.strongDealMinDiscount || 40,
    strongDealMinComparisonGapPct: settings?.strongDealMinComparisonGapPct || 10,
    goodRatingThreshold: settings?.goodRatingThreshold || 4,
    alertTelegramEnabled: Boolean(settings?.alertTelegramEnabled),
    alertInternalEnabled: Boolean(settings?.alertInternalEnabled),
    alertWhatsappPlaceholderEnabled: Boolean(settings?.alertWhatsappPlaceholderEnabled),
    alertCooldownMinutes: settings?.alertCooldownMinutes || 180,
    alertMaxPerProduct: settings?.alertMaxPerProduct || 2,
    telegramMessagePrefix: settings?.telegramMessagePrefix || 'Keepa Alert',
    comparisonSourceConfig: settings?.comparisonSourceConfig || {
      'manual-source': { enabled: true },
      idealo: { enabled: false },
      'custom-api': { enabled: false }
    },
    drawerConfigs: buildDrawerConfigs(settings),
    loggingEnabled: Boolean(settings?.loggingEnabled),
    estimatedTokensPerManualRun: settings?.estimatedTokensPerManualRun || 8
  };
}

function buildFakeDropSettingsForm(settings) {
  return {
    engineEnabled: Boolean(settings?.engineEnabled),
    lowRiskThreshold: settings?.lowRiskThreshold ?? 32,
    highRiskThreshold: settings?.highRiskThreshold ?? 72,
    reviewPriorityThreshold: settings?.reviewPriorityThreshold ?? 58,
    amazonConfidenceStrong: settings?.amazonConfidenceStrong ?? 72,
    stabilityStrong: settings?.stabilityStrong ?? 66,
    referenceInflationThreshold: settings?.referenceInflationThreshold ?? 22,
    volatilityWarningThreshold: settings?.volatilityWarningThreshold ?? 18,
    shortPeakMaxDays: settings?.shortPeakMaxDays ?? 3,
    spikeSensitivity: settings?.spikeSensitivity ?? 16,
    reboundWindowDays: settings?.reboundWindowDays ?? 7,
    weights: {
      stability: settings?.weights?.stability ?? 1,
      manipulation: settings?.weights?.manipulation ?? 1,
      amazon: settings?.weights?.amazon ?? 1,
      feedback: settings?.weights?.feedback ?? 1
    }
  };
}

function buildReviewFilters() {
  return {
    sellerType: 'ALL',
    classification: '',
    onlyUnlabeled: true,
    page: 1,
    limit: 8
  };
}

function buildExampleFilters() {
  return {
    bucket: '',
    label: '',
    sellerType: 'ALL',
    search: '',
    page: 1,
    limit: 8
  };
}

function getWorkflowChip(status) {
  if (status === 'verworfen') {
    return 'danger';
  }

  if (status === 'alert_gesendet') {
    return 'success';
  }

  if (status === 'geprueft') {
    return 'info';
  }

  return 'warning';
}

function getStrengthChip(strength) {
  if (strength === 'stark') {
    return 'success';
  }

  if (strength === 'verwerfen') {
    return 'danger';
  }

  return 'warning';
}

function getFakeDropChip(classification) {
  if (classification === 'echter_deal' || classification === 'amazon_stabil') {
    return 'success';
  }

  if (classification === 'wahrscheinlicher_fake_drop') {
    return 'danger';
  }

  if (classification === 'verdaechtig') {
    return 'warning';
  }

  return 'info';
}

function getFlowStatusTone(value) {
  if (value === 'connected' || value === 'approved' || value === 'sent' || value === 'active') {
    return 'success';
  }

  if (value === 'blocked' || value === 'inactive') {
    return 'danger';
  }

  if (value === 'review' || value === 'optional') {
    return 'warning';
  }

  return 'info';
}

function estimateUsagePreview(filters, settings) {
  const base = Number(settings?.estimatedTokensPerManualRun || 8);
  const categories = Array.isArray(filters?.categories) ? filters.categories.length : 0;
  const limit = Number(filters?.limit || settings?.defaultPageSize || 24);
  const limitFactor = Math.max(0, Math.ceil(limit / 24) - 1);
  const categoryFactor = categories ? Math.ceil(categories / 3) : 0;
  const priceFactor = filters?.minPrice !== '' || filters?.maxPrice !== '' ? 1 : 0;
  const qualityFactor = [filters?.onlyPrime, filters?.onlyInStock, filters?.onlyGoodRating].filter(Boolean).length;

  return Math.max(1, Math.min(250, base + limitFactor + categoryFactor + priceFactor + qualityFactor));
}

function buildLinePath(data, width, height, valueKey) {
  const safeData = Array.isArray(data) ? data : [];
  const values = safeData.map((item) => Number(item?.[valueKey] || 0));
  const maxValue = Math.max(...values, 1);

  return safeData
    .map((item, index) => {
      const x = safeData.length === 1 ? width / 2 : (index / Math.max(safeData.length - 1, 1)) * width;
      const y = height - ((Number(item?.[valueKey] || 0) / maxValue) * (height - 20) + 10);
      return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(' ');
}

function MiniLineChart({ data, valueKey, color }) {
  const safeData = Array.isArray(data) ? data : [];

  if (!safeData.length || safeData.every((item) => Number(item?.[valueKey] || 0) === 0)) {
    return <div className="keepa-chart-empty">Noch keine Verlaufdaten.</div>;
  }

  const width = 620;
  const height = 220;
  const values = safeData.map((item) => Number(item?.[valueKey] || 0));
  const maxValue = Math.max(...values, 1);
  const path = buildLinePath(safeData, width, height, valueKey);

  return (
    <div className="keepa-chart-shell">
      <svg viewBox={`0 0 ${width} ${height}`} className="keepa-chart-svg" role="img" aria-label="Keepa Verlauf">
        {[0, 0.25, 0.5, 0.75, 1].map((step) => {
          const y = 10 + (height - 20) * step;
          return <line key={step} x1="0" y1={y} x2={width} y2={y} className="keepa-chart-grid" />;
        })}
        <path d={path} fill="none" stroke={color} strokeWidth="4" strokeLinecap="round" strokeLinejoin="round" />
        {safeData.map((item, index) => {
          const x = safeData.length === 1 ? width / 2 : (index / Math.max(safeData.length - 1, 1)) * width;
          const y = height - ((Number(item?.[valueKey] || 0) / maxValue) * (height - 20) + 10);

          return <circle key={`${item.date}-${index}`} cx={x} cy={y} r="4" fill={color} />;
        })}
      </svg>
      <div className="keepa-chart-labels">
        {safeData.map((item) => (
          <span key={item.date}>{item.label}</span>
        ))}
      </div>
    </div>
  );
}

function MiniBarChart({ data, valueKey, color, valueFormatter }) {
  const safeData = Array.isArray(data) ? data : [];
  const maxValue = Math.max(...safeData.map((item) => Number(item?.[valueKey] || 0)), 1);

  if (!safeData.length || safeData.every((item) => Number(item?.[valueKey] || 0) === 0)) {
    return <div className="keepa-chart-empty">Noch keine Daten fuer dieses Diagramm.</div>;
  }

  return (
    <div className="keepa-bar-chart">
      {safeData.map((item) => {
        const value = Number(item?.[valueKey] || 0);
        const height = `${Math.max(8, (value / maxValue) * 100)}%`;

        return (
          <div key={`${item.label}-${item.module || item.date}`} className="keepa-bar-chart-item">
            <span className="keepa-bar-chart-value">{valueFormatter ? valueFormatter(value) : value}</span>
            <div className="keepa-bar-chart-column">
              <span className="keepa-bar-chart-fill" style={{ height, background: color }} />
            </div>
            <small>{item.label}</small>
          </div>
        );
      })}
    </div>
  );
}

function MiniDonutChart({ data, valueKey, valueFormatter }) {
  const safeData = (Array.isArray(data) ? data : []).filter((item) => Number(item?.[valueKey] || 0) > 0);
  const total = safeData.reduce((sum, item) => sum + Number(item?.[valueKey] || 0), 0);
  const palette = ['#10b981', '#22c55e', '#38bdf8', '#f59e0b', '#f97316', '#ef4444'];
  let offset = 0;

  if (!safeData.length || total <= 0) {
    return <div className="keepa-chart-empty">Keine Verteilung verfuegbar.</div>;
  }

  return (
    <div className="keepa-donut-layout">
      <svg viewBox="0 0 42 42" className="keepa-donut-chart" role="img" aria-label="Nutzungsverteilung">
        {safeData.map((item, index) => {
          const value = Number(item?.[valueKey] || 0);
          const ratio = (value / total) * 100;
          const circle = (
            <circle
              key={`${item.label}-${index}`}
              cx="21"
              cy="21"
              r="15.915"
              fill="none"
              stroke={palette[index % palette.length]}
              strokeWidth="6"
              strokeDasharray={`${ratio} ${100 - ratio}`}
              strokeDashoffset={25 - offset}
            />
          );
          offset += ratio;
          return circle;
        })}
        <circle cx="21" cy="21" r="10.5" fill="#0f172a" />
        <text x="21" y="21" textAnchor="middle" dominantBaseline="middle" className="keepa-donut-label">
          {total.toFixed(0)}
        </text>
      </svg>

      <div className="keepa-donut-legend">
        {safeData.map((item, index) => (
          <div key={`${item.label}-${index}`} className="keepa-legend-item">
            <span className="keepa-legend-dot" style={{ background: palette[index % palette.length] }} />
            <span>{item.label}</span>
            <strong>{valueFormatter ? valueFormatter(item[valueKey]) : item[valueKey]}</strong>
          </div>
        ))}
      </div>
    </div>
  );
}

function MiniPriceHistory({ points, accent = '#38bdf8' }) {
  const safePoints = Array.isArray(points) ? points : [];
  if (!safePoints.length) {
    return <div className="keepa-mini-chart-empty">Kein Verlauf gespeichert.</div>;
  }

  const width = 260;
  const height = 84;
  const values = safePoints.map((item) => Number(item?.price || 0));
  const maxValue = Math.max(...values, 1);
  const minValue = Math.min(...values, maxValue);
  const scale = Math.max(maxValue - minValue, 1);
  const path = safePoints
    .map((item, index) => {
      const x = safePoints.length === 1 ? width / 2 : (index / Math.max(safePoints.length - 1, 1)) * width;
      const y = height - (((Number(item?.price || 0) - minValue) / scale) * (height - 18) + 9);
      return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(' ');

  return (
    <div className="keepa-mini-chart-card">
      <svg viewBox={`0 0 ${width} ${height}`} className="keepa-mini-chart-svg" role="img" aria-label="Preisverlauf">
        <path d={path} fill="none" stroke={accent} strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
      <div className="keepa-mini-chart-labels">
        <span>{safePoints[0]?.label || '-'}</span>
        <strong>{formatCurrency(safePoints[safePoints.length - 1]?.price)}</strong>
      </div>
    </div>
  );
}

function KeepaPage() {
  const { user } = useAuth();
  const isAdmin = user?.role === 'admin';
  const location = useLocation();
  const initializedRef = useRef(false);
  const ruleInitializedRef = useRef(false);
  const fakeDropSettingsInitializedRef = useRef(false);
  const canonicalPath = useMemo(() => normalizeLearningTabPath(location.pathname), [location.pathname]);
  const navigationBasePath = location.pathname.startsWith('/learning') ? '/learning' : '/keepa';

  const currentTab = useMemo(() => {
    const match = keepaTabs.find((item) => item.path === canonicalPath);
    return match?.path || '/keepa';
  }, [canonicalPath]);

  const [bootLoading, setBootLoading] = useState(true);
  const [manualLoading, setManualLoading] = useState(false);
  const [savingRule, setSavingRule] = useState(false);
  const [savingSettings, setSavingSettings] = useState(false);
  const [testingConnection, setTestingConnection] = useState(false);
  const [testingAlert, setTestingAlert] = useState(false);
  const [savingFakeDropSettings, setSavingFakeDropSettings] = useState(false);
  const [recalculatingFakeDrop, setRecalculatingFakeDrop] = useState(false);
  const [reviewBusyId, setReviewBusyId] = useState(0);
  const [statusMessage, setStatusMessage] = useState('');
  const [statusData, setStatusData] = useState(null);
  const [amazonStatusData, setAmazonStatusData] = useState(null);
  const [copybotOverview, setCopybotOverview] = useState(null);
  const [learningOverview, setLearningOverview] = useState(null);
  const [usageSummary, setUsageSummary] = useState(null);
  const [usageHistory, setUsageHistory] = useState({
    series: [],
    sourceBreakdown: [],
    range: { days: 30, module: 'all' },
    usageModeLabel: ''
  });
  const [usageLogs, setUsageLogs] = useState({
    items: [],
    filters: { range: 'today', module: 'all', limit: 40 },
    availableModules: []
  });
  const [rules, setRules] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [fakeDropSummary, setFakeDropSummary] = useState(null);
  const [fakeDropHistory, setFakeDropHistory] = useState({
    series: [],
    sellerBreakdown: [],
    patternBreakdown: [],
    range: { days: 30 }
  });
  const [reviewQueue, setReviewQueue] = useState({
    items: [],
    pagination: { page: 1, limit: 8, total: 0, totalPages: 1 },
    filters: buildReviewFilters()
  });
  const [exampleLibrary, setExampleLibrary] = useState({
    items: [],
    pagination: { page: 1, limit: 8, total: 0, totalPages: 1 },
    filters: buildExampleFilters(),
    counts: { positive: 0, negative: 0, unsicher: 0 }
  });
  const [reviewFilters, setReviewFilters] = useState(buildReviewFilters());
  const [exampleFilters, setExampleFilters] = useState(buildExampleFilters());
  const [reviewDrafts, setReviewDrafts] = useState({});
  const [fakeDropSettingsForm, setFakeDropSettingsForm] = useState(buildFakeDropSettingsForm());
  const [results, setResults] = useState({
    items: [],
    pagination: { page: 1, limit: 20, total: 0, totalPages: 1 }
  });
  const [resultsFilters, setResultsFilters] = useState({
    workflowStatus: '',
    categoryId: '',
    minDiscount: '',
    minDealScore: '',
    page: 1,
    limit: 20
  });
  const [usageFilters, setUsageFilters] = useState({
    range: 'today',
    module: 'all',
    limit: 40,
    days: 30
  });
  const [activeManualDrawer, setActiveManualDrawer] = useState('AMAZON');
  const [activeAutomationDrawer, setActiveAutomationDrawer] = useState('AMAZON');
  const [manualFilters, setManualFilters] = useState(buildManualFilters(undefined, 'AMAZON'));
  const [manualDryRun, setManualDryRun] = useState(null);
  const [manualResponse, setManualResponse] = useState({
    items: [],
    pagination: { page: 1, limit: 24, hasMore: false, rawResultCount: 0 },
    usage: null,
    protection: null
  });
  const [ruleForm, setRuleForm] = useState(buildRuleForm(undefined, 'AMAZON'));
  const [settingsForm, setSettingsForm] = useState(buildSettingsForm());
  const [selectedResultId, setSelectedResultId] = useState(null);
  const [resultDrafts, setResultDrafts] = useState({});

  async function apiFetch(path, options = {}) {
    const response = await fetch(`${API_BASE_URL}${path}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        'X-User-Role': user?.role || '',
        ...(options.headers || {})
      }
    });
    const data = await response.json().catch(() => ({}));

    if (!response.ok) {
      throw new Error(data?.error || `Request fehlgeschlagen (${response.status})`);
    }

    return data;
  }

  async function loadDashboard(nextResultsFilters = resultsFilters, nextUsageFilters = usageFilters, withSpinner = true) {
    if (withSpinner) {
      setBootLoading(true);
    }

    try {
      const moduleQuery = nextUsageFilters.module === 'all' ? '' : nextUsageFilters.module;
      const [
        statusResponse,
        amazonStatusResponse,
        copybotOverviewResponse,
        learningOverviewResponse,
        usageSummaryResponse,
        rulesResponse,
        alertsResponse,
        resultsResponse,
        historyResponse,
        logsResponse,
        fakeDropSummaryResponse,
        fakeDropHistoryResponse,
        fakeDropSettingsResponse
      ] = await Promise.all([
        apiFetch('/api/keepa/status'),
        apiFetch('/api/amazon/status'),
        apiFetch('/api/copybot/overview'),
        apiFetch('/api/learning/overview'),
        apiFetch('/api/keepa/usage/summary'),
        apiFetch('/api/keepa/rules'),
        apiFetch('/api/keepa/alerts?limit=30'),
        apiFetch(
          `/api/keepa/results?workflowStatus=${encodeURIComponent(nextResultsFilters.workflowStatus || '')}&categoryId=${encodeURIComponent(
            nextResultsFilters.categoryId || ''
          )}&minDiscount=${encodeURIComponent(nextResultsFilters.minDiscount || '')}&minDealScore=${encodeURIComponent(
            nextResultsFilters.minDealScore || ''
          )}&page=${encodeURIComponent(nextResultsFilters.page || 1)}&limit=${encodeURIComponent(nextResultsFilters.limit || 20)}`
        ),
        apiFetch(`/api/keepa/usage/history?days=${encodeURIComponent(nextUsageFilters.days || 30)}&module=${encodeURIComponent(moduleQuery)}`),
        apiFetch(
          `/api/keepa/usage/logs?range=${encodeURIComponent(nextUsageFilters.range || 'today')}&module=${encodeURIComponent(
            moduleQuery
          )}&limit=${encodeURIComponent(nextUsageFilters.limit || 40)}`
        ),
        apiFetch('/api/keepa/fake-drop/summary'),
        apiFetch('/api/keepa/fake-drop/history?days=30'),
        apiFetch('/api/keepa/fake-drop/settings')
      ]);

      setStatusData(statusResponse);
      setAmazonStatusData(amazonStatusResponse);
      setCopybotOverview(copybotOverviewResponse);
      setLearningOverview(learningOverviewResponse);
      setUsageSummary(usageSummaryResponse);
      setRules(rulesResponse.items || []);
      setAlerts(alertsResponse.items || []);
      setResults(resultsResponse);
      setUsageHistory(historyResponse);
      setUsageLogs(logsResponse);
      setFakeDropSummary(fakeDropSummaryResponse);
      setFakeDropHistory(fakeDropHistoryResponse);

      if (!initializedRef.current && statusResponse?.settings) {
        const nextSettingsForm = buildSettingsForm(statusResponse.settings);
        setSettingsForm(nextSettingsForm);
        setManualFilters(buildManualFilters(nextSettingsForm, activeManualDrawer));
        initializedRef.current = true;
      }

      if (!ruleInitializedRef.current && statusResponse?.settings) {
        const nextSettingsForm = buildSettingsForm(statusResponse.settings);
        setRuleForm(buildRuleForm(nextSettingsForm, activeAutomationDrawer));
        ruleInitializedRef.current = true;
      }

      if (!fakeDropSettingsInitializedRef.current && fakeDropSettingsResponse) {
        setFakeDropSettingsForm(buildFakeDropSettingsForm(fakeDropSettingsResponse));
        fakeDropSettingsInitializedRef.current = true;
      }

      if (resultsResponse?.items?.length) {
        const selectedExists = resultsResponse.items.some((item) => item.id === selectedResultId);
        setSelectedResultId(selectedExists ? selectedResultId : resultsResponse.items[0].id);
      } else {
        setSelectedResultId(null);
      }
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : 'Lern-Logik- und Keepa-Daten konnten nicht geladen werden.');
    } finally {
      if (withSpinner) {
        setBootLoading(false);
      }
    }
  }

  useEffect(() => {
    void loadDashboard(resultsFilters, usageFilters, true);
  }, [user?.role]);

  async function loadReviewQueue(nextFilters = reviewFilters) {
    const params = new URLSearchParams({
      page: String(nextFilters.page || 1),
      limit: String(nextFilters.limit || 8),
      sellerType: nextFilters.sellerType || 'ALL',
      classification: nextFilters.classification || '',
      onlyUnlabeled: nextFilters.onlyUnlabeled ? 'true' : 'false'
    });
    const response = await apiFetch(`/api/keepa/fake-drop/review-queue?${params.toString()}`);
    setReviewQueue(response);
  }

  async function loadExampleLibrary(nextFilters = exampleFilters) {
    const params = new URLSearchParams({
      page: String(nextFilters.page || 1),
      limit: String(nextFilters.limit || 8),
      bucket: nextFilters.bucket || '',
      label: nextFilters.label || '',
      sellerType: nextFilters.sellerType || 'ALL',
      search: nextFilters.search || ''
    });
    const response = await apiFetch(`/api/keepa/fake-drop/examples?${params.toString()}`);
    setExampleLibrary(response);
  }

  useEffect(() => {
    if (currentTab === '/keepa/review-queue') {
      void loadReviewQueue(reviewFilters).catch((error) => {
        setStatusMessage(error instanceof Error ? error.message : 'Review Queue konnte nicht geladen werden.');
      });
    }

    if (currentTab === '/keepa/lern-datenbank') {
      void loadExampleLibrary(exampleFilters).catch((error) => {
        setStatusMessage(error instanceof Error ? error.message : 'Lern-Datenbank konnte nicht geladen werden.');
      });
    }
  }, [currentTab, user?.role]);

  const categoryCatalog = statusData?.settings?.categoryCatalog || [];
  const comparisonAdapters = statusData?.settings?.comparisonAdapters || [];
  const selectedResult = useMemo(
    () => results.items.find((item) => item.id === selectedResultId) || null,
    [results.items, selectedResultId]
  );
  const manualUsagePreview = useMemo(
    () => estimateUsagePreview(manualFilters, statusData?.settings),
    [manualFilters, statusData?.settings]
  );
  const manualProtection = manualDryRun?.protection || manualResponse?.protection || statusData?.protection || null;
  const manualConfirmationReady = Boolean(manualDryRun?.confirmationRequired && manualDryRun?.confirmationToken);
  const usageBreakdown = usageHistory?.sourceBreakdown?.length ? usageHistory.sourceBreakdown : usageSummary?.sourceBreakdown || [];
  const recentIssues = usageSummary?.recentIssues || [];
  const fakeDropDistribution = fakeDropSummary?.distribution || [];

  useEffect(() => {
    if (!selectedResult) {
      return;
    }

    setResultDrafts((prev) => {
      if (prev[selectedResult.id]) {
        return prev;
      }

      return {
        ...prev,
        [selectedResult.id]: {
          workflowStatus: selectedResult.workflowStatus,
          note: selectedResult.note || '',
          comparisonPrice: selectedResult.comparisonPrice ?? '',
          comparisonSource: selectedResult.comparisonSource || ''
        }
      };
    });
  }, [selectedResult]);

  useEffect(() => {
    if (!reviewQueue.items?.length) {
      return;
    }

    setReviewDrafts((prev) => {
      const nextState = { ...prev };

      reviewQueue.items.forEach((item) => {
        if (!nextState[item.id]) {
          nextState[item.id] = {
            note: item.fakeDrop?.note || '',
            tags: item.fakeDrop?.tags || [],
            exampleBucket: item.fakeDrop?.currentLabel === 'ja' ? 'positive' : item.fakeDrop?.currentLabel === 'nein' ? 'negative' : 'unsicher'
          };
        }
      });

      return nextState;
    });
  }, [reviewQueue.items]);

  function toggleCategory(target, categoryId) {
    target((prev) => {
      const categories = prev.categories || prev.defaultCategories || [];
      const nextCategories = categories.includes(categoryId)
        ? categories.filter((item) => item !== categoryId)
        : [...categories, categoryId];

      if ('categories' in prev) {
        return { ...prev, categories: nextCategories };
      }

      return { ...prev, defaultCategories: nextCategories };
    });
  }

  function toggleComparisonSource(sourceId) {
    setRuleForm((prev) => ({
      ...prev,
      comparisonSources: prev.comparisonSources.includes(sourceId)
        ? prev.comparisonSources.filter((item) => item !== sourceId)
        : [...prev.comparisonSources, sourceId]
    }));
  }

  function updateSettingsAdapter(sourceId, enabled) {
    setSettingsForm((prev) => ({
      ...prev,
      comparisonSourceConfig: {
        ...prev.comparisonSourceConfig,
        [sourceId]: {
          ...(prev.comparisonSourceConfig?.[sourceId] || {}),
          enabled
        }
      }
    }));
  }

  function getDrawerConfig(drawerKey, source = settingsForm.drawerConfigs) {
    const resolvedDrawerKey = normalizeDrawerKey(drawerKey);
    return source?.[resolvedDrawerKey] || defaultDrawerConfigs[resolvedDrawerKey];
  }

  function updateDrawerConfig(drawerKey, patch) {
    const resolvedDrawerKey = normalizeDrawerKey(drawerKey);
    setSettingsForm((prev) => ({
      ...prev,
      drawerConfigs: {
        ...(prev.drawerConfigs || buildDrawerConfigs(prev)),
        [resolvedDrawerKey]: {
          ...getDrawerConfig(resolvedDrawerKey, prev.drawerConfigs),
          ...patch,
          sellerType: resolvedDrawerKey
        }
      }
    }));
  }

  function handleSelectManualDrawer(drawerKey) {
    const resolvedDrawerKey = normalizeDrawerKey(drawerKey);
    setActiveManualDrawer(resolvedDrawerKey);
    setManualFilters(buildManualFilters(settingsForm, resolvedDrawerKey));
    setManualDryRun(null);
  }

  function updateManualFilters(patch) {
    setManualFilters((prev) => ({
      ...prev,
      ...patch
    }));
    updateDrawerConfig(activeManualDrawer, patch);
    setManualDryRun(null);
  }

  function handleSelectAutomationDrawer(drawerKey) {
    setActiveAutomationDrawer(normalizeDrawerKey(drawerKey));
  }

  function loadDrawerIntoRuleForm(drawerKey = activeAutomationDrawer) {
    const resolvedDrawerKey = normalizeDrawerKey(drawerKey);
    const drawerConfig = getDrawerConfig(resolvedDrawerKey);
    setRuleForm((prev) => ({
      ...prev,
      drawerKey: resolvedDrawerKey,
      sellerType: resolvedDrawerKey,
      minDiscount: drawerConfig.minDiscount,
      minPrice: drawerConfig.minPrice,
      maxPrice: drawerConfig.maxPrice,
      categories: [...drawerConfig.categories],
      trendInterval: drawerConfig.trendInterval,
      sortBy: drawerConfig.sortBy,
      onlyPrime: drawerConfig.onlyPrime,
      onlyInStock: drawerConfig.onlyInStock,
      onlyGoodRating: drawerConfig.onlyGoodRating,
      onlyWithReviews: drawerConfig.onlyWithReviews,
      amazonOfferMode: drawerConfig.amazonOfferMode,
      singleVariantOnly: drawerConfig.singleVariantOnly,
      recentPriceChangeOnly: drawerConfig.recentPriceChangeOnly
    }));
  }

  function toggleReviewTag(reviewItemId, tagId) {
    setReviewDrafts((prev) => {
      const currentTags = prev[reviewItemId]?.tags || [];
      const nextTags = currentTags.includes(tagId)
        ? currentTags.filter((item) => item !== tagId)
        : [...currentTags, tagId];

      return {
        ...prev,
        [reviewItemId]: {
          ...prev[reviewItemId],
          tags: nextTags
        }
      };
    });
  }

  async function handleManualSearch(page = 1, confirmed = false) {
    setManualLoading(true);
    setStatusMessage('');

    try {
      const data = await apiFetch('/api/keepa/manual-search', {
        method: 'POST',
        body: JSON.stringify({
          ...manualFilters,
          page,
          confirmed,
          confirmationToken: confirmed ? manualDryRun?.confirmationToken || '' : ''
        })
      });

      if (data?.executed === false) {
        setManualDryRun(data.dryRun || null);
        setManualResponse((prev) => ({
          ...prev,
          protection: data.protection || null
        }));

        if (data?.dryRun?.blocked) {
          setStatusMessage(data.dryRun?.protection?.blockReason || 'Keepa-Schutz aktiv – Abfrage wurde blockiert.');
        } else if (data?.dryRun?.confirmationRequired) {
          setStatusMessage('Keepa-Dry-Run erstellt. Bitte die Abfrage jetzt bewusst bestaetigen.');
        }

        return;
      }

      setManualDryRun(null);
      setManualFilters((prev) => ({ ...prev, page, limit: data?.filters?.limit ?? prev.limit }));
      setManualResponse(data);
      setStatusMessage('Keepa-Abfrage abgeschlossen.');
      await loadDashboard(resultsFilters, usageFilters, false);
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : 'Manuelle Suche fehlgeschlagen.');
    } finally {
      setManualLoading(false);
    }
  }

  async function handleConfirmManualSearch(page = 1) {
    if (!manualDryRun?.confirmationToken) {
      setStatusMessage('Bitte zuerst einen Dry-Run erzeugen.');
      return;
    }

    await handleManualSearch(page, true);
  }

  async function handleSaveRule() {
    if (!isAdmin) {
      return;
    }

    setSavingRule(true);
    setStatusMessage('');

    try {
      const method = ruleForm.id ? 'PATCH' : 'POST';
      const path = ruleForm.id ? `/api/keepa/rules/${ruleForm.id}` : '/api/keepa/rules';
      await apiFetch(path, {
        method,
        body: JSON.stringify(ruleForm)
      });

      setRuleForm(buildRuleForm(statusData?.settings));
      setStatusMessage(ruleForm.id ? 'Keepa-Regel aktualisiert.' : 'Keepa-Regel angelegt.');
      await loadDashboard(resultsFilters, usageFilters, false);
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : 'Keepa-Regel konnte nicht gespeichert werden.');
    } finally {
      setSavingRule(false);
    }
  }

  async function handleToggleRule(rule, isActive) {
    if (!isAdmin) {
      return;
    }

    try {
      await apiFetch(`/api/keepa/rules/${rule.id}`, {
        method: 'PATCH',
        body: JSON.stringify({
          ...rule,
          isActive
        })
      });
      await loadDashboard(resultsFilters, usageFilters, false);
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : 'Regelstatus konnte nicht aktualisiert werden.');
    }
  }

  async function handleSaveSettings() {
    if (!isAdmin) {
      return;
    }

    setSavingSettings(true);
    setStatusMessage('');

    try {
      const data = await apiFetch('/api/keepa/settings', {
        method: 'PUT',
        body: JSON.stringify(settingsForm)
      });

      const nextSettingsForm = buildSettingsForm(data);
      setSettingsForm(nextSettingsForm);
      setManualFilters(buildManualFilters(nextSettingsForm, activeManualDrawer));
      setRuleForm((prev) => ({
        ...buildRuleForm(nextSettingsForm, activeAutomationDrawer),
        id: prev.id,
        name: prev.name,
        minDealScore: prev.minDealScore,
        intervalMinutes: prev.intervalMinutes,
      comparisonSources: [...prev.comparisonSources],
        isActive: prev.isActive
      }));
      setStatusMessage('Keepa-Einstellungen gespeichert.');
      await loadDashboard(resultsFilters, usageFilters, false);
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : 'Keepa-Einstellungen konnten nicht gespeichert werden.');
    } finally {
      setSavingSettings(false);
    }
  }

  async function handleTestConnection() {
    if (!isAdmin) {
      return;
    }

    setTestingConnection(true);
    setStatusMessage('');

    try {
      const data = await apiFetch('/api/keepa/test-connection', {
        method: 'POST',
        body: JSON.stringify({})
      });
      setStatusMessage(`Keepa verbunden, Tokens verfuegbar: ${data.tokensLeft}`);
      await loadDashboard(resultsFilters, usageFilters, false);
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : 'Keepa-Verbindungstest fehlgeschlagen.');
    } finally {
      setTestingConnection(false);
    }
  }

  async function handleTestAlert() {
    if (!isAdmin) {
      return;
    }

    setTestingAlert(true);
    setStatusMessage('');

    try {
      const data = await apiFetch('/api/keepa/alerts/test', {
        method: 'POST',
        body: JSON.stringify({})
      });
      setStatusMessage(`Test-Alert abgeschlossen: ${data.outputs?.length || 0} Kanaele verarbeitet.`);
      await loadDashboard(resultsFilters, usageFilters, false);
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : 'Test-Alert fehlgeschlagen.');
    } finally {
      setTestingAlert(false);
    }
  }

  async function handleApplyReviewFilters(page = 1) {
    const nextFilters = {
      ...reviewFilters,
      page
    };
    setReviewFilters(nextFilters);

    try {
      await loadReviewQueue(nextFilters);
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : 'Review Queue konnte nicht geladen werden.');
    }
  }

  async function handleApplyExampleFilters(page = 1) {
    const nextFilters = {
      ...exampleFilters,
      page
    };
    setExampleFilters(nextFilters);

    try {
      await loadExampleLibrary(nextFilters);
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : 'Lern-Datenbank konnte nicht geladen werden.');
    }
  }

  async function handleSubmitReview(reviewItemId, label, saveAsExample = false) {
    setReviewBusyId(reviewItemId);
    setStatusMessage('');

    try {
      const draft = reviewDrafts[reviewItemId] || {};
      await apiFetch(`/api/keepa/fake-drop/review/${reviewItemId}`, {
        method: 'POST',
        body: JSON.stringify({
          label,
          note: draft.note || '',
          tags: draft.tags || [],
          saveAsExample,
          exampleBucket: draft.exampleBucket || ''
        })
      });

      setStatusMessage(`Review gespeichert: ${getReviewOptionMeta(label).label}.`);
      await loadDashboard(resultsFilters, usageFilters, false);
      await loadReviewQueue(reviewFilters);
      if (saveAsExample || currentTab === '/keepa/lern-datenbank') {
        await loadExampleLibrary(exampleFilters);
      }
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : 'Review konnte nicht gespeichert werden.');
    } finally {
      setReviewBusyId(0);
    }
  }

  async function handleSaveFakeDropSettings() {
    if (!isAdmin) {
      return;
    }

    setSavingFakeDropSettings(true);
    setStatusMessage('');

    try {
      const data = await apiFetch('/api/keepa/fake-drop/settings', {
        method: 'PATCH',
        body: JSON.stringify(fakeDropSettingsForm)
      });
      setFakeDropSettingsForm(buildFakeDropSettingsForm(data));
      setStatusMessage('Fake-Drop-Heuristiken gespeichert.');
      await loadDashboard(resultsFilters, usageFilters, false);
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : 'Fake-Drop-Heuristiken konnten nicht gespeichert werden.');
    } finally {
      setSavingFakeDropSettings(false);
    }
  }

  async function handleRecalculateFakeDrop() {
    if (!isAdmin) {
      return;
    }

    setRecalculatingFakeDrop(true);
    setStatusMessage('');

    try {
      const data = await apiFetch('/api/keepa/fake-drop/recalculate', {
        method: 'POST',
        body: JSON.stringify({})
      });
      setStatusMessage(`Fake-Drop-Analyse neu berechnet: ${data.processedCount || 0} Treffer.`);
      await loadDashboard(resultsFilters, usageFilters, false);
      await loadReviewQueue(reviewFilters);
      await loadExampleLibrary(exampleFilters);
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : 'Fake-Drop-Neuberechnung fehlgeschlagen.');
    } finally {
      setRecalculatingFakeDrop(false);
    }
  }

  async function handleApplyResultsFilters(page = 1) {
    const nextFilters = {
      ...resultsFilters,
      page
    };
    setResultsFilters(nextFilters);
    await loadDashboard(nextFilters, usageFilters, false);
  }

  async function handleApplyUsageFilters(nextPatch = {}) {
    const nextFilters = {
      ...usageFilters,
      ...nextPatch
    };
    setUsageFilters(nextFilters);
    await loadDashboard(resultsFilters, nextFilters, false);
  }

  async function handleSaveResult(resultId, workflowStatusOverride) {
    const draft = resultDrafts[resultId];
    if (!draft) {
      return;
    }

    try {
      const data = await apiFetch(`/api/keepa/results/${resultId}`, {
        method: 'PATCH',
        body: JSON.stringify({
          workflowStatus: workflowStatusOverride || draft.workflowStatus,
          note: draft.note,
          comparisonPrice: draft.comparisonPrice,
          comparisonSource: draft.comparisonSource
        })
      });

      setResultDrafts((prev) => ({
        ...prev,
        [resultId]: {
          workflowStatus: data.workflowStatus,
          note: data.note || '',
          comparisonPrice: data.comparisonPrice ?? '',
          comparisonSource: data.comparisonSource || ''
        }
      }));
      setStatusMessage('Treffer aktualisiert.');
      await loadDashboard(resultsFilters, usageFilters, false);
      setSelectedResultId(resultId);
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : 'Treffer konnte nicht aktualisiert werden.');
    }
  }

  function renderCategoryPicker(selectedIds, onToggle) {
    return (
      <div className="keepa-category-grid">
        {categoryCatalog.map((category) => (
          <label key={category.id} className={`keepa-category-card ${selectedIds.includes(category.id) ? 'active' : ''}`}>
            <input
              type="checkbox"
              checked={selectedIds.includes(category.id)}
              onChange={() => onToggle(category.id)}
            />
            <span>
              <strong>{category.name}</strong>
              <small>{category.description}</small>
            </span>
          </label>
        ))}
      </div>
    );
  }

  function renderDrawerCards(mode = 'manual') {
    const activeDrawer = mode === 'manual' ? activeManualDrawer : activeAutomationDrawer;
    const onSelect = mode === 'manual' ? handleSelectManualDrawer : handleSelectAutomationDrawer;

    return (
      <div className="keepa-drawer-grid">
        {keepaDrawerCatalog.map((drawer) => {
          const drawerConfig = getDrawerConfig(drawer.key);

          return (
            <button
              key={drawer.key}
              type="button"
              className={`keepa-drawer-card ${activeDrawer === drawer.key ? 'active' : ''}`}
              onClick={() => onSelect(drawer.key)}
            >
              <div className="keepa-drawer-card-top">
                <div>
                  <strong>{drawer.label}</strong>
                  <p className="text-muted">{drawer.description}</p>
                </div>
                <div className="keepa-card-tags">
                  <span className={`status-chip ${drawerConfig.active ? 'success' : 'warning'}`}>
                    {drawerConfig.active ? 'aktiv' : 'pausiert'}
                  </span>
                  <span className={`status-chip ${drawerConfig.patternSupportEnabled ? 'info' : 'warning'}`}>
                    Muster {drawerConfig.patternSupportEnabled ? 'an' : 'aus'}
                  </span>
                  {mode === 'automation' && (
                    <span className={`status-chip ${drawerConfig.autoModeAllowed ? 'info' : 'warning'}`}>
                      Auto {drawerConfig.autoModeAllowed ? 'an' : 'aus'}
                    </span>
                  )}
                </div>
              </div>
              <div className="keepa-drawer-card-metrics">
                <span>Rabatt ab {drawerConfig.minDiscount}%</span>
                <span>Intervall {keepaTrendIntervalOptions.find((item) => item.value === drawerConfig.trendInterval)?.label || drawerConfig.trendInterval}</span>
                <span>Sortierung {keepaSortOptions.find((item) => item.value === drawerConfig.sortBy)?.label || drawerConfig.sortBy}</span>
                <span>Auto-Posting {drawerConfig.testGroupPostingAllowed ? 'erlaubt' : 'gesperrt'}</span>
              </div>
            </button>
          );
        })}
      </div>
    );
  }

  function renderTopKpiBar() {
    const overview = statusData?.overview;
    const connection = statusData?.connection;
    const kpis = usageSummary?.kpis || {};
    const quickStats = [
      {
        label: 'Keepa Status',
        value: overview?.apiStatus || '-',
        detail: connection?.connected ? 'verbunden' : 'vorbereitet'
      },
      {
        label: 'Letzte Abfrage',
        value: formatDateTime(kpis.lastRequestAt),
        detail: 'zuletzt protokolliert'
      },
      {
        label: 'Requests heute',
        value: kpis.requestsToday || 0,
        detail: 'offizielle Keepa-Requests'
      },
      {
        label: 'Treffer heute',
        value: kpis.hitsToday || 0,
        detail: 'manuell + Automatik'
      },
      {
        label: 'Offene Reviews',
        value: fakeDropSummary?.kpis?.openReviewCount || 0,
        detail: 'warten auf Freigabe'
      },
      {
        label: 'Aktive Regeln',
        value: kpis.activeRulesCount || 0,
        detail: 'im Backend aktiv'
      }
    ];

    return (
      <section className="card keepa-kpi-strip">
        {quickStats.map((item) => (
          <article key={item.label} className="keepa-kpi-pill">
            <span>{item.label}</span>
            <strong>{item.value}</strong>
            <small>{item.detail}</small>
          </article>
        ))}
      </section>
    );
  }

  function renderOverviewTab() {
    const overview = statusData?.overview;
    const connection = statusData?.connection;
    const amazonOverview = amazonStatusData?.overview;
    const amazonConnection = amazonStatusData?.connection;
    const learningPipeline = Array.isArray(learningOverview?.pipeline) ? learningOverview.pipeline : [];
    const learningSellerTypes = Array.isArray(learningOverview?.sellerTypes) ? learningOverview.sellerTypes : [];
    const learningSellerControls = Array.isArray(learningOverview?.sellerControls) ? learningOverview.sellerControls : [];
    const sentAlerts = alerts.filter((item) => item.status === 'sent').length;
    const blockedAlerts = alerts.filter((item) => item.status === 'blocked' || item.status === 'failed').length;
    const reviewAlerts = alerts.filter((item) => item.status === 'review' || item.status === 'stored').length;
    const latestTelegramFailure = alerts.find((item) => item.status === 'failed') || null;
    const suspiciousCases = fakeDropDistribution
      .filter((item) => ['wahrscheinlicher_fake_drop', 'verdaechtig', 'manuelle_pruefung'].includes(item.classification))
      .reduce((sum, item) => sum + Number(item.count || 0), 0);
    const keepaPipeline = learningPipeline.find((item) => item.id === 'auto_deals');
    const scrapperPipeline = learningPipeline.find((item) => item.id === 'scrapper');
    const generatorPipeline = learningPipeline.find((item) => item.id === 'generator');
    const flowLanes = [
      {
        id: 'keepa',
        title: 'Keepa + Amazon API / Auto-Deals',
        badge: connection?.connected && amazonConnection?.configured ? 'verbunden' : 'vorbereitet',
        badgeTone: connection?.connected && amazonConnection?.configured ? 'connected' : 'review',
        summary:
          keepaPipeline?.detail || 'Keepa liefert Preisverlauf, Amazon API liefert Produkt- und Affiliate-Daten fuer Auto-Deals.',
        steps: [
          {
            eyebrow: 'Quelle',
            title: 'Keepa + Amazon API',
            detail:
              connection?.connected && amazonConnection?.configured
                ? 'Keepa prueft Preisverlauf, Amazon API liefert Produkt-/Affiliate-Daten als zweite Quelle.'
                : 'Keepa und Amazon API sind getrennt vorbereitet und werden je nach Verfuegbarkeit zusammengefuehrt.',
            tone: connection?.connected || amazonConnection?.configured ? 'connected' : 'review'
          },
          {
            eyebrow: 'Pflichtschicht',
            title: 'Lern-Logik',
            detail: 'Preisverlauf, Fake-Drop und Freigabe-Regeln muessen zuerst durchlaufen.',
            tone: 'active'
          },
          {
            eyebrow: 'Regeln',
            title: 'AMAZON / FBA / FBM',
            detail: 'Getrennte Seller-Type-Schwellen bleiben als eigene Logik aktiv.',
            tone: 'active'
          },
          {
            eyebrow: 'Entscheidung',
            title: sentAlerts ? 'approved_for_test_group' : reviewAlerts ? 'review' : 'blocked / review',
            detail: `${sentAlerts} gesendet, ${reviewAlerts} in Review, ${blockedAlerts} blockiert.`,
            tone: sentAlerts ? 'approved' : reviewAlerts ? 'review' : 'blocked'
          },
          {
            eyebrow: 'Output',
            title: 'Telegram Testgruppe',
            detail: settingsForm.alertTelegramEnabled ? 'Nur freigegebene Auto-Deals werden ausgegeben.' : 'Telegram-Auto-Output ist derzeit deaktiviert.',
            tone: settingsForm.alertTelegramEnabled ? 'sent' : 'inactive'
          }
        ]
      },
      {
        id: 'scrapper',
        title: 'Scrapper / Rohdeals',
        badge: 'pflichtgekoppelt',
        badgeTone: 'connected',
        summary: scrapperPipeline?.detail || 'Rohdeals werden zuerst bewertet und dann weitergeleitet.',
        steps: [
          {
            eyebrow: 'Quelle',
            title: 'Scrapper',
            detail: 'Quellen und Rohdeals bleiben vom Bewertungsbereich getrennt.',
            tone: 'connected'
          },
          {
            eyebrow: 'Pflichtschicht',
            title: 'Lern-Logik',
            detail: 'Scrapper-Deals duerfen die Bewertung nicht umgehen.',
            tone: 'active'
          },
          {
            eyebrow: 'Regeln',
            title: 'AMAZON / FBA / FBM',
            detail: `${fakeDropSummary?.kpis?.openReviewCount || 0} Faelle warten aktuell auf Review oder Freigabe.`,
            tone: 'active'
          },
          {
            eyebrow: 'Entscheidung',
            title: (fakeDropSummary?.kpis?.openReviewCount || 0) > 0 ? 'review' : 'approved / block',
            detail: 'Rohdeals landen je nach Verlauf in Review, Testgruppe oder Block.',
            tone: (fakeDropSummary?.kpis?.openReviewCount || 0) > 0 ? 'review' : 'info'
          },
          {
            eyebrow: 'Output',
            title: 'Review / Testgruppe',
            detail: 'Nur freigegebene Deals koennen spaeter in Outputs weiterlaufen.',
            tone: 'info'
          }
        ]
      },
      {
        id: 'generator',
        title: 'Generator / Manuell',
        badge: 'sauber getrennt',
        badgeTone: 'info',
        summary: generatorPipeline?.detail || 'Manuelle Deals bleiben schnell und direkt veroeffentlichbar.',
        steps: [
          {
            eyebrow: 'Quelle',
            title: 'Generator',
            detail: 'Manueller Arbeitsbereich fuer Erstellen, Bearbeiten und Posten.',
            tone: 'connected'
          },
          {
            eyebrow: 'Unterstuetzung',
            title: 'Optionale Lern-Logik',
            detail: 'Die Anbindung bleibt intern aktiv, aber nicht mehr als Standard-UI im Generator sichtbar.',
            tone: 'optional'
          },
          {
            eyebrow: 'Regeln',
            title: 'Seller Type Kontext',
            detail: 'Bewertung und Keepa-Kontext koennen intern mitlaufen, ohne den Workflow zu ueberlagern.',
            tone: 'info'
          },
          {
            eyebrow: 'Entscheidung',
            title: 'manuell gesteuert',
            detail: 'Der Admin bewertet im separaten Bereich, der Mitarbeiter arbeitet im Generator sauber weiter.',
            tone: 'info'
          },
          {
            eyebrow: 'Output',
            title: 'Direct Publish',
            detail: 'Direktes Telegram-Posting bleibt als separater manueller Flow bestehen.',
            tone: 'sent'
          }
        ]
      }
    ];
    const dashboardMetrics = [
      {
        label: 'Offene Reviews',
        value: fakeDropSummary?.kpis?.openReviewCount || overview?.fakeDropSummary?.kpis?.openReviewCount || 0,
        detail: 'Faelle aus Quelle, Review oder Auto-Flow'
      },
      {
        label: 'Verdaechtige Verlaeufe',
        value: suspiciousCases,
        detail: 'mit Fake-Drop- oder Pruef-Hinweis'
      },
      {
        label: 'Telegram Outputs',
        value: sentAlerts,
        detail: 'zuletzt erfolgreich gesendet'
      },
      {
        label: 'Letzte manuelle Suche',
        value: formatDateTime(usageSummary?.lastManualSearch?.createdAt),
        detail: `${usageSummary?.lastManualSearch?.resultCount || 0} Treffer aus kontrolliertem Abruf`
      }
    ];
    const sourceStatusCards = [
      {
        label: 'Keepa Status',
        tone: connection?.connected ? 'success' : statusData?.settings?.keepaKeyStatus?.connected ? 'warning' : 'danger',
        status: connection?.connected ? 'verbunden' : statusData?.settings?.keepaKeyStatus?.connected ? 'fehler' : 'nicht konfiguriert',
        value: formatDateTime(connection?.checkedAt || overview?.lastSync),
        detail: connection?.connected ? 'Preisverlauf aktiv.' : 'Preisquelle noch nicht sauber verbunden.'
      },
      {
        label: 'Amazon API Status',
        tone:
          amazonOverview?.apiStatus === 'verbunden'
            ? 'success'
            : amazonOverview?.apiStatus === 'auth_fehler' || amazonOverview?.apiStatus === 'fehler'
              ? 'danger'
              : 'warning',
        status: amazonOverview?.apiStatus || 'vorbereitet',
        value: formatDateTime(amazonOverview?.lastSuccessfulFetch),
        detail:
          amazonOverview?.apiStatus === 'verbunden'
            ? `Produkt- und Affiliate-Daten aktiv. Deprecated ab ${learningOverview?.amazon?.deprecation?.date || '2026-04-30'}.`
            : amazonOverview?.lastErrorMessage ||
              `Amazon API vorbereitet. Deprecated ab ${learningOverview?.amazon?.deprecation?.date || '2026-04-30'}.`
      },
      {
        label: 'Scrapper Status',
        tone: copybotOverview?.copybotEnabled ? 'success' : 'warning',
        status: copybotOverview?.copybotEnabled ? 'aktiv' : 'deaktiviert',
        value: `${copybotOverview?.reviewCount || 0} Review-Faelle`,
        detail: copybotOverview?.lastProcessedSource?.name || 'Rohdeals bleiben getrennt.'
      }
    ];
    const processingStatusCards = [
      {
        label: 'Link Builder',
        status: 'aktiv',
        tone: 'success',
        detail: 'Externe Amazon-Links werden standardisiert.'
      },
      {
        label: 'Lern-Logik',
        status: 'pflicht',
        tone: 'success',
        detail: 'Quellen laufen vor jedem Output ueber die zentrale Entscheidung.'
      },
      {
        label: 'AMAZON / FBA / FBM',
        status: 'getrennt',
        tone: 'info',
        detail: `${learningSellerTypes.length} Seller-Type-Profile mit eigenen Regeln und Feedback-Schwellen.`
      }
    ];
    const outputStatusCards = [
      {
        label: 'Review',
        status: reviewAlerts > 0 ? 'offen' : 'leer',
        tone: reviewAlerts > 0 ? 'warning' : 'info',
        detail: `${reviewAlerts} Deals warten auf Freigabe.`
      },
      {
        label: 'Telegram Testgruppe',
        status: sentAlerts > 0 ? 'aktiv' : settingsForm.alertTelegramEnabled ? 'bereit' : 'deaktiviert',
        tone: sentAlerts > 0 ? 'success' : settingsForm.alertTelegramEnabled ? 'info' : 'warning',
        detail: sentAlerts > 0 ? `${sentAlerts} Alerts erfolgreich gesendet.` : 'Output nur fuer freigegebene Deals.'
      },
      {
        label: 'Live-Gruppen',
        status: 'vorbereitet',
        tone: 'info',
        detail: 'Weiterer Output bleibt separat vorbereitet und laeuft aktuell noch nicht automatisch.'
      }
    ];
    const sellerControlCards = keepaDrawerCatalog.map((drawer) => {
      const drawerConfig = getDrawerConfig(drawer.key);
      const control = learningSellerControls.find((item) => item.id === drawer.key) || {};

      return {
        id: drawer.key,
        label: drawer.label,
        active: drawerConfig.active,
        patternSupportEnabled: drawerConfig.patternSupportEnabled,
        autoModeAllowed: drawerConfig.autoModeAllowed,
        autoPostingEnabled: drawerConfig.testGroupPostingAllowed && settingsForm.alertTelegramEnabled,
        rulesActive: control.rulesActive !== false,
        lastDecision: control.lastDecision || 'noch_keine',
        lastDecisionDetail: shortenText(control.lastDecisionDetail || 'Noch keine letzte Entscheidung gespeichert.', 90),
        lastRunAt: control.lastRunAt || null,
        lastAsin: control.lastAsin || '',
        lastStrength: control.lastStrength || ''
      };
    });
    const faultStatusCards = [
      {
        label: 'Amazon letzter Fehler',
        value: formatDateTime(amazonOverview?.lastErrorAt),
        detail: amazonOverview?.lastErrorMessage || 'Noch kein Amazon-Fehler gespeichert.'
      },
      {
        label: 'Amazon Auth Fehler',
        value: formatDateTime(amazonOverview?.lastAuthErrorAt),
        detail: amazonOverview?.authErrorCount24h ? `${amazonOverview.authErrorCount24h} Auth-Fehler in 24h.` : 'Kein letzter Auth-Fehler gespeichert.'
      },
      {
        label: 'Keine Treffer',
        value: `${amazonOverview?.noHitsCount24h || 0}`,
        detail: 'Amazon-Requests ohne Treffer in den letzten 24 Stunden.'
      },
      {
        label: 'Telegram Fehler',
        value: latestTelegramFailure ? formatDateTime(latestTelegramFailure.createdAt) : '-',
        detail: latestTelegramFailure?.errorMessage || 'Noch kein letzter Telegram-Fehler gespeichert.'
      }
    ];
    const statusAndOutputCards = [...sourceStatusCards, ...processingStatusCards, ...outputStatusCards, ...faultStatusCards].map((item) => ({
      ...item,
      detail: shortenText(item.detail, 96)
    }));

    return (
      <div className="keepa-section-stack">
        <section className="card keepa-banner keepa-admin-banner keepa-admin-summary">
          <div className="dashboard-hero-copy">
            <p className="section-title">Admin-Steuerbereich</p>
            <h2 className="page-title">Logik-Zentrale fuer Quellen, Regeln und Output</h2>
            <p className="page-subtitle">Generator und Scrapper bleiben clean, die Steuerung sitzt kompakt hier.</p>
          </div>
          <div className="dashboard-chip-row">
            <span className="status-chip info">Nur fuer Admin sichtbar</span>
            <span className={`status-chip ${connection?.connected ? 'success' : 'warning'}`}>
              {connection?.connected ? 'Keepa verbunden' : 'Keepa vorbereitet'}
            </span>
            <span
              className={`status-chip ${
                amazonOverview?.apiStatus === 'verbunden'
                  ? 'success'
                  : amazonOverview?.apiStatus === 'auth_fehler' || amazonOverview?.apiStatus === 'fehler'
                    ? 'danger'
                    : 'warning'
              }`}
            >
              {amazonOverview?.apiStatus === 'verbunden' ? 'Amazon API verbunden' : 'Amazon API vorbereitet'}
            </span>
          </div>
        </section>

        <section className="card keepa-panel keepa-overview-panel">
          <div className="dashboard-section-header">
            <div className="dashboard-title-block">
              <p className="section-title">Steuerung</p>
              <h2>Quellen, Kennzahlen und Seller-Typen kompakt</h2>
            </div>
            {isAdmin && (
              <button className="primary" onClick={() => void handleSaveSettings()} disabled={savingSettings}>
                {savingSettings ? 'Speichert...' : 'Steuerung speichern'}
              </button>
            )}
          </div>

          <div className="dashboard-stat-grid">
            {dashboardMetrics.map((item) => (
              <section key={item.label} className="card keepa-metric-card keepa-compact-card">
                <p className="section-title">{item.label}</p>
                <h2>{item.value}</h2>
                <p className="dashboard-meta-line">{item.detail}</p>
              </section>
            ))}
          </div>

          <div className="keepa-control-grid">
            {sellerControlCards.map((item) => (
              <article key={item.id} className="keepa-control-card">
                <div className="keepa-control-header">
                  <div className="dashboard-title-block">
                    <span className="dashboard-link-meta">{item.label}</span>
                    <h3>{item.lastDecision}</h3>
                  </div>
                  <span className={`status-chip ${item.active ? 'success' : 'warning'}`}>
                    {item.active ? 'aktiv' : 'inaktiv'}
                  </span>
                </div>

                <div className="dashboard-chip-row">
                  <span className={`status-chip ${item.patternSupportEnabled ? 'info' : 'warning'}`}>
                    Muster {item.patternSupportEnabled ? 'an' : 'aus'}
                  </span>
                  <span className={`status-chip ${item.autoModeAllowed ? 'info' : 'warning'}`}>
                    Auto-Modus {item.autoModeAllowed ? 'an' : 'aus'}
                  </span>
                  <span className={`status-chip ${item.autoPostingEnabled ? 'success' : 'warning'}`}>
                    Auto-Posting {item.autoPostingEnabled ? 'an' : 'aus'}
                  </span>
                </div>

                <div className="keepa-control-meta">
                  <p className="dashboard-meta-line">Regeln {item.rulesActive ? 'aktiv' : 'vorbereitet'}{item.lastStrength ? ` | Staerke ${item.lastStrength}` : ''}</p>
                  <p className="dashboard-meta-line">Letzter Lauf {formatDateTime(item.lastRunAt)}{item.lastAsin ? ` | ${item.lastAsin}` : ''}</p>
                  <p className="dashboard-meta-line">{item.lastDecisionDetail}</p>
                </div>

                {isAdmin && (
                  <div className="dashboard-toggle-grid">
                    <label className="dashboard-toggle-card">
                      <span>Muster</span>
                      <input
                        type="checkbox"
                        checked={item.patternSupportEnabled}
                        onChange={(event) => updateDrawerConfig(item.id, { patternSupportEnabled: event.target.checked })}
                      />
                    </label>
                    <label className="dashboard-toggle-card">
                      <span>Auto-Posting</span>
                      <input
                        type="checkbox"
                        checked={getDrawerConfig(item.id).testGroupPostingAllowed}
                        onChange={(event) => updateDrawerConfig(item.id, { testGroupPostingAllowed: event.target.checked })}
                      />
                    </label>
                  </div>
                )}
              </article>
            ))}
          </div>
        </section>

        <section className="card keepa-panel">
          <div className="dashboard-title-block">
            <p className="section-title">Deal-Flow</p>
            <h2>Quelle, Pflichtschicht und Output auf einen Blick</h2>
          </div>
          <div className="keepa-flow-board">
            {flowLanes.map((lane) => (
              <article key={lane.id} className="keepa-flow-lane">
                <div className="keepa-flow-lane-header">
                  <div>
                    <p className="section-title">{lane.title}</p>
                    <strong>{shortenText(lane.summary, 110)}</strong>
                  </div>
                  <span className={`status-chip ${getFlowStatusTone(lane.badgeTone)}`}>{lane.badge}</span>
                </div>
                <div className="keepa-flow-steps">
                  {lane.steps.map((step, index) => (
                    <div key={`${lane.id}-${step.eyebrow}`} className="keepa-flow-step-wrap">
                      <section className="keepa-flow-step">
                        <div className="keepa-flow-step-header">
                          <span>{step.eyebrow}</span>
                          <span className={`status-chip ${getFlowStatusTone(step.tone)}`}>{step.title}</span>
                        </div>
                        <p>{shortenText(step.detail, 84)}</p>
                      </section>
                      {index < lane.steps.length - 1 && (
                        <div className="keepa-flow-arrow" aria-hidden="true">
                          -&gt;
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </article>
            ))}
          </div>
        </section>

        <section className="card keepa-panel">
          <div className="dashboard-title-block">
            <p className="section-title">Systemstatus</p>
            <h2>Quellen, Verarbeitung, Output und Fehler kurz erklaert</h2>
          </div>
          <div className="keepa-status-grid">
            {statusAndOutputCards.map((item) => (
              <section key={item.label} className="card keepa-status-card keepa-compact-card">
                <div className="keepa-panel-header">
                  <p className="section-title">{item.label}</p>
                  {'status' in item ? <span className={`status-chip ${item.tone}`}>{item.status}</span> : null}
                </div>
                <h3>{item.value || '-'}</h3>
                <p className="dashboard-meta-line">{item.detail}</p>
              </section>
            ))}
          </div>
        </section>

        <section className="card keepa-panel">
          <div className="dashboard-title-block">
            <p className="section-title">Seller Type Logik</p>
            <h2>AMAZON, FBA und FBM bleiben getrennt</h2>
          </div>
          <div className="dashboard-compact-grid">
            {learningSellerTypes.map((item) => (
              <div key={item.id} className="keepa-metric-card keepa-compact-card">
                <p className="section-title">{item.id}</p>
                <h2>{item.keepaRating}</h2>
                <p className="dashboard-meta-line">Rabatt {item.minDiscount}% | Score {item.minScore} | Fake-Drop {item.maxFakeDropRisk}</p>
                <p className="dashboard-meta-line">
                  Labels: {Array.isArray(item.learningLabels) && item.learningLabels.length ? item.learningLabels.join(', ') : 'noch offen'}
                </p>
              </div>
            ))}
          </div>
        </section>

        <section className="card keepa-banner">
          <div className="dashboard-hero-copy">
            <p className="section-title">Verbindungs-Hinweis</p>
            <h2 className="page-title">{connection?.connected ? 'Keepa ist aktiv verbunden' : 'Keepa ist vorbereitet'}</h2>
            <p className="page-subtitle">API-Keys bleiben im Backend, Verbrauchswerte werden intern protokolliert.</p>
          </div>
          <span className={`status-chip ${connection?.connected ? 'success' : 'warning'}`}>
            {connection?.connected ? 'Verbunden' : 'Nicht verbunden'}
          </span>
        </section>

        <div className="keepa-analytics-grid">
          <section className="card keepa-panel">
            <div className="keepa-panel-header">
              <div>
                <p className="section-title">Verlauf</p>
                <h2>Keepa Nutzung ueber Zeit</h2>
              </div>
              <span className="status-chip info">{usageHistory?.usageModeLabel || 'intern geschaetzt'}</span>
            </div>
            <MiniLineChart data={usageHistory?.series || []} valueKey="estimatedUsage" color="#10b981" />
          </section>

          <section className="card keepa-panel">
            <div className="keepa-panel-header">
              <div>
                <p className="section-title">Verbrauch nach Quelle</p>
                <h2>Aktive Module im Blick</h2>
              </div>
            </div>
            <MiniBarChart data={usageBreakdown} valueKey="estimatedUsage" color="linear-gradient(180deg, #10b981, #0f766e)" valueFormatter={formatUsage} />
          </section>
        </div>

        <div className="keepa-split-panels">
          <section className="card keepa-panel">
            <div className="keepa-panel-header">
              <div>
                <p className="section-title">Entscheidungen</p>
                <h2>Aktuelle Deal-Eingaenge</h2>
              </div>
            </div>
            <div className="keepa-list">
              {(overview?.latestHits || []).map((item) => (
                <button key={item.id} className="keepa-list-item" onClick={() => setSelectedResultId(item.id)}>
                  <div>
                    <strong>{item.title}</strong>
                    <p className="text-muted">
                      {item.asin} - {formatCurrency(item.currentPrice)} - {formatPercent(item.keepaDiscount)}
                    </p>
                  </div>
                  <span className={`status-chip ${getStrengthChip(item.dealStrength)}`}>{strengthLabels[item.dealStrength]}</span>
                </button>
              ))}
              {!overview?.latestHits?.length && <p className="text-muted">Noch keine Keepa-Treffer gespeichert.</p>}
            </div>
          </section>

          <section className="card keepa-panel">
            <div className="keepa-panel-header">
              <div>
                <p className="section-title">Outputs & Logs</p>
                <h2>Telegram und letzte Versandpfade</h2>
              </div>
            </div>
            <div className="keepa-list">
              {(overview?.latestAlerts || []).map((item) => (
                <div key={item.id} className="keepa-list-item static">
                  <div>
                    <strong>{item.channelType}</strong>
                    <p className="text-muted">{item.messagePreview || item.asin}</p>
                  </div>
                  <span className={`status-chip ${item.status === 'failed' ? 'danger' : item.status === 'sent' ? 'success' : 'info'}`}>
                    {item.status}
                  </span>
                </div>
              ))}
              {!overview?.latestAlerts?.length && <p className="text-muted">Noch keine Alerts protokolliert.</p>}
            </div>
          </section>
        </div>
      </div>
    );
  }

  function renderManualSearchTab() {
    const lastUsage = manualResponse?.usage;
    const activeDrawerConfig = getDrawerConfig(activeManualDrawer);
    const protectionTone = manualProtection?.blocked ? 'danger' : manualConfirmationReady ? 'warning' : 'success';
    const protectionLabel = manualProtection?.blocked
      ? 'Geblockt'
      : manualConfirmationReady
        ? 'Bestaetigung noetig'
        : 'Bereit';

    return (
      <div className="keepa-section-stack">
        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Manuelle Suche</p>
              <h2>Deals kontrolliert abrufen</h2>
            </div>
            <div className="keepa-inline-actions">
              {isAdmin && (
                <button className="secondary" onClick={() => void handleSaveSettings()} disabled={savingSettings}>
                  {savingSettings ? 'Speichert...' : 'Profil speichern'}
                </button>
              )}
              {manualDryRun && (
                <button className="secondary" onClick={() => setManualDryRun(null)} disabled={manualLoading}>
                  Vorschau verwerfen
                </button>
              )}
              <button className="secondary" onClick={() => void handleManualSearch(1)} disabled={manualLoading}>
                {manualLoading ? 'Laedt...' : 'Pruefung starten'}
              </button>
              <button className="primary" onClick={() => void handleConfirmManualSearch(1)} disabled={manualLoading || !manualConfirmationReady}>
                {manualLoading && manualConfirmationReady ? 'Bestaetigt...' : 'Abfrage bestaetigen'}
              </button>
            </div>
          </div>

          <p className="text-muted" style={{ margin: 0 }}>
            Manuelle Keepa-Abfragen starten jetzt immer mit einem Dry-Run. Erst nach klarer Bestaetigung wird die
            echte Query fuer AMAZON, FBA oder FBM ausgefuehrt.
          </p>

          {renderDrawerCards('manual')}

          <div className="keepa-card-metrics three">
            <span>
              <strong>Vorab geschaetzt:</strong> {formatUsage(manualUsagePreview)}
            </span>
            <span>
              <strong>Zuletzt erfasst:</strong> {formatUsage(lastUsage?.estimatedUsage)}
            </span>
            <span>
              <strong>Messung:</strong> {lastUsage?.usageModeLabel || 'intern geschaetzt'}
            </span>
            <span>
              <strong>Treffer:</strong> {lastUsage?.resultCount ?? manualResponse?.items?.length ?? 0}
            </span>
            <span>
              <strong>Laufzeit:</strong> {formatDuration(lastUsage?.durationMs)}
            </span>
            <span>
              <strong>Quelle:</strong> {lastUsage?.sourceLabel || 'manuell'}
            </span>
            <span>
              <strong>Schublade:</strong> {activeManualDrawer}
            </span>
            <span>
              <strong>Auto-Output:</strong> {activeDrawerConfig.testGroupPostingAllowed ? 'erlaubt' : 'gesperrt'}
            </span>
            <span>
              <strong>Tokens:</strong> {manualProtection?.tokensLeft ?? statusData?.connection?.tokensLeft ?? '-'}
            </span>
            <span>
              <strong>Schutz:</strong> {protectionLabel}
            </span>
            <span>
              <strong>Cooldown:</strong>{' '}
              {manualProtection?.cooldownActive ? `${Math.ceil((manualProtection?.cooldownRemainingMs || 0) / 1000)}s` : 'frei'}
            </span>
            <span>
              <strong>Limit-Hardcap:</strong> {manualProtection?.cappedLimit ?? 12}
            </span>
          </div>

          <section className="card keepa-metric-card">
            <p className="section-title">Keepa-Schutzschicht</p>
            <h2>{protectionLabel}</h2>
            <div className="keepa-card-tags" style={{ marginBottom: 12 }}>
              <span className={`status-chip ${protectionTone}`}>{protectionLabel}</span>
              {manualProtection?.hardStopActive && <span className="status-chip danger">Hard Stop aktiv</span>}
              {manualProtection?.requestActive && <span className="status-chip warning">Request aktiv</span>}
              {manualConfirmationReady && <span className="status-chip info">Dry-Run bereit</span>}
            </div>
            <p className="text-muted" style={{ marginTop: 0 }}>
              {manualProtection?.blockReason ||
                'Vor jedem echten Keepa-Deal-Request wird jetzt zuerst eine Kosten- und Risiko-Vorschau erzeugt.'}
            </p>
            <div className="keepa-card-metrics three">
              <span>
                <strong>Restguthaben:</strong> {manualProtection?.tokensLeft ?? statusData?.connection?.tokensLeft ?? '-'}
              </span>
              <span>
                <strong>Minimum:</strong> {manualProtection?.minTokensRequired ?? '-'}
              </span>
              <span>
                <strong>Geschaetzter Run:</strong> {manualProtection?.estimatedTokenCost ?? '-'}
              </span>
              <span>
                <strong>Risiko:</strong> {manualProtection?.riskLevel || '-'}
              </span>
              <span>
                <strong>Letzte Sperre:</strong> {formatDateTime(manualProtection?.lastBlockedAt)}
              </span>
              <span>
                <strong>Letzter Lauf:</strong> {formatDateTime(manualProtection?.lastFinishedAt)}
              </span>
            </div>
            {manualProtection?.warnings?.length ? (
              <div className="keepa-card-tags">
                {manualProtection.warnings.map((warning) => (
                  <span key={warning} className="status-chip warning">
                    {warning}
                  </span>
                ))}
              </div>
            ) : null}
          </section>

          <div className="keepa-form-grid">
            <label>
              <span className="section-title">Preisverfall Intervall</span>
              <select value={manualFilters.trendInterval} onChange={(event) => updateManualFilters({ trendInterval: event.target.value })}>
                {keepaTrendIntervalOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>

            <label>
              <span className="section-title">Mindest-Rabatt</span>
              <div className="keepa-range-row">
                <input
                  type="range"
                  min="5"
                  max="90"
                  value={manualFilters.minDiscount}
                  onChange={(event) => updateManualFilters({ minDiscount: Number(event.target.value) })}
                />
                <strong>{manualFilters.minDiscount}%</strong>
              </div>
            </label>

            <label>
              <span className="section-title">Sortierung</span>
              <select value={manualFilters.sortBy} onChange={(event) => updateManualFilters({ sortBy: event.target.value })}>
                {keepaSortOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>

            <label>
              <span className="section-title">Mindestpreis</span>
              <input
                type="number"
                value={manualFilters.minPrice}
                onChange={(event) => updateManualFilters({ minPrice: event.target.value })}
                placeholder="0"
              />
            </label>

            <label>
              <span className="section-title">Hoechstpreis</span>
              <input
                type="number"
                value={manualFilters.maxPrice}
                onChange={(event) => updateManualFilters({ maxPrice: event.target.value })}
                placeholder="500"
              />
            </label>

            <label>
              <span className="section-title">Treffer pro Lauf</span>
              <select
                value={manualFilters.limit}
                onChange={(event) => updateManualFilters({ limit: Number(event.target.value) })}
              >
                <option value="12">12</option>
                <option value="24">24</option>
                <option value="48">48</option>
              </select>
            </label>

            <label>
              <span className="section-title">Amazon-Angebot</span>
              <select value={manualFilters.amazonOfferMode} onChange={(event) => updateManualFilters({ amazonOfferMode: event.target.value })}>
                {keepaAmazonOfferOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
          </div>

          <div className="keepa-flag-grid">
            <label className="checkbox-card">
              <span>Nur Prime</span>
              <input
                type="checkbox"
                checked={manualFilters.onlyPrime}
                onChange={(event) => updateManualFilters({ onlyPrime: event.target.checked })}
              />
            </label>
            <label className="checkbox-card">
              <span>Nur lagernd</span>
              <input
                type="checkbox"
                checked={manualFilters.onlyInStock}
                onChange={(event) => updateManualFilters({ onlyInStock: event.target.checked })}
              />
            </label>
            <label className="checkbox-card">
              <span>Nur gute Bewertung</span>
              <input
                type="checkbox"
                checked={manualFilters.onlyGoodRating}
                onChange={(event) => updateManualFilters({ onlyGoodRating: event.target.checked })}
              />
            </label>
            <label className="checkbox-card">
              <span>Nur mit Bewertungen</span>
              <input
                type="checkbox"
                checked={manualFilters.onlyWithReviews}
                onChange={(event) => updateManualFilters({ onlyWithReviews: event.target.checked })}
              />
            </label>
            <label className="checkbox-card">
              <span>Nur eine Variante</span>
              <input
                type="checkbox"
                checked={manualFilters.singleVariantOnly}
                onChange={(event) => updateManualFilters({ singleVariantOnly: event.target.checked })}
              />
            </label>
            <label className="checkbox-card">
              <span>Nur letzte Preisphase</span>
              <input
                type="checkbox"
                checked={manualFilters.recentPriceChangeOnly}
                onChange={(event) => updateManualFilters({ recentPriceChangeOnly: event.target.checked })}
              />
            </label>
          </div>

          <div>
            <p className="section-title">Kategorien</p>
            {renderCategoryPicker(manualFilters.categories, (categoryId) =>
              updateManualFilters({
                categories: manualFilters.categories.includes(categoryId)
                  ? manualFilters.categories.filter((item) => item !== categoryId)
                  : [...manualFilters.categories, categoryId]
              })
            )}
          </div>
        </section>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Suchergebnisse</p>
              <h2>Gefundene Deals</h2>
            </div>
            <p className="text-muted" style={{ margin: 0 }}>
              Kontrollierte Abfrage mit Paging, Filterlimit und sichtbarem Verbrauchs-Tracking.
            </p>
          </div>

          {manualResponse.items?.length ? (
            <>
              <div className="keepa-table-wrap">
                <table className="keepa-table">
                  <thead>
                    <tr>
                      <th>Produkt</th>
                      <th>ASIN</th>
                      <th>Preis</th>
                      <th>Rabatt</th>
                      <th>Referenz / Verlauf</th>
                      <th>Verkaeufer</th>
                      <th>Kategorie</th>
                      <th>Score</th>
                      <th>Status</th>
                      <th>Link</th>
                    </tr>
                  </thead>
                  <tbody>
                    {manualResponse.items.map((item) => (
                      <tr key={item.id}>
                        <td>
                          <div className="keepa-product-cell">
                            <img src={item.imageUrl || FALLBACK_IMAGE} alt={item.title} />
                            <div>
                              <strong>{item.title}</strong>
                              <small className="text-muted">{item.comparisonSource || 'Keine Vergleichsquelle'}</small>
                            </div>
                          </div>
                        </td>
                        <td>{item.asin}</td>
                        <td>{formatCurrency(item.currentPrice)}</td>
                        <td>{formatPercent(item.keepaDiscount)}</td>
                        <td>
                          <strong>{formatCurrency(item.referencePrice)}</strong>
                          <small className="text-muted">{item.referenceLabel}</small>
                        </td>
                        <td>{item.sellerType}</td>
                        <td>{item.categoryName || '-'}</td>
                        <td>
                          <strong>{item.dealScore}</strong>
                          <small className="text-muted">Risk {item.fakeDrop?.fakeDropRisk ?? '-'}</small>
                        </td>
                        <td>
                          <div className="keepa-card-tags">
                            <span className={`status-chip ${getStrengthChip(item.dealStrength)}`}>{strengthLabels[item.dealStrength]}</span>
                            {item.fakeDrop && (
                              <span className={`status-chip ${getFakeDropChip(item.fakeDrop.classification)}`}>{item.fakeDrop.classificationLabel}</span>
                            )}
                          </div>
                        </td>
                        <td>
                          <a href={item.productUrl} target="_blank" rel="noreferrer">
                            Produkt
                          </a>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="keepa-results-grid mobile-only">
                {manualResponse.items.map((item) => (
                  <article key={item.id} className="keepa-result-card">
                    <div className="keepa-result-top">
                      <img src={item.imageUrl || FALLBACK_IMAGE} alt={item.title} />
                      <div>
                        <strong>{item.title}</strong>
                        <p className="text-muted">{item.asin}</p>
                      </div>
                    </div>
                    <div className="keepa-card-metrics">
                      <span>{formatCurrency(item.currentPrice)}</span>
                      <span>{formatPercent(item.keepaDiscount)}</span>
                      <span>{item.dealScore}</span>
                    </div>
                    <div className="keepa-card-tags">
                      <span className={`status-chip ${getStrengthChip(item.dealStrength)}`}>{strengthLabels[item.dealStrength]}</span>
                      <span className="status-chip info">{item.sellerType}</span>
                      {item.fakeDrop && <span className={`status-chip ${getFakeDropChip(item.fakeDrop.classification)}`}>{item.fakeDrop.classificationLabel}</span>}
                    </div>
                  </article>
                ))}
              </div>

              <div className="keepa-pagination">
                <button className="secondary" onClick={() => void handleManualSearch((manualFilters.page || 1) - 1)} disabled={manualLoading || (manualFilters.page || 1) <= 1}>
                  Vorherige Seite
                </button>
                <span className="text-muted">
                  Seite {manualResponse.pagination?.page || manualFilters.page || 1} - Roh-Treffer {manualResponse.pagination?.rawResultCount || 0}
                </span>
                <button
                  className="secondary"
                  onClick={() => void handleManualSearch((manualFilters.page || 1) + 1)}
                  disabled={manualLoading || !manualResponse.pagination?.hasMore}
                >
                  Naechste Seite
                </button>
              </div>
            </>
          ) : (
            <p className="text-muted">{manualLoading ? 'Suche laeuft...' : 'Noch keine manuelle Suche ausgefuehrt.'}</p>
          )}
        </section>
      </div>
    );
  }

  function renderAutomatikTab() {
    const activeAutoDrawerConfig = getDrawerConfig(activeAutomationDrawer);

    return (
      <div className="keepa-section-stack">
        <div className="responsive-grid">
          <section className="card keepa-metric-card">
            <p className="section-title">Globaler Auto-Modus</p>
            <h2>{settingsForm.schedulerEnabled ? 'Aktiv' : 'Pausiert'}</h2>
            <p className="text-muted">Nur aktive Schubladen duerfen im Hintergrund laden und an die Testgruppe ausgeben.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Vergleichs-Adapter</p>
            <h2>{comparisonAdapters.filter((item) => item.enabled).length}</h2>
            <p className="text-muted">Aktive legale Vergleichsquellen oder sichere Platzhalter.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Letzter Automatik-Lauf</p>
            <h2>{formatDateTime(usageSummary?.lastAutomationRun?.createdAt)}</h2>
            <p className="text-muted">
              {usageSummary?.lastAutomationRun?.resultCount || 0} Treffer, {formatUsage(usageSummary?.lastAutomationRun?.estimatedUsage)}.
            </p>
          </section>
        </div>

        <section className="card keepa-banner">
          <div>
            <p className="section-title">Alert-Gating</p>
            <h2 className="page-title">Automatik trennt starke Deals von Review-Faellen</h2>
            <p className="page-subtitle">
              Alerts gehen nur bei starkem Deal-Score und niedrigem Fake-Drop-Risiko oder hoher Amazon-Confidence raus, waehrend unsichere Treffer automatisch in die Review Queue wandern.
            </p>
          </div>
          <span className="status-chip info">Review Queue aktiv</span>
        </section>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Schubladen-Steuerung</p>
              <h2>Auto-Modus nur pro aktivierter Schublade</h2>
            </div>
            <div className="keepa-inline-actions">
              {isAdmin && (
                <button className="secondary" onClick={() => void handleSaveSettings()} disabled={savingSettings}>
                  {savingSettings ? 'Speichert...' : 'Schubladen speichern'}
                </button>
              )}
              <button className="primary" onClick={() => loadDrawerIntoRuleForm(activeAutomationDrawer)}>
                Schublade in Regel laden
              </button>
            </div>
          </div>

          {renderDrawerCards('automation')}

          <div className="keepa-flag-grid">
            <label className="checkbox-card">
              <span>Schublade aktiv</span>
              <input
                type="checkbox"
                checked={activeAutoDrawerConfig.active}
                disabled={!isAdmin}
                onChange={(event) => updateDrawerConfig(activeAutomationDrawer, { active: event.target.checked })}
              />
            </label>
            <label className="checkbox-card">
              <span>Auto-Modus erlaubt</span>
              <input
                type="checkbox"
                checked={activeAutoDrawerConfig.autoModeAllowed}
                disabled={!isAdmin}
                onChange={(event) => updateDrawerConfig(activeAutomationDrawer, { autoModeAllowed: event.target.checked })}
              />
            </label>
            <label className="checkbox-card">
              <span>Muster-Unterstuetzung aktiv</span>
              <input
                type="checkbox"
                checked={activeAutoDrawerConfig.patternSupportEnabled}
                disabled={!isAdmin}
                onChange={(event) =>
                  updateDrawerConfig(activeAutomationDrawer, { patternSupportEnabled: event.target.checked })
                }
              />
            </label>
            <label className="checkbox-card">
              <span>Testgruppen-Posting erlaubt</span>
              <input
                type="checkbox"
                checked={activeAutoDrawerConfig.testGroupPostingAllowed}
                disabled={!isAdmin}
                onChange={(event) =>
                  updateDrawerConfig(activeAutomationDrawer, {
                    testGroupPostingAllowed: event.target.checked
                  })
                }
              />
            </label>
          </div>

          <div className="keepa-card-metrics three">
            <span>
              <strong>Drawer:</strong> {activeAutomationDrawer}
            </span>
            <span>
              <strong>Muster:</strong> {activeAutoDrawerConfig.patternSupportEnabled ? 'aktiv' : 'deaktiviert'}
            </span>
            <span>
              <strong>Intervall:</strong>{' '}
              {keepaTrendIntervalOptions.find((item) => item.value === activeAutoDrawerConfig.trendInterval)?.label || activeAutoDrawerConfig.trendInterval}
            </span>
            <span>
              <strong>Rabatt:</strong> ab {activeAutoDrawerConfig.minDiscount}%
            </span>
            <span>
              <strong>Amazon-Angebot:</strong>{' '}
              {keepaAmazonOfferOptions.find((item) => item.value === activeAutoDrawerConfig.amazonOfferMode)?.label || activeAutoDrawerConfig.amazonOfferMode}
            </span>
            <span>
              <strong>Sortierung:</strong>{' '}
              {keepaSortOptions.find((item) => item.value === activeAutoDrawerConfig.sortBy)?.label || activeAutoDrawerConfig.sortBy}
            </span>
            <span>
              <strong>Schutz:</strong> Lern-Logik Pflicht
            </span>
          </div>
        </section>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Automatik-Regel</p>
              <h2>{ruleForm.id ? 'Regel bearbeiten' : 'Neue Regel anlegen'}</h2>
            </div>
            {isAdmin && (
              <button className="primary" onClick={() => void handleSaveRule()} disabled={savingRule}>
                {savingRule ? 'Speichert...' : ruleForm.id ? 'Regel speichern' : 'Regel erstellen'}
              </button>
            )}
          </div>

          <div className="keepa-form-grid">
            <label>
              <span className="section-title">Regelname</span>
              <input value={ruleForm.name} onChange={(event) => setRuleForm((prev) => ({ ...prev, name: event.target.value }))} placeholder="Blitzangebote Elektronik" />
            </label>
            <label>
              <span className="section-title">Mindest-Rabatt</span>
              <input type="number" value={ruleForm.minDiscount} onChange={(event) => setRuleForm((prev) => ({ ...prev, minDiscount: Number(event.target.value || 0) }))} />
            </label>
            <label>
              <span className="section-title">Verkaeufer-Typ</span>
              <select value={ruleForm.sellerType} onChange={(event) => setRuleForm((prev) => ({ ...prev, sellerType: event.target.value }))}>
                {sellerTypeOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span className="section-title">Mindestpreis</span>
              <input type="number" value={ruleForm.minPrice} onChange={(event) => setRuleForm((prev) => ({ ...prev, minPrice: event.target.value }))} />
            </label>
            <label>
              <span className="section-title">Hoechstpreis</span>
              <input type="number" value={ruleForm.maxPrice} onChange={(event) => setRuleForm((prev) => ({ ...prev, maxPrice: event.target.value }))} />
            </label>
            <label>
              <span className="section-title">Mindest-Deal-Score</span>
              <input type="number" value={ruleForm.minDealScore} onChange={(event) => setRuleForm((prev) => ({ ...prev, minDealScore: Number(event.target.value || 0) }))} />
            </label>
            <label>
              <span className="section-title">Pruefintervall (Minuten)</span>
              <input type="number" value={ruleForm.intervalMinutes} onChange={(event) => setRuleForm((prev) => ({ ...prev, intervalMinutes: Number(event.target.value || 0) }))} />
            </label>
          </div>

          <div className="keepa-flag-grid">
            <label className="checkbox-card">
              <span>Nur Prime</span>
              <input type="checkbox" checked={ruleForm.onlyPrime} onChange={(event) => setRuleForm((prev) => ({ ...prev, onlyPrime: event.target.checked }))} />
            </label>
            <label className="checkbox-card">
              <span>Nur lagernd</span>
              <input type="checkbox" checked={ruleForm.onlyInStock} onChange={(event) => setRuleForm((prev) => ({ ...prev, onlyInStock: event.target.checked }))} />
            </label>
            <label className="checkbox-card">
              <span>Nur gute Bewertung</span>
              <input type="checkbox" checked={ruleForm.onlyGoodRating} onChange={(event) => setRuleForm((prev) => ({ ...prev, onlyGoodRating: event.target.checked }))} />
            </label>
            <label className="checkbox-card">
              <span>Aktiv</span>
              <input type="checkbox" checked={ruleForm.isActive} onChange={(event) => setRuleForm((prev) => ({ ...prev, isActive: event.target.checked }))} />
            </label>
          </div>

          <div>
            <p className="section-title">Kategorien</p>
            {renderCategoryPicker(ruleForm.categories, (categoryId) => toggleCategory(setRuleForm, categoryId))}
          </div>

          <div>
            <p className="section-title">Price Comparison Adapters</p>
            <div className="keepa-flag-grid">
              {comparisonAdapters.map((adapter) => (
                <label key={adapter.id} className="checkbox-card">
                  <span>
                    <strong>{adapter.name}</strong>
                    <small className="text-muted">{adapter.description}</small>
                  </span>
                  <input
                    type="checkbox"
                    checked={ruleForm.comparisonSources.includes(adapter.id)}
                    onChange={() => toggleComparisonSource(adapter.id)}
                  />
                </label>
              ))}
            </div>
          </div>
        </section>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Aktive Regeln</p>
              <h2>Bestehende Automatik</h2>
            </div>
          </div>

          <div className="keepa-list">
            {rules.map((rule) => (
              <article key={rule.id} className="keepa-rule-card">
                <div className="keepa-rule-card-top">
                  <div>
                    <strong>{rule.name}</strong>
                    <p className="text-muted">
                      Rabatt ab {rule.minDiscount}% - Score ab {rule.minDealScore} - {rule.intervalMinutes} Minuten
                    </p>
                  </div>
                  <span className={`status-chip ${rule.isActive ? 'success' : 'warning'}`}>{rule.isActive ? 'Aktiv' : 'Pausiert'}</span>
                </div>

                <div className="keepa-card-metrics three">
                  <span>
                    <strong>Letzter Lauf:</strong> {formatDateTime(rule.lastRunAt)}
                  </span>
                  <span>
                    <strong>Naechster Lauf:</strong> {formatDateTime(rule.nextRunAt)}
                  </span>
                  <span>
                    <strong>Treffer gesamt:</strong> {rule.totalHits || 0}
                  </span>
                  <span>
                    <strong>Alerts gesendet:</strong> {rule.alertsSent || 0}
                  </span>
                  <span>
                    <strong>Verbrauch:</strong> {formatUsage(rule.estimatedUsageTotal)}
                  </span>
                  <span>
                    <strong>Letzte Laufzeit:</strong> {formatDuration(rule.lastDurationMs)}
                  </span>
                </div>

                <div className="keepa-card-tags">
                  <span className="status-chip info">{rule.sellerType}</span>
                  {(rule.categories || []).slice(0, 3).map((categoryId) => {
                    const category = categoryCatalog.find((item) => item.id === categoryId);
                    return (
                      <span key={categoryId} className="status-chip info">
                        {category?.name || categoryId}
                      </span>
                    );
                  })}
                </div>

                {isAdmin && (
                  <div className="keepa-inline-actions">
                    <button className="secondary small" onClick={() => setRuleForm({ ...rule })}>
                      Bearbeiten
                    </button>
                    <button className="secondary small" onClick={() => void handleToggleRule(rule, !rule.isActive)}>
                      {rule.isActive ? 'Pausieren' : 'Aktivieren'}
                    </button>
                  </div>
                )}
              </article>
            ))}
            {!rules.length && <p className="text-muted">Noch keine Automatik-Regeln vorhanden.</p>}
          </div>
        </section>
      </div>
    );
  }

  function renderResultsTab() {
    const selectedDraft = selectedResult ? resultDrafts[selectedResult.id] || {} : {};

    return (
      <div className="keepa-section-stack">
        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Ergebnisse</p>
              <h2>Alle gespeicherten Keepa-Treffer</h2>
            </div>
            <button className="primary" onClick={() => void handleApplyResultsFilters(1)}>
              Filter anwenden
            </button>
          </div>

          <div className="keepa-form-grid">
            <label>
              <span className="section-title">Status</span>
              <select value={resultsFilters.workflowStatus} onChange={(event) => setResultsFilters((prev) => ({ ...prev, workflowStatus: event.target.value }))}>
                {workflowStatusOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span className="section-title">Kategorie</span>
              <select value={resultsFilters.categoryId} onChange={(event) => setResultsFilters((prev) => ({ ...prev, categoryId: event.target.value }))}>
                <option value="">Alle Kategorien</option>
                {categoryCatalog.map((category) => (
                  <option key={category.id} value={category.id}>
                    {category.name}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span className="section-title">Rabatt ab</span>
              <input type="number" value={resultsFilters.minDiscount} onChange={(event) => setResultsFilters((prev) => ({ ...prev, minDiscount: event.target.value }))} />
            </label>
            <label>
              <span className="section-title">Deal-Score ab</span>
              <input type="number" value={resultsFilters.minDealScore} onChange={(event) => setResultsFilters((prev) => ({ ...prev, minDealScore: event.target.value }))} />
            </label>
          </div>
        </section>

        <div className="keepa-results-layout">
          <section className="card keepa-panel">
            <div className="keepa-list">
              {results.items.map((item) => (
                <button
                  key={item.id}
                  className={`keepa-result-card selectable ${selectedResultId === item.id ? 'active' : ''}`}
                  onClick={() => setSelectedResultId(item.id)}
                >
                  <div className="keepa-result-top">
                    <img src={item.imageUrl || FALLBACK_IMAGE} alt={item.title} />
                    <div>
                      <strong>{item.title}</strong>
                      <p className="text-muted">
                        {item.asin} - {item.categoryName || '-'}
                      </p>
                    </div>
                  </div>
                  <div className="keepa-card-metrics">
                    <span>{formatCurrency(item.currentPrice)}</span>
                    <span>{formatPercent(item.keepaDiscount)}</span>
                    <span>{item.dealScore}</span>
                  </div>
                  <div className="keepa-card-tags">
                    <span className={`status-chip ${getStrengthChip(item.dealStrength)}`}>{strengthLabels[item.dealStrength]}</span>
                    <span className={`status-chip ${getWorkflowChip(item.workflowStatus)}`}>{workflowLabels[item.workflowStatus]}</span>
                    {item.fakeDrop && (
                      <span className={`status-chip ${getFakeDropChip(item.fakeDrop.classification)}`}>
                        {item.fakeDrop.classificationLabel}
                      </span>
                    )}
                  </div>
                </button>
              ))}
              {!results.items.length && <p className="text-muted">Noch keine gespeicherten Ergebnisse gefunden.</p>}
            </div>

            <div className="keepa-pagination">
              <button className="secondary" onClick={() => void handleApplyResultsFilters(results.pagination.page - 1)} disabled={results.pagination.page <= 1}>
                Vorherige Seite
              </button>
              <span className="text-muted">
                Seite {results.pagination.page} von {results.pagination.totalPages}
              </span>
              <button
                className="secondary"
                onClick={() => void handleApplyResultsFilters(results.pagination.page + 1)}
                disabled={results.pagination.page >= results.pagination.totalPages}
              >
                Naechste Seite
              </button>
            </div>
          </section>

          <aside className="card keepa-panel keepa-drawer">
            {selectedResult ? (
              <>
                <div className="keepa-panel-header">
                  <div>
                    <p className="section-title">Detailansicht</p>
                    <h2>{selectedResult.title}</h2>
                  </div>
                  <div className="keepa-card-tags">
                    <span className={`status-chip ${getStrengthChip(selectedResult.dealStrength)}`}>{strengthLabels[selectedResult.dealStrength]}</span>
                    <span className={`status-chip ${getWorkflowChip(selectedResult.workflowStatus)}`}>{workflowLabels[selectedResult.workflowStatus]}</span>
                  </div>
                </div>

                <div className="keepa-drawer-grid">
                  <div>
                    <span className="section-title">Produktdaten</span>
                    <p>ASIN: {selectedResult.asin}</p>
                    <p>Verkaeufer: {selectedResult.sellerType}</p>
                    <p>Kategorie: {selectedResult.categoryName || '-'}</p>
                    <p>Zuletzt aktualisiert: {formatDateTime(selectedResult.updatedAt)}</p>
                  </div>
                  <div>
                    <span className="section-title">Preisinfo</span>
                    <p>Aktueller Preis: {formatCurrency(selectedResult.currentPrice)}</p>
                    <p>Referenz: {formatCurrency(selectedResult.referencePrice)}</p>
                    <p>Keepa-Rabatt: {formatPercent(selectedResult.keepaDiscount)}</p>
                    <p>Deal-Score: {selectedResult.dealScore}</p>
                  </div>
                </div>

                <div className="keepa-info-card">
                  <p className="section-title">Vergleichspreis</p>
                  <p>Quelle: {selectedResult.comparisonSource || 'Nicht verbunden'}</p>
                  <p>Preis: {formatCurrency(selectedResult.comparisonPrice)}</p>
                  <p>Preisunterschied: {formatCurrency(selectedResult.priceDifferenceAbs)}</p>
                  <p>Preisunterschied %: {formatPercent(selectedResult.priceDifferencePct)}</p>
                </div>

                <div className="keepa-info-card">
                  <p className="section-title">Begruendung Deal-Score</p>
                  <p className="text-muted">{selectedResult.strengthReason || 'Keine Begruendung verfuegbar.'}</p>
                </div>

                <div className="keepa-info-card">
                  <p className="section-title">Fake-Drop Analyse</p>
                  {selectedResult.fakeDrop ? (
                    <div className="keepa-section-stack">
                      <div className="keepa-card-tags">
                        <span className={`status-chip ${getFakeDropChip(selectedResult.fakeDrop.classification)}`}>
                          {selectedResult.fakeDrop.classificationLabel}
                        </span>
                        <span className="status-chip info">Risk {selectedResult.fakeDrop.fakeDropRisk}</span>
                        <span className="status-chip info">Stability {selectedResult.fakeDrop.stabilityScore}</span>
                        <span className="status-chip info">Amazon {selectedResult.fakeDrop.amazonConfidence}</span>
                      </div>
                      <MiniPriceHistory points={selectedResult.fakeDrop.chartPoints || []} accent="#f59e0b" />
                      <p className="text-muted">
                        {selectedResult.fakeDrop.analysisReason || 'Noch keine Analyse-Begruendung gespeichert.'}
                      </p>
                    </div>
                  ) : (
                    <p className="text-muted">Noch keine Fake-Drop-Analyse fuer diesen Treffer vorhanden.</p>
                  )}
                </div>

                <label>
                  <span className="section-title">Workflow-Status</span>
                  <select
                    value={selectedDraft.workflowStatus || selectedResult.workflowStatus}
                    onChange={(event) =>
                      setResultDrafts((prev) => ({
                        ...prev,
                        [selectedResult.id]: {
                          ...(prev[selectedResult.id] || {}),
                          workflowStatus: event.target.value
                        }
                      }))
                    }
                  >
                    {workflowStatusOptions.slice(1).map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>

                <div className="keepa-form-grid">
                  <label>
                    <span className="section-title">Vergleichsquelle</span>
                    <input
                      value={selectedDraft.comparisonSource || ''}
                      onChange={(event) =>
                        setResultDrafts((prev) => ({
                          ...prev,
                          [selectedResult.id]: {
                            ...(prev[selectedResult.id] || {}),
                            comparisonSource: event.target.value
                          }
                        }))
                      }
                      placeholder="Manuelle Vergleichsquelle"
                    />
                  </label>
                  <label>
                    <span className="section-title">Vergleichspreis</span>
                    <input
                      type="number"
                      value={selectedDraft.comparisonPrice ?? ''}
                      onChange={(event) =>
                        setResultDrafts((prev) => ({
                          ...prev,
                          [selectedResult.id]: {
                            ...(prev[selectedResult.id] || {}),
                            comparisonPrice: event.target.value
                          }
                        }))
                      }
                    />
                  </label>
                </div>

                <label>
                  <span className="section-title">Notiz</span>
                  <textarea
                    rows="4"
                    value={selectedDraft.note || ''}
                    onChange={(event) =>
                      setResultDrafts((prev) => ({
                        ...prev,
                        [selectedResult.id]: {
                          ...(prev[selectedResult.id] || {}),
                          note: event.target.value
                        }
                      }))
                    }
                    placeholder="Interne Notiz zum Treffer"
                  />
                </label>

                <div className="keepa-inline-actions">
                  <button className="primary" onClick={() => void handleSaveResult(selectedResult.id)}>
                    Speichern
                  </button>
                  <button className="secondary" onClick={() => void handleSaveResult(selectedResult.id, 'geprueft')}>
                    Freigeben
                  </button>
                  <button className="secondary" onClick={() => void handleSaveResult(selectedResult.id, 'verworfen')}>
                    Verwerfen
                  </button>
                  <a className="secondary keepa-link-button" href={selectedResult.productUrl} target="_blank" rel="noreferrer">
                    Produkt oeffnen
                  </a>
                </div>
              </>
            ) : (
              <p className="text-muted">Waehle links einen Treffer, um die Detailansicht zu oeffnen.</p>
            )}
          </aside>
        </div>
      </div>
    );
  }

  function renderNotificationsTab() {
    return (
      <div className="keepa-section-stack">
        <div className="responsive-grid">
          <section className="card keepa-metric-card">
            <p className="section-title">Telegram Alert</p>
            <h2>{settingsForm.alertTelegramEnabled ? 'Aktiv' : 'Inaktiv'}</h2>
            <p className="text-muted">
              {statusData?.settings?.telegramConfigStatus?.botTokenConfigured ? 'Bot im Backend konfiguriert.' : 'Bot-Token fehlt im Backend.'}
            </p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Interne Nachricht</p>
            <h2>{settingsForm.alertInternalEnabled ? 'Aktiv' : 'Inaktiv'}</h2>
            <p className="text-muted">Interne Keepa-Meldungen bleiben im Affiliate Manager Pro sichtbar.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">WhatsApp</p>
            <h2>{settingsForm.alertWhatsappPlaceholderEnabled ? 'Platzhalter aktiv' : 'Platzhalter aus'}</h2>
            <p className="text-muted">Nur als deaktivierbarer sicherer Platzhalter ohne Live-Anbindung.</p>
          </section>
        </div>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Alert-Regeln</p>
              <h2>Cooldown und Duplikat-Schutz</h2>
            </div>
            {isAdmin && (
              <button className="primary" onClick={() => void handleTestAlert()} disabled={testingAlert}>
                {testingAlert ? 'Testet...' : 'Test-Alert senden'}
              </button>
            )}
          </div>
          <div className="keepa-card-metrics three">
            <span>Cooldown {settingsForm.alertCooldownMinutes} Min.</span>
            <span>Max {settingsForm.alertMaxPerProduct} Alerts / Produkt</span>
            <span>Duplikate werden serverseitig geblockt</span>
          </div>
        </section>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Alert-Historie</p>
              <h2>Gesendete und blockierte Benachrichtigungen</h2>
            </div>
          </div>
          <div className="keepa-table-wrap">
            <table className="keepa-table">
              <thead>
                <tr>
                  <th>Kanal</th>
                  <th>Status</th>
                  <th>Preview</th>
                  <th>Fehler</th>
                  <th>Zeitpunkt</th>
                </tr>
              </thead>
              <tbody>
                {alerts.map((item) => (
                  <tr key={item.id}>
                    <td>{item.channelType}</td>
                    <td>
                      <span className={`status-chip ${item.status === 'failed' ? 'danger' : item.status === 'sent' ? 'success' : 'info'}`}>
                        {item.status}
                      </span>
                    </td>
                    <td>{item.messagePreview || item.asin}</td>
                    <td>{item.errorMessage || '-'}</td>
                    <td>{formatDateTime(item.createdAt)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {!alerts.length && <p className="text-muted">Noch keine Alert-Historie vorhanden.</p>}
        </section>
      </div>
    );
  }

  function renderSettingsTab() {
    return (
      <div className="keepa-section-stack">
        <div className="responsive-grid">
          <section className="card keepa-metric-card">
            <p className="section-title">Keepa API Key</p>
            <h2>{statusData?.settings?.keepaKeyStatus?.connected ? 'Verbunden' : 'Nicht gesetzt'}</h2>
            <p className="text-muted">{statusData?.settings?.keepaKeyStatus?.masked || 'API-Key bleibt nur im Backend / .env.'}</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Telegram</p>
            <h2>
              {statusData?.settings?.telegramConfigStatus?.botTokenConfigured &&
              statusData?.settings?.telegramConfigStatus?.chatIdConfigured
                ? 'Konfiguriert'
                : 'Unvollstaendig'}
            </h2>
            <p className="text-muted">
              Chat-ID: {statusData?.settings?.telegramConfigStatus?.maskedChatId || 'nicht hinterlegt'}
            </p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Verbindung testen</p>
            <h2>{statusData?.connection?.connected ? 'OK' : 'Pruefen'}</h2>
            <button className="secondary" onClick={() => void handleTestConnection()} disabled={!isAdmin || testingConnection}>
              {testingConnection ? 'Prueft...' : 'Test-Verbindung'}
            </button>
          </section>
        </div>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Standardwerte</p>
              <h2>Keepa Grundeinstellungen</h2>
            </div>
            {isAdmin && (
              <button className="primary" onClick={() => void handleSaveSettings()} disabled={savingSettings}>
                {savingSettings ? 'Speichert...' : 'Speichern'}
              </button>
            )}
          </div>

          <div className="keepa-flag-grid">
            <label className="checkbox-card">
              <span>Keepa aktiviert</span>
              <input type="checkbox" checked={settingsForm.keepaEnabled} onChange={(event) => setSettingsForm((prev) => ({ ...prev, keepaEnabled: event.target.checked }))} />
            </label>
            <label className="checkbox-card">
              <span>Scheduler aktiv</span>
              <input type="checkbox" checked={settingsForm.schedulerEnabled} onChange={(event) => setSettingsForm((prev) => ({ ...prev, schedulerEnabled: event.target.checked }))} />
            </label>
            <label className="checkbox-card">
              <span>Logging aktiv</span>
              <input type="checkbox" checked={settingsForm.loggingEnabled} onChange={(event) => setSettingsForm((prev) => ({ ...prev, loggingEnabled: event.target.checked }))} />
            </label>
          </div>

          <div className="keepa-form-grid">
            <label>
              <span className="section-title">Marketplace</span>
              <select value={settingsForm.domainId} onChange={(event) => setSettingsForm((prev) => ({ ...prev, domainId: Number(event.target.value) }))}>
                {(statusData?.settings?.domainOptions || []).map((option) => (
                  <option key={option.id} value={option.id}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span className="section-title">Standard-Rabatt</span>
              <input type="number" value={settingsForm.defaultDiscount} onChange={(event) => setSettingsForm((prev) => ({ ...prev, defaultDiscount: Number(event.target.value || 0) }))} />
            </label>
            <label>
              <span className="section-title">Standard-Verkaeufer</span>
              <select value={settingsForm.defaultSellerType} onChange={(event) => setSettingsForm((prev) => ({ ...prev, defaultSellerType: event.target.value }))}>
                {sellerTypeOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span className="section-title">Standard-Minimumpreis</span>
              <input type="number" value={settingsForm.defaultMinPrice} onChange={(event) => setSettingsForm((prev) => ({ ...prev, defaultMinPrice: event.target.value }))} />
            </label>
            <label>
              <span className="section-title">Standard-Hoechstpreis</span>
              <input type="number" value={settingsForm.defaultMaxPrice} onChange={(event) => setSettingsForm((prev) => ({ ...prev, defaultMaxPrice: event.target.value }))} />
            </label>
            <label>
              <span className="section-title">Pruefintervall</span>
              <input type="number" value={settingsForm.defaultIntervalMinutes} onChange={(event) => setSettingsForm((prev) => ({ ...prev, defaultIntervalMinutes: Number(event.target.value || 0) }))} />
            </label>
            <label>
              <span className="section-title">Seitenlimit</span>
              <input type="number" value={settingsForm.defaultPageSize} onChange={(event) => setSettingsForm((prev) => ({ ...prev, defaultPageSize: Number(event.target.value || 0) }))} />
            </label>
            <label>
              <span className="section-title">Starker Deal ab Rabatt</span>
              <input type="number" value={settingsForm.strongDealMinDiscount} onChange={(event) => setSettingsForm((prev) => ({ ...prev, strongDealMinDiscount: Number(event.target.value || 0) }))} />
            </label>
            <label>
              <span className="section-title">Vergleichsdifferenz ab %</span>
              <input type="number" value={settingsForm.strongDealMinComparisonGapPct} onChange={(event) => setSettingsForm((prev) => ({ ...prev, strongDealMinComparisonGapPct: Number(event.target.value || 0) }))} />
            </label>
            <label>
              <span className="section-title">Gute Bewertung ab</span>
              <input type="number" step="0.1" value={settingsForm.goodRatingThreshold} onChange={(event) => setSettingsForm((prev) => ({ ...prev, goodRatingThreshold: Number(event.target.value || 0) }))} />
            </label>
            <label>
              <span className="section-title">Telegram Prefix</span>
              <input value={settingsForm.telegramMessagePrefix} onChange={(event) => setSettingsForm((prev) => ({ ...prev, telegramMessagePrefix: event.target.value }))} />
            </label>
            <label>
              <span className="section-title">Geschaetzte Tokens / manueller Lauf</span>
              <input type="number" value={settingsForm.estimatedTokensPerManualRun} onChange={(event) => setSettingsForm((prev) => ({ ...prev, estimatedTokensPerManualRun: Number(event.target.value || 0) }))} />
            </label>
          </div>

          <div>
            <p className="section-title">Standard-Kategorien</p>
            {renderCategoryPicker(settingsForm.defaultCategories, (categoryId) => toggleCategory(setSettingsForm, categoryId))}
          </div>
        </section>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Alert-Einstellungen</p>
              <h2>Benachrichtigungen und Vergleichsquellen</h2>
            </div>
          </div>

          <div className="keepa-flag-grid">
            <label className="checkbox-card">
              <span>Telegram Alert</span>
              <input type="checkbox" checked={settingsForm.alertTelegramEnabled} onChange={(event) => setSettingsForm((prev) => ({ ...prev, alertTelegramEnabled: event.target.checked }))} />
            </label>
            <label className="checkbox-card">
              <span>Interne Nachricht</span>
              <input type="checkbox" checked={settingsForm.alertInternalEnabled} onChange={(event) => setSettingsForm((prev) => ({ ...prev, alertInternalEnabled: event.target.checked }))} />
            </label>
            <label className="checkbox-card">
              <span>WhatsApp Platzhalter</span>
              <input type="checkbox" checked={settingsForm.alertWhatsappPlaceholderEnabled} onChange={(event) => setSettingsForm((prev) => ({ ...prev, alertWhatsappPlaceholderEnabled: event.target.checked }))} />
            </label>
          </div>

          <div className="keepa-form-grid">
            <label>
              <span className="section-title">Cooldown Minuten</span>
              <input type="number" value={settingsForm.alertCooldownMinutes} onChange={(event) => setSettingsForm((prev) => ({ ...prev, alertCooldownMinutes: Number(event.target.value || 0) }))} />
            </label>
            <label>
              <span className="section-title">Max Alerts pro Produkt</span>
              <input type="number" value={settingsForm.alertMaxPerProduct} onChange={(event) => setSettingsForm((prev) => ({ ...prev, alertMaxPerProduct: Number(event.target.value || 0) }))} />
            </label>
          </div>

          <div>
            <p className="section-title">Vergleichsquellen-Konfiguration</p>
            <div className="keepa-flag-grid">
              {comparisonAdapters.map((adapter) => (
                <label key={adapter.id} className="checkbox-card">
                  <span>
                    <strong>{adapter.name}</strong>
                    <small className="text-muted">{adapter.description}</small>
                  </span>
                  <input
                    type="checkbox"
                    checked={Boolean(settingsForm.comparisonSourceConfig?.[adapter.id]?.enabled)}
                    onChange={(event) => updateSettingsAdapter(adapter.id, event.target.checked)}
                  />
                </label>
              ))}
            </div>
          </div>
        </section>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Fake-Drop Engine</p>
              <h2>Heuristiken, Schwellen und Trainings-Feedback</h2>
            </div>
            {isAdmin && (
              <div className="keepa-inline-actions">
                <button className="secondary" onClick={() => void handleRecalculateFakeDrop()} disabled={recalculatingFakeDrop}>
                  {recalculatingFakeDrop ? 'Berechnet neu...' : 'Neu berechnen'}
                </button>
                <button className="primary" onClick={() => void handleSaveFakeDropSettings()} disabled={savingFakeDropSettings}>
                  {savingFakeDropSettings ? 'Speichert...' : 'Heuristiken speichern'}
                </button>
              </div>
            )}
          </div>

          <div className="keepa-flag-grid">
            <label className="checkbox-card">
              <span>Fake-Drop-Engine aktiv</span>
              <input
                type="checkbox"
                checked={fakeDropSettingsForm.engineEnabled}
                onChange={(event) => setFakeDropSettingsForm((prev) => ({ ...prev, engineEnabled: event.target.checked }))}
              />
            </label>
            <div className="keepa-info-card">
              <p className="section-title">Trainings-Feedback</p>
              <div className="keepa-feedback-list">
                {(statusData?.settings?.fakeDropSettings?.feedbackAdjustments || fakeDropSummary?.feedback || []).slice(0, 3).map((item) => (
                  <div key={item.sellerType} className="keepa-feedback-item">
                    <strong>{item.sellerType}</strong>
                    <span>{item.note || `${item.total || 0} Reviews, Anpassung ${item.riskAdjustment || 0}`}</span>
                  </div>
                ))}
                {!((statusData?.settings?.fakeDropSettings?.feedbackAdjustments || fakeDropSummary?.feedback || []).length) && (
                  <p className="text-muted">Noch kein Trainings-Feedback vorhanden.</p>
                )}
              </div>
            </div>
          </div>

          <div className="keepa-form-grid">
            <label>
              <span className="section-title">Low Risk bis</span>
              <input
                type="number"
                value={fakeDropSettingsForm.lowRiskThreshold}
                onChange={(event) => setFakeDropSettingsForm((prev) => ({ ...prev, lowRiskThreshold: Number(event.target.value || 0) }))}
              />
            </label>
            <label>
              <span className="section-title">High Risk ab</span>
              <input
                type="number"
                value={fakeDropSettingsForm.highRiskThreshold}
                onChange={(event) => setFakeDropSettingsForm((prev) => ({ ...prev, highRiskThreshold: Number(event.target.value || 0) }))}
              />
            </label>
            <label>
              <span className="section-title">Review Priority ab</span>
              <input
                type="number"
                value={fakeDropSettingsForm.reviewPriorityThreshold}
                onChange={(event) => setFakeDropSettingsForm((prev) => ({ ...prev, reviewPriorityThreshold: Number(event.target.value || 0) }))}
              />
            </label>
            <label>
              <span className="section-title">Amazon Confidence stark</span>
              <input
                type="number"
                value={fakeDropSettingsForm.amazonConfidenceStrong}
                onChange={(event) => setFakeDropSettingsForm((prev) => ({ ...prev, amazonConfidenceStrong: Number(event.target.value || 0) }))}
              />
            </label>
            <label>
              <span className="section-title">Stability stark</span>
              <input
                type="number"
                value={fakeDropSettingsForm.stabilityStrong}
                onChange={(event) => setFakeDropSettingsForm((prev) => ({ ...prev, stabilityStrong: Number(event.target.value || 0) }))}
              />
            </label>
            <label>
              <span className="section-title">Referenzpreis-Inflation ab %</span>
              <input
                type="number"
                value={fakeDropSettingsForm.referenceInflationThreshold}
                onChange={(event) => setFakeDropSettingsForm((prev) => ({ ...prev, referenceInflationThreshold: Number(event.target.value || 0) }))}
              />
            </label>
            <label>
              <span className="section-title">Volatilitaet Warnung ab %</span>
              <input
                type="number"
                value={fakeDropSettingsForm.volatilityWarningThreshold}
                onChange={(event) => setFakeDropSettingsForm((prev) => ({ ...prev, volatilityWarningThreshold: Number(event.target.value || 0) }))}
              />
            </label>
            <label>
              <span className="section-title">Kurzer Peak max Tage</span>
              <input
                type="number"
                value={fakeDropSettingsForm.shortPeakMaxDays}
                onChange={(event) => setFakeDropSettingsForm((prev) => ({ ...prev, shortPeakMaxDays: Number(event.target.value || 0) }))}
              />
            </label>
            <label>
              <span className="section-title">Spike-Sensitivitaet %</span>
              <input
                type="number"
                value={fakeDropSettingsForm.spikeSensitivity}
                onChange={(event) => setFakeDropSettingsForm((prev) => ({ ...prev, spikeSensitivity: Number(event.target.value || 0) }))}
              />
            </label>
            <label>
              <span className="section-title">Rebound-Fenster Tage</span>
              <input
                type="number"
                value={fakeDropSettingsForm.reboundWindowDays}
                onChange={(event) => setFakeDropSettingsForm((prev) => ({ ...prev, reboundWindowDays: Number(event.target.value || 0) }))}
              />
            </label>
            <label>
              <span className="section-title">Gewicht Stability</span>
              <input
                type="number"
                step="0.1"
                value={fakeDropSettingsForm.weights.stability}
                onChange={(event) =>
                  setFakeDropSettingsForm((prev) => ({
                    ...prev,
                    weights: { ...prev.weights, stability: Number(event.target.value || 0) }
                  }))
                }
              />
            </label>
            <label>
              <span className="section-title">Gewicht Manipulation</span>
              <input
                type="number"
                step="0.1"
                value={fakeDropSettingsForm.weights.manipulation}
                onChange={(event) =>
                  setFakeDropSettingsForm((prev) => ({
                    ...prev,
                    weights: { ...prev.weights, manipulation: Number(event.target.value || 0) }
                  }))
                }
              />
            </label>
            <label>
              <span className="section-title">Gewicht Amazon</span>
              <input
                type="number"
                step="0.1"
                value={fakeDropSettingsForm.weights.amazon}
                onChange={(event) =>
                  setFakeDropSettingsForm((prev) => ({
                    ...prev,
                    weights: { ...prev.weights, amazon: Number(event.target.value || 0) }
                  }))
                }
              />
            </label>
            <label>
              <span className="section-title">Gewicht Feedback</span>
              <input
                type="number"
                step="0.1"
                value={fakeDropSettingsForm.weights.feedback}
                onChange={(event) =>
                  setFakeDropSettingsForm((prev) => ({
                    ...prev,
                    weights: { ...prev.weights, feedback: Number(event.target.value || 0) }
                  }))
                }
              />
            </label>
          </div>
        </section>
      </div>
    );
  }

  function renderUsageTab() {
    const requestTracking = usageSummary?.requestTracking || {};
    const recent60s = requestTracking.windows?.last60s || {};
    const recent5m = requestTracking.windows?.last5m || {};
    const warningsActive = requestTracking.warningsActive || [];
    const latestKeepaRequests = (usageLogs.items || []).filter((item) => item.action === 'keepa-request').slice(0, 10);
    const usageSeries = usageHistory?.series || [];
    const hasTokenSeries = usageSeries.some((item) => Number(item.tokensUsed || 0) > 0);

    return (
      <div className="keepa-section-stack">
        <div className="responsive-grid">
          <section className="card keepa-metric-card">
            <p className="section-title">Letzter Keepa-Request</p>
            <h2>{formatDateTime(requestTracking?.lastRequest?.createdAt)}</h2>
            <p className="text-muted">
              {formatKeepaModeLabel(requestTracking?.lastRequest?.mode)} {requestTracking?.lastRequest?.drawerKey || '-'} - {formatUsage(requestTracking?.lastRequest?.tokensUsed)}
            </p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Tokens heute</p>
            <h2>{formatUsage(requestTracking?.tokensToday)}</h2>
            <p className="text-muted">{requestTracking?.requestsToday || 0} echte Keepa-Requests heute.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Verbrauch letzte Minute</p>
            <h2>{formatUsage(recent60s?.tokensUsed)}</h2>
            <p className="text-muted">
              {recent60s?.requestCount || 0} Requests in 60s.
            </p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Requests pro Minute</p>
            <h2>{requestTracking?.requestsPerMinute?.toFixed ? requestTracking.requestsPerMinute.toFixed(1) : requestTracking?.requestsPerMinute || '0.0'}</h2>
            <p className="text-muted">
              Basis: 5-Minuten-Fenster mit {recent5m?.requestCount || 0} Requests.
            </p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Ø Tokens / Request</p>
            <h2>{formatUsage(requestTracking?.averageTokensPerRequest)}</h2>
            <p className="text-muted">Heute uebers alle echten Keepa-Requests berechnet.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Ø Tokens / Ergebnis</p>
            <h2>{formatUsage(requestTracking?.averageTokensPerResult)}</h2>
            <p className="text-muted">{usageSummary?.today?.hitCount || 0} Treffer dienen als Basis.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Teuerster Request</p>
            <h2>{formatUsage(requestTracking?.expensiveRequest?.tokensUsed)}</h2>
            <p className="text-muted">
              {formatDateTime(requestTracking?.expensiveRequest?.createdAt)} - {requestTracking?.expensiveRequest?.drawerKey || '-'}
            </p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Ø pro Tag</p>
            <h2>{formatUsage(requestTracking?.averageTokensPerDay)}</h2>
            <p className="text-muted">Seit Monatsstart, ohne Fantasiewerte.</p>
          </section>
        </div>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Schutzstatus</p>
              <h2>Warnungen und Verbrauchsschutz</h2>
            </div>
          </div>
          <div className="keepa-list">
            {warningsActive.map((item) => (
              <div key={item.code} className="keepa-list-item static">
                <div>
                  <strong>{item.title}</strong>
                  <p className="text-muted">{item.message}</p>
                </div>
                <span className={`status-chip ${item.level === 'danger' ? 'danger' : 'warning'}`}>{item.code}</span>
              </div>
            ))}
            {!warningsActive.length && <p className="text-muted">Aktuell keine aktiven Keepa-Verbrauchswarnungen.</p>}
          </div>
        </section>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Filter</p>
              <h2>Verbrauch & Logs eingrenzen</h2>
            </div>
          </div>

          <div className="keepa-form-grid">
            <label>
              <span className="section-title">Zeitraum</span>
              <select value={usageFilters.range} onChange={(event) => void handleApplyUsageFilters({ range: event.target.value })}>
                {usageRangeOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span className="section-title">Modul</span>
              <select value={usageFilters.module} onChange={(event) => void handleApplyUsageFilters({ module: event.target.value })}>
                <option value="all">Alle Module</option>
                {(usageLogs.availableModules || []).map((item) => (
                  <option key={item.id} value={item.id}>
                    {item.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span className="section-title">Log-Limit</span>
              <select value={usageFilters.limit} onChange={(event) => void handleApplyUsageFilters({ limit: Number(event.target.value) })}>
                <option value="20">20</option>
                <option value="40">40</option>
                <option value="80">80</option>
                <option value="120">120</option>
              </select>
            </label>
          </div>
        </section>

        <div className="keepa-analytics-grid">
          <section className="card keepa-panel">
            <div className="keepa-panel-header">
              <div>
                <p className="section-title">Linienchart</p>
                <h2>{hasTokenSeries ? 'Tokenverbrauch ueber Zeit' : 'Keepa-Nutzung ueber Zeit'}</h2>
              </div>
            </div>
            <MiniLineChart data={usageSeries} valueKey={hasTokenSeries ? 'tokensUsed' : 'estimatedUsage'} color="#10b981" />
          </section>

          <section className="card keepa-panel">
            <div className="keepa-panel-header">
              <div>
                <p className="section-title">Balkendiagramm</p>
                <h2>Nutzung nach Quelle / Modul</h2>
              </div>
            </div>
            <MiniBarChart data={usageBreakdown} valueKey="estimatedUsage" color="linear-gradient(180deg, #38bdf8, #0ea5e9)" valueFormatter={formatUsage} />
          </section>
        </div>

        <div className="keepa-analytics-grid">
          <section className="card keepa-panel">
            <div className="keepa-panel-header">
              <div>
                <p className="section-title">Treffer pro Tag</p>
                <h2>Gespeicherte Deals im Verlauf</h2>
              </div>
            </div>
            <MiniBarChart data={usageHistory?.series || []} valueKey="hitCount" color="linear-gradient(180deg, #f59e0b, #d97706)" valueFormatter={(value) => `${value}`} />
          </section>

          <section className="card keepa-panel">
            <div className="keepa-panel-header">
              <div>
                <p className="section-title">Verteilung</p>
                <h2>Doughnut nach Quelle</h2>
              </div>
            </div>
            <MiniDonutChart data={usageBreakdown} valueKey="estimatedUsage" valueFormatter={formatUsage} />
          </section>
        </div>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Request Tracking</p>
              <h2>Letzte echte Keepa-Requests</h2>
            </div>
          </div>

          <div className="keepa-table-wrap">
            <table className="keepa-table">
              <thead>
                <tr>
                  <th>Start</th>
                  <th>Ende</th>
                  <th>Mode</th>
                  <th>Schublade</th>
                  <th>Tokens vorher</th>
                  <th>Tokens nachher</th>
                  <th>Verbrauch</th>
                  <th>Treffer</th>
                  <th>Dauer</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {latestKeepaRequests.map((item) => (
                  <tr key={`request-${item.id}`}>
                    <td>{formatDateTime(item.timestampStart)}</td>
                    <td>{formatDateTime(item.timestampEnd)}</td>
                    <td>{formatKeepaModeLabel(item.mode)}</td>
                    <td>{item.drawerKey || '-'}</td>
                    <td>{item.tokensBefore ?? '-'}</td>
                    <td>{item.tokensAfter ?? '-'}</td>
                    <td>{formatUsage(item.tokensUsed)}</td>
                    <td>{item.resultCount}</td>
                    <td>{formatDuration(item.durationMs)}</td>
                    <td>
                      <span className={`status-chip ${item.requestStatus === 'error' ? 'danger' : item.requestStatus === 'warning' ? 'warning' : 'success'}`}>
                        {item.requestStatus}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {!latestKeepaRequests.length && <p className="text-muted">Noch keine echten Keepa-Requests im Tracking vorhanden.</p>}
        </section>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Fehler / Warnungen</p>
              <h2>Auffaellige Keepa-Laeufe</h2>
            </div>
          </div>
          <div className="keepa-list">
            {recentIssues.map((item) => (
              <div key={item.id} className="keepa-list-item static">
                <div>
                  <strong>{item.actionLabel}</strong>
                  <p className="text-muted">
                    {item.moduleLabel} - {formatDateTime(item.createdAt)} - {item.errorMessage || 'Warnung ohne Fehltext'}
                  </p>
                </div>
                <span className={`status-chip ${item.requestStatus === 'error' ? 'danger' : 'warning'}`}>{item.requestStatus}</span>
              </div>
            ))}
            {!recentIssues.length && <p className="text-muted">Aktuell keine Fehler oder Warnungen im Usage-Tracking.</p>}
          </div>
        </section>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Protokoll</p>
              <h2>Letzte Requests und Aktionen</h2>
            </div>
          </div>

          <div className="keepa-table-wrap">
            <table className="keepa-table">
              <thead>
                <tr>
                  <th>Zeitpunkt</th>
                  <th>Aktion</th>
                  <th>Modul</th>
                  <th>Mode</th>
                  <th>Schublade</th>
                  <th>Filter</th>
                  <th>Treffer</th>
                  <th>Dauer</th>
                  <th>Verbrauch</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {(usageLogs.items || []).map((item) => (
                  <tr key={item.id}>
                    <td>{formatDateTime(item.createdAt)}</td>
                    <td>{item.actionLabel}</td>
                    <td>{item.moduleLabel}</td>
                    <td>{formatKeepaModeLabel(item.mode)}</td>
                    <td>{item.drawerKey || '-'}</td>
                    <td>
                      {item.filters ? (
                        <span className="keepa-log-filters">
                          {Object.entries(item.filters)
                            .slice(0, 4)
                            .map(([key, value]) => `${key}: ${Array.isArray(value) ? value.join(', ') : value}`)
                            .join(' - ')}
                        </span>
                      ) : (
                        '-'
                      )}
                    </td>
                    <td>{item.resultCount}</td>
                    <td>{formatDuration(item.durationMs)}</td>
                    <td>{formatUsage(item.tokensUsed || item.estimatedUsage)}</td>
                    <td>
                      <span className={`status-chip ${item.requestStatus === 'error' ? 'danger' : item.requestStatus === 'warning' ? 'warning' : 'success'}`}>
                        {item.requestStatus}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {!usageLogs.items?.length && <p className="text-muted">Noch keine Usage-Logs fuer diesen Filter vorhanden.</p>}
        </section>
      </div>
    );
  }

  function renderFakeDropAnalysisTab() {
    const kpis = fakeDropSummary?.kpis || {};

    return (
      <div className="keepa-section-stack">
        <div className="responsive-grid">
          <section className="card keepa-metric-card">
            <p className="section-title">echte Deals</p>
            <h2>{kpis.echterDealCount || 0}</h2>
            <p className="text-muted">Stabile Verlaeufe mit niedrigem Fake-Drop-Risiko.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">verdaechtige Deals</p>
            <h2>{kpis.suspiciousCount || 0}</h2>
            <p className="text-muted">Auffaellige Verlaeufe fuer menschliche Pruefung.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Fake-Drop-Verdacht</p>
            <h2>{kpis.fakeDropCount || 0}</h2>
            <p className="text-muted">Hoher Manipulationsverdacht durch Peaks, Rebounds oder Referenz-Tricks.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Amazon stabil</p>
            <h2>{kpis.amazonStableCount || 0}</h2>
            <p className="text-muted">Amazon-Angebote mit sauberem Downtrend und hoher Confidence.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">offene Reviews</p>
            <h2>{kpis.openReviewCount || 0}</h2>
            <p className="text-muted">Noch unbestaetigte Faelle in der Review Queue.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">unlabeled</p>
            <h2>{kpis.unlabeledCount || 0}</h2>
            <p className="text-muted">Faelle ohne menschliches Feedback und ohne Beispiel-Lernwert.</p>
          </section>
        </div>

        <section className="card keepa-banner">
          <div>
            <p className="section-title">Transparente Heuristik</p>
            <h2 className="page-title">Keine Black-Box, sondern nachvollziehbare Regeln</h2>
            <p className="page-subtitle">
              Jeder Score wird aus Stabilitaet, Manipulationsmustern, Seller-Typ, Amazon-Confidence und sichtbarem Trainings-Feedback berechnet und als Begruendung gespeichert.
            </p>
          </div>
          {isAdmin && (
            <button className="secondary" onClick={() => void handleRecalculateFakeDrop()} disabled={recalculatingFakeDrop}>
              {recalculatingFakeDrop ? 'Berechnet neu...' : 'Analyse neu berechnen'}
            </button>
          )}
        </section>

        <div className="keepa-analytics-grid">
          <section className="card keepa-panel">
            <div className="keepa-panel-header">
              <div>
                <p className="section-title">Anteil verdaechtig / sauber</p>
                <h2>Fake-Drop-Verlauf ueber Zeit</h2>
              </div>
            </div>
            <MiniLineChart data={fakeDropHistory?.series || []} valueKey="suspiciousCount" color="#f97316" />
          </section>

          <section className="card keepa-panel">
            <div className="keepa-panel-header">
              <div>
                <p className="section-title">Verteilung</p>
                <h2>Klassifikation aller gescorten Treffer</h2>
              </div>
            </div>
            <MiniDonutChart data={fakeDropDistribution} valueKey="count" valueFormatter={(value) => `${value}`} />
          </section>
        </div>

        <div className="keepa-analytics-grid">
          <section className="card keepa-panel">
            <div className="keepa-panel-header">
              <div>
                <p className="section-title">Muster</p>
                <h2>Zuletzt erkannte Fake-Muster</h2>
              </div>
            </div>
            <MiniBarChart
              data={(fakeDropHistory?.patternBreakdown || fakeDropSummary?.patternBreakdown || []).map((item) => ({
                label: item.label,
                count: item.count
              }))}
              valueKey="count"
              color="linear-gradient(180deg, #ef4444, #dc2626)"
              valueFormatter={(value) => `${value}`}
            />
          </section>

          <section className="card keepa-panel">
            <div className="keepa-panel-header">
              <div>
                <p className="section-title">Seller-Typen</p>
                <h2>Scoring nach Amazon, FBA und FBM</h2>
              </div>
            </div>
            <MiniBarChart
              data={(fakeDropHistory?.sellerBreakdown || []).map((item) => ({
                label: item.label,
                count: item.count
              }))}
              valueKey="count"
              color="linear-gradient(180deg, #38bdf8, #0284c7)"
              valueFormatter={(value) => `${value}`}
            />
          </section>
        </div>

        <div className="keepa-split-panels">
          <section className="card keepa-panel">
            <div className="keepa-panel-header">
              <div>
                <p className="section-title">Letzte Bewertungen</p>
                <h2>Human-in-the-loop Feedback</h2>
              </div>
            </div>
            <div className="keepa-list">
              {(fakeDropSummary?.recentReviews || []).map((item) => (
                <div key={item.id} className="keepa-list-item static">
                  <div>
                    <strong>{item.title || item.asin}</strong>
                    <p className="text-muted">
                      {item.labelLabel} - {item.classificationLabel} - {formatDateTime(item.createdAt)}
                    </p>
                  </div>
                  <span className={`status-chip ${getFakeDropChip(item.classification)}`}>{item.classificationLabel}</span>
                </div>
              ))}
              {!fakeDropSummary?.recentReviews?.length && <p className="text-muted">Noch keine Bewertungen in der Lern-Datenbank vorhanden.</p>}
            </div>
          </section>

          <section className="card keepa-panel">
            <div className="keepa-panel-header">
              <div>
                <p className="section-title">Trainings-Feedback</p>
                <h2>Abgeleitete Risiko-Anpassungen</h2>
              </div>
            </div>
            <div className="keepa-list">
              {(fakeDropSummary?.feedback || []).map((item) => (
                <div key={item.sellerType} className="keepa-list-item static">
                  <div>
                    <strong>{item.sellerType}</strong>
                    <p className="text-muted">
                      {item.total || 0} gelabelte Beispiele - positive {item.positiveCount || 0}, negative {item.negativeCount || 0}
                    </p>
                  </div>
                  <span className={`status-chip ${item.riskAdjustment > 0 ? 'warning' : item.riskAdjustment < 0 ? 'success' : 'info'}`}>
                    {item.riskAdjustment > 0 ? '+' : ''}
                    {item.riskAdjustment || 0}
                  </span>
                </div>
              ))}
              {!fakeDropSummary?.feedback?.length && <p className="text-muted">Noch nicht genug Feedback fuer adaptive Risiko-Hinweise vorhanden.</p>}
            </div>
          </section>
        </div>
      </div>
    );
  }

  function renderReviewQueueTab() {
    return (
      <div className="keepa-section-stack">
        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Review Queue</p>
              <h2>Verdaechtige oder interessante Deals manuell bewerten</h2>
            </div>
            <button className="primary" onClick={() => void handleApplyReviewFilters(1)}>
              Filter anwenden
            </button>
          </div>

          <div className="keepa-form-grid">
            <label>
              <span className="section-title">Seller-Typ</span>
              <select value={reviewFilters.sellerType} onChange={(event) => setReviewFilters((prev) => ({ ...prev, sellerType: event.target.value }))}>
                {sellerTypeOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span className="section-title">Analysefilter</span>
              <select value={reviewFilters.classification} onChange={(event) => setReviewFilters((prev) => ({ ...prev, classification: event.target.value }))}>
                {fakeDropFilterOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label className="checkbox-card">
              <span>Nur unlabeled</span>
              <input
                type="checkbox"
                checked={reviewFilters.onlyUnlabeled}
                onChange={(event) => setReviewFilters((prev) => ({ ...prev, onlyUnlabeled: event.target.checked }))}
              />
            </label>
          </div>
        </section>

        <div className="keepa-list">
          {reviewQueue.items.map((item) => {
            const draft = reviewDrafts[item.id] || {};

            return (
              <article key={item.id} className="keepa-review-card">
                <div className="keepa-review-header">
                  <div className="keepa-result-top">
                    <img src={item.imageUrl || FALLBACK_IMAGE} alt={item.title} />
                    <div>
                      <strong>{item.title}</strong>
                      <p className="text-muted">
                        {item.asin} - {item.sellerType} - {item.sourceLabel || 'Keepa'} - {item.categoryName || '-'}
                      </p>
                    </div>
                  </div>
                  <div className="keepa-card-tags">
                    <span className={`status-chip ${getFakeDropChip(item.fakeDrop?.classification)}`}>
                      {item.fakeDrop?.classificationLabel || 'MANUELLE PRUEFUNG'}
                    </span>
                    {item.lastLabel && <span className="status-chip info">Letztes Label: {item.lastLabel.labelLabel}</span>}
                  </div>
                </div>

                <div className="keepa-card-metrics three">
                  <span>
                    <strong>Preis</strong>
                    {formatCurrency(item.currentPrice)}
                  </span>
                  <span>
                    <strong>Rabatt</strong>
                    {formatPercent(item.keepaDiscount)}
                  </span>
                  <span>
                    <strong>Deal Score</strong>
                    {item.dealScore ?? '-'}
                  </span>
                  <span>
                    <strong>Fake-Drop Risk</strong>
                    {item.fakeDrop?.fakeDropRisk ?? '-'}
                  </span>
                  <span>
                    <strong>Stability</strong>
                    {item.fakeDrop?.stabilityScore ?? '-'}
                  </span>
                  <span>
                    <strong>Amazon Confidence</strong>
                    {item.fakeDrop?.amazonConfidence ?? '-'}
                  </span>
                </div>

                <div className="keepa-review-grid">
                  <div className="keepa-section-stack">
                    <MiniPriceHistory points={item.fakeDrop?.chartPoints || []} accent="#f97316" />
                    <div className="keepa-info-card">
                      <p className="section-title">Analyse-Begruendung</p>
                      <p className="text-muted">{item.fakeDrop?.analysisReason || 'Noch keine Begruendung gespeichert.'}</p>
                    </div>
                    {item.similarCaseSummary?.total > 0 && (
                      <div className="keepa-info-card">
                        <p className="section-title">Aehnliche Faelle</p>
                        <p className="text-muted">
                          {item.similarCaseSummary.total} aehnliche {item.sellerType}-Faelle.
                          {' '}Good: {item.similarCaseSummary.positiveCount || 0}, kritisch: {item.similarCaseSummary.negativeCount || 0}, Review: {item.similarCaseSummary.uncertainCount || 0}.
                        </p>
                        {!!item.similarCases?.length && (
                          <div className="keepa-list">
                            {item.similarCases.slice(0, 3).map((similar) => (
                              <div key={`${item.id}-${similar.reviewItemId}`} className="keepa-list-item static">
                                <div>
                                  <strong>{similar.title || similar.asin}</strong>
                                  <p className="text-muted">
                                    {similar.sourceLabel} - {similar.labelLabel} - {similar.similarityScore}% Match
                                  </p>
                                </div>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    )}
                    <div className="keepa-card-tags">
                      {(item.fakeDrop?.flags || []).slice(0, 4).map((flag) => (
                        <span key={flag.id} className="status-chip warning">
                          {flag.label}
                        </span>
                      ))}
                      {(item.fakeDrop?.positives || []).slice(0, 3).map((flag) => (
                        <span key={flag.id} className="status-chip success">
                          {flag.label}
                        </span>
                      ))}
                    </div>
                  </div>

                  <div className="keepa-section-stack">
                    <label>
                      <span className="section-title">Notiz</span>
                      <textarea
                        rows="4"
                        value={draft.note || ''}
                        onChange={(event) =>
                          setReviewDrafts((prev) => ({
                            ...prev,
                            [item.id]: {
                              ...prev[item.id],
                              note: event.target.value
                            }
                          }))
                        }
                        placeholder="Warum ist der Verlauf gut, schlecht oder unsicher?"
                      />
                    </label>

                    <div>
                      <p className="section-title">Tags</p>
                      <div className="keepa-tag-grid">
                        {reviewTagOptions.map((tag) => (
                          <button
                            key={tag.value}
                            type="button"
                            className={`keepa-tag-toggle ${(draft.tags || []).includes(tag.value) ? 'active' : ''}`}
                            onClick={() => toggleReviewTag(item.id, tag.value)}
                          >
                            {tag.label}
                          </button>
                        ))}
                      </div>
                    </div>

                    <label>
                      <span className="section-title">Beispiel-Bucket</span>
                      <select
                        value={draft.exampleBucket || 'unsicher'}
                        onChange={(event) =>
                          setReviewDrafts((prev) => ({
                            ...prev,
                            [item.id]: {
                              ...prev[item.id],
                              exampleBucket: event.target.value
                            }
                          }))
                        }
                      >
                        {exampleBucketOptions.slice(1).map((option) => (
                          <option key={option.value} value={option.value}>
                            {option.label}
                          </option>
                        ))}
                      </select>
                    </label>

                    <div className="keepa-inline-actions">
                      {reviewLabelOptions.map((option) => (
                        <button
                          key={option.value}
                          className={isNegativeReviewValue(option.value) ? 'secondary' : 'primary'}
                          onClick={() => void handleSubmitReview(item.id, option.value)}
                          disabled={reviewBusyId === item.id}
                        >
                          {option.label}
                        </button>
                      ))}
                      <button
                        className="secondary"
                        onClick={() => void handleSubmitReview(item.id, item.lastLabel?.label || item.fakeDrop?.currentLabel || 'eventuell_gut', true)}
                        disabled={reviewBusyId === item.id}
                      >
                        Als Beispiel speichern
                      </button>
                    </div>
                  </div>
                </div>
              </article>
            );
          })}
          {!reviewQueue.items.length && <p className="text-muted">Aktuell sind keine offenen Review-Faelle fuer diesen Filter vorhanden.</p>}
        </div>

        <div className="keepa-pagination">
          <button className="secondary" onClick={() => void handleApplyReviewFilters(reviewQueue.pagination.page - 1)} disabled={reviewQueue.pagination.page <= 1}>
            Vorherige Seite
          </button>
          <span className="text-muted">
            Seite {reviewQueue.pagination.page} von {reviewQueue.pagination.totalPages}
          </span>
          <button
            className="secondary"
            onClick={() => void handleApplyReviewFilters(reviewQueue.pagination.page + 1)}
            disabled={reviewQueue.pagination.page >= reviewQueue.pagination.totalPages}
          >
            Naechste Seite
          </button>
        </div>
      </div>
    );
  }

  function renderLearningLibraryTab() {
    return (
      <div className="keepa-section-stack">
        <div className="responsive-grid">
          <section className="card keepa-metric-card">
            <p className="section-title">Positive Beispiele</p>
            <h2>{exampleLibrary.counts?.positive || 0}</h2>
            <p className="text-muted">Bestaetigte Deals als saubere Referenzfaelle.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Negative Beispiele</p>
            <h2>{exampleLibrary.counts?.negative || 0}</h2>
            <p className="text-muted">Beispiele fuer Fake-Drops, Tricks und unbrauchbare Verlaeufe.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Unsichere Beispiele</p>
            <h2>{exampleLibrary.counts?.unsicher || 0}</h2>
            <p className="text-muted">Grenzfaelle fuer spaetere Regelanpassungen.</p>
          </section>
        </div>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Lern-Datenbank</p>
              <h2>Gespeicherte Beispiele, Label-Historie und aehnliche Faelle</h2>
            </div>
            <button className="primary" onClick={() => void handleApplyExampleFilters(1)}>
              Filter anwenden
            </button>
          </div>

          <div className="keepa-form-grid">
            <label>
              <span className="section-title">Bucket</span>
              <select value={exampleFilters.bucket} onChange={(event) => setExampleFilters((prev) => ({ ...prev, bucket: event.target.value }))}>
                {exampleBucketOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span className="section-title">Label</span>
              <select value={exampleFilters.label} onChange={(event) => setExampleFilters((prev) => ({ ...prev, label: event.target.value }))}>
                <option value="">Alle Labels</option>
                {reviewLabelOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span className="section-title">Seller-Typ</span>
              <select value={exampleFilters.sellerType} onChange={(event) => setExampleFilters((prev) => ({ ...prev, sellerType: event.target.value }))}>
                {sellerTypeOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span className="section-title">Suche</span>
              <input value={exampleFilters.search} onChange={(event) => setExampleFilters((prev) => ({ ...prev, search: event.target.value }))} placeholder="ASIN, Titel oder Kategorie" />
            </label>
          </div>
        </section>

        <div className="keepa-list">
          {exampleLibrary.items.map((item) => (
            <article key={item.id} className="keepa-example-card">
              <div className="keepa-review-header">
                <div className="keepa-result-top">
                  <img src={item.imageUrl || FALLBACK_IMAGE} alt={item.title} />
                  <div>
                    <strong>{item.title}</strong>
                    <p className="text-muted">
                      {item.asin} - {item.sellerType} - {item.sourceLabel || 'Keepa'} - {item.categoryName || '-'}
                    </p>
                  </div>
                </div>
                <div className="keepa-card-tags">
                  <span className="status-chip info">{item.bucketLabel}</span>
                  <span className={`status-chip ${getFakeDropChip(item.classification)}`}>{item.classificationLabel}</span>
                  <span className="status-chip success">{item.labelLabel}</span>
                </div>
              </div>

              <div className="keepa-review-grid">
                <div className="keepa-section-stack">
                  <MiniPriceHistory points={item.chartPoints || []} accent="#22c55e" />
                  <div className="keepa-card-metrics three">
                    <span>
                      <strong>Preis</strong>
                      {formatCurrency(item.currentPrice)}
                    </span>
                    <span>
                      <strong>Rabatt</strong>
                      {formatPercent(item.keepaDiscount)}
                    </span>
                    <span>
                      <strong>Fake-Drop Risk</strong>
                      {item.fakeDropRisk ?? '-'}
                    </span>
                  </div>
                </div>

                <div className="keepa-section-stack">
                  <div className="keepa-card-tags">
                    {(item.tags || []).map((tag) => (
                      <span key={tag} className="status-chip info">
                        {reviewTagOptions.find((option) => option.value === tag)?.label || tag}
                      </span>
                    ))}
                  </div>
                  <div className="keepa-info-card">
                    <p className="section-title">Notiz</p>
                    <p className="text-muted">{item.note || 'Keine Notiz zum Beispiel gespeichert.'}</p>
                  </div>
                  <div className="keepa-info-card">
                    <p className="section-title">Aehnliche Faelle</p>
                    <div className="keepa-list">
                      {(item.similarCases || []).map((similar) => (
                        <div key={similar.id} className="keepa-list-item static">
                          <div>
                            <strong>{similar.title || similar.asin}</strong>
                            <p className="text-muted">
                              {similar.sourceLabel || 'Keepa'} - {similar.labelLabel} - {similar.classificationLabel} - Risk {similar.fakeDropRisk}
                              {typeof similar.similarityScore === 'number' ? ` - ${similar.similarityScore}% Match` : ''}
                            </p>
                          </div>
                          <span className="status-chip info">{similar.categoryName || 'Vergleich'}</span>
                        </div>
                      ))}
                      {!item.similarCases?.length && <p className="text-muted">Keine aehnlichen gespeicherten Faelle gefunden.</p>}
                    </div>
                  </div>
                </div>
              </div>
            </article>
          ))}
          {!exampleLibrary.items.length && <p className="text-muted">Noch keine Beispiele in der Lern-Datenbank fuer diesen Filter gespeichert.</p>}
        </div>

        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Label-Historie</p>
              <h2>Zuletzt gespeicherte Bewertungen</h2>
            </div>
          </div>
          <div className="keepa-list">
            {(fakeDropSummary?.recentReviews || []).map((item) => (
              <div key={item.id} className="keepa-list-item static">
                <div>
                  <strong>{item.title || item.asin}</strong>
                  <p className="text-muted">
                    {item.labelLabel} - {item.classificationLabel} - {formatDateTime(item.createdAt)}
                  </p>
                </div>
                <div className="keepa-card-tags">
                  <span className={`status-chip ${getFakeDropChip(item.classification)}`}>{item.classificationLabel}</span>
                  <span className="status-chip info">{item.labelLabel}</span>
                </div>
              </div>
            ))}
            {!fakeDropSummary?.recentReviews?.length && <p className="text-muted">Noch keine Label-Historie vorhanden.</p>}
          </div>
        </section>

        <div className="keepa-pagination">
          <button className="secondary" onClick={() => void handleApplyExampleFilters(exampleLibrary.pagination.page - 1)} disabled={exampleLibrary.pagination.page <= 1}>
            Vorherige Seite
          </button>
          <span className="text-muted">
            Seite {exampleLibrary.pagination.page} von {exampleLibrary.pagination.totalPages}
          </span>
          <button
            className="secondary"
            onClick={() => void handleApplyExampleFilters(exampleLibrary.pagination.page + 1)}
            disabled={exampleLibrary.pagination.page >= exampleLibrary.pagination.totalPages}
          >
            Naechste Seite
          </button>
        </div>
      </div>
    );
  }

  function renderCurrentTab() {
    if (currentTab === '/keepa/manual-search') {
      return renderManualSearchTab();
    }

    if (currentTab === '/keepa/automatik') {
      return renderAutomatikTab();
    }

    if (currentTab === '/keepa/ergebnisse') {
      return renderResultsTab();
    }

    if (currentTab === '/keepa/benachrichtigungen') {
      return renderNotificationsTab();
    }

    if (currentTab === '/keepa/einstellungen') {
      return renderSettingsTab();
    }

    if (currentTab === '/keepa/verbrauch-logs') {
      return renderUsageTab();
    }

    if (currentTab === '/keepa/fake-drop-analyse') {
      return renderFakeDropAnalysisTab();
    }

    if (currentTab === '/keepa/review-queue') {
      return renderReviewQueueTab();
    }

    if (currentTab === '/keepa/lern-datenbank') {
      return renderLearningLibraryTab();
    }

    return renderOverviewTab();
  }

  return (
    <Layout>
      <div className="keepa-page">
        <section className="card keepa-hero">
          <div className="dashboard-hero-copy">
            <p className="section-title">Admin-Zentrale</p>
            <h1 className="page-title">Logik-Zentrale fuer Quellen, Regeln und Output</h1>
            <p className="page-subtitle">Admin sieht hier den Deal-Flow kompakt, Generator und Scrapper bleiben davon getrennt.</p>
          </div>
          <div className="dashboard-chip-row">
            <span className="status-chip info">{isAdmin ? 'Admin sichtbar' : 'versteckt'}</span>
            <span className={`status-chip ${statusData?.connection?.connected ? 'success' : 'warning'}`}>
              {statusData?.connection?.connected ? 'Keepa verbunden' : 'Keepa getrennt vorbereitet'}
            </span>
          </div>
        </section>

        <section className="card keepa-tab-card">
          <nav className="keepa-tabs">
            {keepaTabs.map((item) => (
              <NavLink
                key={item.path}
                to={buildLearningTabPath(navigationBasePath, item.path)}
                className={() => (currentTab === item.path ? 'keepa-tab active' : 'keepa-tab')}
              >
                {item.label}
              </NavLink>
            ))}
          </nav>
        </section>

        {!bootLoading && renderTopKpiBar()}

        {statusMessage && (
          <section className="card keepa-message-card">
            <p style={{ margin: 0 }}>{statusMessage}</p>
          </section>
        )}

        {bootLoading ? <section className="card keepa-message-card">Lern-Logik-Daten werden geladen...</section> : renderCurrentTab()}
      </div>
    </Layout>
  );
}

export default KeepaPage;
