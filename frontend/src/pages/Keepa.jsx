import { useEffect, useMemo, useRef, useState } from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';
import './Keepa.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';
const FALLBACK_IMAGE =
  'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="72" height="72"><rect width="72" height="72" rx="18" fill="%230f172a"/><text x="50%" y="52%" dominant-baseline="middle" text-anchor="middle" fill="%2394a3b8" font-family="Arial" font-size="14">AM</text></svg>';

const keepaTabs = [
  { label: 'Uebersicht', path: '/keepa' },
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

const sellerTypeOptions = [
  { value: 'ALL', label: 'Alle' },
  { value: 'AMAZON', label: 'Amazon' },
  { value: 'FBA', label: 'FBA' },
  { value: 'FBM', label: 'FBM' }
];

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
  { value: 'ja', label: 'Ja' },
  { value: 'nein', label: 'Nein' },
  { value: 'eventuell_gut', label: 'Eventuell gut' },
  { value: 'ueberspringen', label: 'Ueberspringen' }
];

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

function buildManualFilters(settings) {
  return {
    page: 1,
    limit: settings?.defaultPageSize || 24,
    minDiscount: settings?.defaultDiscount || 40,
    sellerType: settings?.defaultSellerType || 'ALL',
    categories: settings?.defaultCategories || [],
    minPrice: settings?.defaultMinPrice ?? '',
    maxPrice: settings?.defaultMaxPrice ?? '',
    onlyPrime: false,
    onlyInStock: true,
    onlyGoodRating: false
  };
}

function buildRuleForm(settings) {
  return {
    id: 0,
    name: '',
    minDiscount: settings?.defaultDiscount || 40,
    sellerType: settings?.defaultSellerType || 'ALL',
    categories: settings?.defaultCategories || [],
    minPrice: settings?.defaultMinPrice ?? '',
    maxPrice: settings?.defaultMaxPrice ?? '',
    minDealScore: 70,
    intervalMinutes: settings?.defaultIntervalMinutes || 60,
    onlyPrime: false,
    onlyInStock: true,
    onlyGoodRating: false,
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

  const currentTab = useMemo(() => {
    const match = keepaTabs.find((item) => item.path === location.pathname);
    return match?.path || '/keepa';
  }, [location.pathname]);

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
  const [manualFilters, setManualFilters] = useState(buildManualFilters());
  const [manualResponse, setManualResponse] = useState({
    items: [],
    pagination: { page: 1, limit: 24, hasMore: false, rawResultCount: 0 },
    usage: null
  });
  const [ruleForm, setRuleForm] = useState(buildRuleForm());
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
      setUsageSummary(usageSummaryResponse);
      setRules(rulesResponse.items || []);
      setAlerts(alertsResponse.items || []);
      setResults(resultsResponse);
      setUsageHistory(historyResponse);
      setUsageLogs(logsResponse);
      setFakeDropSummary(fakeDropSummaryResponse);
      setFakeDropHistory(fakeDropHistoryResponse);

      if (!initializedRef.current && statusResponse?.settings) {
        setManualFilters(buildManualFilters(statusResponse.settings));
        setSettingsForm(buildSettingsForm(statusResponse.settings));
        initializedRef.current = true;
      }

      if (!ruleInitializedRef.current && statusResponse?.settings) {
        setRuleForm(buildRuleForm(statusResponse.settings));
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
      setStatusMessage(error instanceof Error ? error.message : 'Keepa-Daten konnten nicht geladen werden.');
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

  async function handleManualSearch(page = 1) {
    setManualLoading(true);
    setStatusMessage('');

    try {
      const data = await apiFetch('/api/keepa/manual-search', {
        method: 'POST',
        body: JSON.stringify({
          ...manualFilters,
          page
        })
      });

      setManualFilters((prev) => ({ ...prev, page }));
      setManualResponse(data);
      await loadDashboard(resultsFilters, usageFilters, false);
    } catch (error) {
      setStatusMessage(error instanceof Error ? error.message : 'Manuelle Suche fehlgeschlagen.');
    } finally {
      setManualLoading(false);
    }
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

      setSettingsForm(buildSettingsForm(data));
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

      setStatusMessage(`Review gespeichert: ${reviewLabelOptions.find((item) => item.value === label)?.label || label}.`);
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

  function renderTopKpiBar() {
    const overview = statusData?.overview;
    const connection = statusData?.connection;
    const kpis = usageSummary?.kpis || {};

    return (
      <section className="card keepa-kpi-strip">
        <article className="keepa-kpi-pill">
          <span>Status</span>
          <strong>{overview?.apiStatus || '-'}</strong>
          <small>{connection?.connected ? 'Backend-Only verbunden' : 'Noch keine aktive Verbindung'}</small>
        </article>
        <article className="keepa-kpi-pill">
          <span>Letzte Abfrage</span>
          <strong>{formatDateTime(kpis.lastRequestAt)}</strong>
          <small>Zuletzt protokollierter Keepa-Request</small>
        </article>
        <article className="keepa-kpi-pill">
          <span>Requests heute</span>
          <strong>{kpis.requestsToday || 0}</strong>
          <small>Offiziell erfasste Keepa-Requests</small>
        </article>
        <article className="keepa-kpi-pill">
          <span>Requests Monat</span>
          <strong>{kpis.requestsMonth || 0}</strong>
          <small>Seit Monatsbeginn</small>
        </article>
        <article className="keepa-kpi-pill">
          <span>Verbrauch heute</span>
          <strong>{formatUsage(kpis.estimatedUsageToday)}</strong>
          <small>Intern geschaetzt</small>
        </article>
        <article className="keepa-kpi-pill">
          <span>Verbrauch Monat</span>
          <strong>{formatUsage(kpis.estimatedUsageMonth)}</strong>
          <small>Intern geschaetzt</small>
        </article>
        <article className="keepa-kpi-pill">
          <span>Treffer heute</span>
          <strong>{kpis.hitsToday || 0}</strong>
          <small>Manuelle Suche + Automatik</small>
        </article>
        <article className="keepa-kpi-pill">
          <span>Aktive Regeln</span>
          <strong>{kpis.activeRulesCount || 0}</strong>
          <small>Automatik-Regeln im Backend</small>
        </article>
      </section>
    );
  }

  function renderOverviewTab() {
    const overview = statusData?.overview;
    const connection = statusData?.connection;

    return (
      <div className="keepa-section-stack">
        <div className="responsive-grid">
          <section className="card keepa-metric-card">
            <p className="section-title">Keepa Nutzung heute</p>
            <h2>{formatUsage(usageSummary?.today?.estimatedUsage)}</h2>
            <p className="text-muted">
              {usageSummary?.today?.requestCount || 0} Requests, {usageSummary?.today?.hitCount || 0} Treffer, {usageSummary?.usageModeLabel || 'intern geschaetzt'}.
            </p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Keepa Nutzung Monat</p>
            <h2>{formatUsage(usageSummary?.month?.estimatedUsage)}</h2>
            <p className="text-muted">Monatsprojektion {formatUsage(usageSummary?.kpis?.monthlyProjection)}.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Letzte manuelle Suche</p>
            <h2>{formatDateTime(usageSummary?.lastManualSearch?.createdAt)}</h2>
            <p className="text-muted">
              {usageSummary?.lastManualSearch?.resultCount || 0} Treffer, {formatUsage(usageSummary?.lastManualSearch?.estimatedUsage)}.
            </p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Letzter Automatik-Lauf</p>
            <h2>{formatDateTime(usageSummary?.lastAutomationRun?.createdAt)}</h2>
            <p className="text-muted">
              {usageSummary?.lastAutomationRun?.resultCount || 0} Treffer, {formatUsage(usageSummary?.lastAutomationRun?.estimatedUsage)}.
            </p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">API Status</p>
            <h2>{overview?.apiStatus || '-'}</h2>
            <p className="text-muted">
              {connection?.connected ? `Tokens verfuegbar: ${connection.tokensLeft}` : 'Keepa aktuell nicht verbunden.'}
            </p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Letzte Treffer</p>
            <h2>{overview?.latestHits?.length || 0}</h2>
            <p className="text-muted">Persistierte Treffer aus Suche und Automatik.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Offene Reviews</p>
            <h2>{fakeDropSummary?.kpis?.openReviewCount || overview?.fakeDropSummary?.kpis?.openReviewCount || 0}</h2>
            <p className="text-muted">Verdaechtige oder unsichere Verlaeufe fuer die Review Queue.</p>
          </section>
        </div>

        <section className="card keepa-banner">
          <div>
            <p className="section-title">Verbindungs-Hinweis</p>
            <h2 className="page-title">{connection?.connected ? 'Keepa ist aktiv verbunden' : 'Keepa ist noch nicht voll aktiv'}</h2>
            <p className="page-subtitle">
              API-Key bleibt im Backend, Verbrauchswerte werden intern protokolliert und offizielle Token-Werte nur genutzt, wenn Keepa sie sauber zurueckmeldet.
            </p>
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
                <p className="section-title">Letzte Treffer</p>
                <h2>Aktuelle Keepa-Ergebnisse</h2>
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
                <p className="section-title">Letzte Alerts</p>
                <h2>Benachrichtigungen</h2>
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

    return (
      <div className="keepa-section-stack">
        <section className="card keepa-panel">
          <div className="keepa-panel-header">
            <div>
              <p className="section-title">Manuelle Suche</p>
              <h2>Deals kontrolliert abrufen</h2>
            </div>
            <button className="primary" onClick={() => void handleManualSearch(1)} disabled={manualLoading}>
              {manualLoading ? 'Laedt...' : 'Deals aktualisieren'}
            </button>
          </div>

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
          </div>

          <div className="keepa-form-grid">
            <label>
              <span className="section-title">Mindest-Rabatt</span>
              <div className="keepa-range-row">
                <input
                  type="range"
                  min="30"
                  max="90"
                  value={manualFilters.minDiscount}
                  onChange={(event) => setManualFilters((prev) => ({ ...prev, minDiscount: Number(event.target.value) }))}
                />
                <strong>{manualFilters.minDiscount}%</strong>
              </div>
            </label>

            <label>
              <span className="section-title">Verkaeufer-Typ</span>
              <select
                value={manualFilters.sellerType}
                onChange={(event) => setManualFilters((prev) => ({ ...prev, sellerType: event.target.value }))}
              >
                {sellerTypeOptions.map((option) => (
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
                onChange={(event) => setManualFilters((prev) => ({ ...prev, minPrice: event.target.value }))}
                placeholder="0"
              />
            </label>

            <label>
              <span className="section-title">Hoechstpreis</span>
              <input
                type="number"
                value={manualFilters.maxPrice}
                onChange={(event) => setManualFilters((prev) => ({ ...prev, maxPrice: event.target.value }))}
                placeholder="500"
              />
            </label>

            <label>
              <span className="section-title">Treffer pro Lauf</span>
              <select
                value={manualFilters.limit}
                onChange={(event) => setManualFilters((prev) => ({ ...prev, limit: Number(event.target.value) }))}
              >
                <option value="12">12</option>
                <option value="24">24</option>
                <option value="48">48</option>
              </select>
            </label>
          </div>

          <div className="keepa-flag-grid">
            <label className="checkbox-card">
              <span>Nur Prime</span>
              <input
                type="checkbox"
                checked={manualFilters.onlyPrime}
                onChange={(event) => setManualFilters((prev) => ({ ...prev, onlyPrime: event.target.checked }))}
              />
            </label>
            <label className="checkbox-card">
              <span>Nur lagernd</span>
              <input
                type="checkbox"
                checked={manualFilters.onlyInStock}
                onChange={(event) => setManualFilters((prev) => ({ ...prev, onlyInStock: event.target.checked }))}
              />
            </label>
            <label className="checkbox-card">
              <span>Nur gute Bewertung</span>
              <input
                type="checkbox"
                checked={manualFilters.onlyGoodRating}
                onChange={(event) => setManualFilters((prev) => ({ ...prev, onlyGoodRating: event.target.checked }))}
              />
            </label>
          </div>

          <div>
            <p className="section-title">Kategorien</p>
            {renderCategoryPicker(manualFilters.categories, (categoryId) => toggleCategory(setManualFilters, categoryId))}
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
    return (
      <div className="keepa-section-stack">
        <div className="responsive-grid">
          <section className="card keepa-metric-card">
            <p className="section-title">Scheduler</p>
            <h2>{settingsForm.schedulerEnabled ? 'Aktiv' : 'Pausiert'}</h2>
            <p className="text-muted">Prueft aktive Regeln im Backend-Intervall und vermeidet Endlosschleifen.</p>
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
    return (
      <div className="keepa-section-stack">
        <div className="responsive-grid">
          <section className="card keepa-metric-card">
            <p className="section-title">Keepa Nutzung heute</p>
            <h2>{formatUsage(usageSummary?.today?.estimatedUsage)}</h2>
            <p className="text-muted">{usageSummary?.today?.requestCount || 0} Requests heute, Verbrauch klar als intern geschaetzt markiert.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Keepa Nutzung Monat</p>
            <h2>{formatUsage(usageSummary?.month?.estimatedUsage)}</h2>
            <p className="text-muted">Monatsnutzung bisher, als geschaetzter Verbrauch ausgewiesen.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Letzte manuelle Suche</p>
            <h2>{formatDateTime(usageSummary?.lastManualSearch?.createdAt)}</h2>
            <p className="text-muted">
              {usageSummary?.lastManualSearch?.resultCount || 0} Treffer - {formatDuration(usageSummary?.lastManualSearch?.durationMs)}
            </p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Letzter Automatik-Lauf</p>
            <h2>{formatDateTime(usageSummary?.lastAutomationRun?.createdAt)}</h2>
            <p className="text-muted">
              {usageSummary?.lastAutomationRun?.resultCount || 0} Treffer - {formatDuration(usageSummary?.lastAutomationRun?.durationMs)}
            </p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Gefundene Deals heute</p>
            <h2>{usageSummary?.dealsToday || 0}</h2>
            <p className="text-muted">Manuelle Suche und Automatik kombiniert.</p>
          </section>
          <section className="card keepa-metric-card">
            <p className="section-title">Monatsprojektion</p>
            <h2>{formatUsage(usageSummary?.kpis?.monthlyProjection)}</h2>
            <p className="text-muted">{usageSummary?.usageModeLabel || 'intern geschaetzt'}.</p>
          </section>
        </div>

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
                <h2>Keepa-Nutzung ueber Zeit</h2>
              </div>
            </div>
            <MiniLineChart data={usageHistory?.series || []} valueKey="estimatedUsage" color="#10b981" />
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
                    <td>{formatUsage(item.estimatedUsage)}</td>
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
                        {item.asin} - {item.sellerType} - {item.categoryName || '-'}
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
                          className={option.value === 'nein' ? 'secondary' : 'primary'}
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
                      {item.asin} - {item.sellerType} - {item.categoryName || '-'}
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
                              {similar.labelLabel} - {similar.classificationLabel} - Risk {similar.fakeDropRisk}
                            </p>
                          </div>
                          <span className="status-chip info">{similar.bucketLabel}</span>
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
          <div>
            <p className="section-title">Keepa</p>
            <h1 className="page-title">Deal Monitoring mit sicherer Backend-Architektur</h1>
            <p className="page-subtitle">
              Keepa laeuft nur ueber eigene Backend-Endpunkte, API-Keys bleiben im Backend und das Verbrauchs-Tracking wird sichtbar aus Backend-Logs und geschaetzten Usage-Werten aufgebaut.
            </p>
          </div>
          <span className={`status-chip ${statusData?.connection?.connected ? 'success' : 'warning'}`}>
            {statusData?.connection?.connected ? 'Keepa verbunden' : 'Keepa nicht verbunden'}
          </span>
        </section>

        <section className="card keepa-tab-card">
          <nav className="keepa-tabs">
            {keepaTabs.map((item) => (
              <NavLink key={item.path} to={item.path} className={({ isActive }) => (isActive ? 'keepa-tab active' : 'keepa-tab')}>
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

        {bootLoading ? <section className="card keepa-message-card">Keepa-Daten werden geladen...</section> : renderCurrentTab()}
      </div>
    </Layout>
  );
}

export default KeepaPage;
