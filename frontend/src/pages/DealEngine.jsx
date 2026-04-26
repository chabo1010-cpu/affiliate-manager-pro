import { useEffect, useMemo, useState } from 'react';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';
import './DealEngine.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';

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

function DealEnginePage() {
  const { user } = useAuth();
  const [dashboard, setDashboard] = useState(null);
  const [settings, setSettings] = useState(null);
  const [samplePayload, setSamplePayload] = useState(null);
  const [form, setForm] = useState(() => buildFormFromSample({}));
  const [result, setResult] = useState(null);
  const [status, setStatus] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [analyzing, setAnalyzing] = useState(false);

  useEffect(() => {
    let cancelled = false;

    async function apiFetch(path, options = {}) {
      const response = await fetch(`${API_BASE_URL}${path}`, {
        headers: {
          'Content-Type': 'application/json',
          'X-User-Role': user?.role || ''
        },
        ...options
      });
      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(data?.error || `Request fehlgeschlagen (${response.status}).`);
      }

      return data;
    }

    async function loadPage() {
      try {
        setLoading(true);
        setStatus('');

        const [dashboardData, settingsData, sampleData] = await Promise.all([
          apiFetch('/api/deal-engine/dashboard'),
          apiFetch('/api/deal-engine/settings'),
          apiFetch('/api/deal-engine/sample')
        ]);

        if (cancelled) {
          return;
        }

        setDashboard(dashboardData);
        setSettings(settingsData.item);
        setSamplePayload(sampleData.item);
        setForm((current) => {
          const isStillDefault = !current.title && !current.amazonUrl && !current.amazonPrice;

          return isStillDefault ? buildFormFromSample(sampleData.item) : current;
        });
      } catch (error) {
        if (!cancelled) {
          setStatus(error instanceof Error ? error.message : 'Deal Engine konnte nicht geladen werden.');
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadPage();

    return () => {
      cancelled = true;
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

  async function refreshDashboard() {
    const response = await fetch(`${API_BASE_URL}/api/deal-engine/dashboard`, {
      headers: {
        'Content-Type': 'application/json',
        'X-User-Role': user?.role || ''
      }
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(data?.error || 'Dashboard konnte nicht aktualisiert werden.');
    }

    setDashboard(data);
  }

  async function handleSaveSettings() {
    if (!settings) {
      return;
    }

    try {
      setSaving(true);
      setStatus('');
      const response = await fetch(`${API_BASE_URL}/api/deal-engine/settings`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          'X-User-Role': user?.role || ''
        },
        body: JSON.stringify(settings)
      });
      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(data?.error || 'Regler konnten nicht gespeichert werden.');
      }

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
      const response = await fetch(`${API_BASE_URL}/api/deal-engine/analyze`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-User-Role': user?.role || ''
        },
        body: JSON.stringify(payload)
      });
      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(data?.error || 'Analyse fehlgeschlagen.');
      }

      setResult(data.item);
      await refreshDashboard();
      setStatus(`Analyse abgeschlossen: ${data.item?.decision || 'unbekannt'}.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Analyse fehlgeschlagen.');
    } finally {
      setAnalyzing(false);
    }
  }

  const currentResult = result || dashboard?.runs?.[0] || null;
  const overviewCards = useMemo(() => {
    const metrics = dashboard?.metrics || {};
    const outputSnapshot = dashboard?.outputs?.snapshot || {};
    const systemStatus = dashboard?.systemStatus || {};

    return [
      {
        title: 'Systemstatus',
        value: systemStatus.label || '-',
        detail: systemStatus.detail || 'Keine Details',
        tone: systemStatus.tone || 'info'
      },
      {
        title: 'Internet entscheidet',
        value: `${toNumber(metrics.marketRuns)} Markt-Runs`,
        detail: 'Marktpreis bleibt die Hauptentscheidung, wenn die Seller-Regeln Marktvergleich erlauben.',
        tone: 'success'
      },
      {
        title: 'Keepa Fallback',
        value: `${toNumber(metrics.keepaFallbackRuns)} Fallbacks`,
        detail: 'Keepa wird nur ohne brauchbaren Marktpreis genutzt.',
        tone: toNumber(metrics.keepaFallbackRuns) > 0 ? 'info' : 'warning'
      },
      {
        title: 'Outputs',
        value: `${outputSnapshot.telegram?.configured ? 'Telegram bereit' : 'Telegram pruefen'} / ${
          outputSnapshot.whatsapp?.configured ? 'WhatsApp bereit' : 'WhatsApp pruefen'
        }`,
        detail: `${toNumber(dashboard?.outputs?.openQueueCount)} offene Queue-Eintraege fuer die Deal Engine`,
        tone: toNumber(dashboard?.outputs?.openQueueCount) > 0 ? 'warning' : 'success'
      }
    ];
  }, [dashboard]);

  return (
    <Layout>
      <div className="engine-page">
        <section className="card engine-hero">
          <div className="engine-hero-grid">
            <div className="engine-hero-copy">
              <p className="section-title">Deal Engine</p>
              <h1 className="page-title">Internet zuerst, Keepa nur Fallback, KI nur im Unsicherheitsfall</h1>
              <p className="page-subtitle">
                Dashboard, Quellen, Regler und Output sind direkt an die bestehende App gekoppelt. Marktvergleich und
                KI koennen jetzt seller-genau fuer Amazon Direct, FBA, FBM und Unknown gesteuert werden.
              </p>
            </div>
            <div className="engine-hero-side">
              <span className="badge">Umsetzbar im vorhandenen Stack</span>
              <span className={`status-chip ${dashboard?.systemStatus?.tone || 'info'}`}>
                {dashboard?.systemStatus?.label || 'loading'}
              </span>
              <span className="badge">System laeuft komplett ohne KI</span>
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
            <section className="card engine-panel">
              <div className="engine-panel-header">
                <div>
                  <p className="section-title">Dashboard</p>
                  <h2 className="page-title">Live Uebersicht</h2>
                </div>
                <span className="engine-header-note">{dashboard?.feasibility?.detail || '-'}</span>
              </div>
              <div className="engine-card-grid">
                {overviewCards.map((card) => (
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
                {(dashboard?.liveFlow || []).map((step) => (
                  <article key={step.id} className={`engine-flow-card engine-tone-${step.tone}`}>
                    <div className="engine-card-head">
                      <strong>{step.label}</strong>
                      <span className={`status-chip ${step.tone}`}>{step.tone}</span>
                    </div>
                    <h3>{step.title}</h3>
                    <p>{step.detail}</p>
                  </article>
                ))}
              </div>
            </section>

            <section className="engine-mandatory-grid">
              <section className="card engine-panel">
                <div className="engine-panel-header">
                  <div>
                    <p className="section-title">Quellen</p>
                    <h2 className="page-title">Aktive Inputs</h2>
                  </div>
                  <span className="engine-header-note">
                    {dashboard?.sources?.activeCount || 0} aktiv | {dashboard?.sources?.telegramCount || 0} Telegram |{' '}
                    {dashboard?.sources?.whatsappCount || 0} WhatsApp
                  </span>
                </div>
                <div className="engine-list">
                  {(dashboard?.sources?.items || []).length ? (
                    dashboard.sources.items.map((item) => (
                      <article key={item.id} className="engine-list-item">
                        <div className="engine-card-head">
                          <strong>{item.name}</strong>
                          <span className={`status-chip ${getToneClass(item.platform)}`}>{item.platform}</span>
                        </div>
                        <p>
                          Typ {item.source_type || 'manual'} | Prioritaet {item.priority || 0} | Letzter Import{' '}
                          {formatDateTime(item.last_import_at)}
                        </p>
                      </article>
                    ))
                  ) : (
                    <p className="engine-empty">Keine aktiven Quellen vorhanden.</p>
                  )}
                </div>
              </section>

              <section className="card engine-panel">
                <div className="engine-panel-header">
                  <div>
                    <p className="section-title">Regler</p>
                    <h2 className="page-title">Steuerung</h2>
                  </div>
                  <span className="engine-header-note">
                    {user?.role === 'admin' ? 'Admin darf speichern' : 'Nur Lesemodus'}
                  </span>
                </div>

                {settings ? (
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
                    <div className="engine-settings-section engine-span-2">
                      <p className="section-title">Qualitaet</p>
                      <p className="engine-header-note">Seller-Gates fuer Marktvergleich, KI und unbekannte Verkaeufer.</p>
                    </div>
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
                      <span>Marktvergleich nur bei Verkauf & Versand durch Amazon</span>
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
                      <span>KI nur bei Verkauf & Versand durch Amazon</span>
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
                      <span>FBA Drittanbieter fuer Marktvergleich erlauben</span>
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
                      <span>FBA Drittanbieter fuer KI erlauben</span>
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

                <div className="engine-actions">
                  <button type="button" className="secondary" disabled={user?.role !== 'admin' || saving} onClick={handleSaveSettings}>
                    {saving ? 'Speichert...' : 'Regler speichern'}
                  </button>
                </div>
              </section>

              <section className="card engine-panel">
                <div className="engine-panel-header">
                  <div>
                    <p className="section-title">Output</p>
                    <h2 className="page-title">Telegram und WhatsApp</h2>
                  </div>
                  <span className="engine-header-note">
                    {dashboard?.outputs?.openQueueCount || 0} offene Deal-Engine-Queues
                  </span>
                </div>
                <div className="engine-card-grid">
                  <article className={`engine-card engine-tone-${dashboard?.outputs?.snapshot?.telegram?.configured ? 'success' : 'warning'}`}>
                    <div className="engine-card-head">
                      <p className="section-title">Telegram</p>
                      <span className={`status-chip ${dashboard?.outputs?.snapshot?.telegram?.configured ? 'success' : 'warning'}`}>
                        {dashboard?.outputs?.snapshot?.telegram?.configured ? 'ready' : 'check'}
                      </span>
                    </div>
                    <h3>{dashboard?.outputs?.snapshot?.telegram?.targets || 0} Ziele</h3>
                    <p>Engine {dashboard?.outputs?.snapshot?.telegram?.enabledByEngine ? 'aktiv' : 'deaktiviert'}</p>
                  </article>
                  <article className={`engine-card engine-tone-${dashboard?.outputs?.snapshot?.whatsapp?.configured ? 'success' : 'warning'}`}>
                    <div className="engine-card-head">
                      <p className="section-title">WhatsApp</p>
                      <span className={`status-chip ${dashboard?.outputs?.snapshot?.whatsapp?.configured ? 'success' : 'warning'}`}>
                        {dashboard?.outputs?.snapshot?.whatsapp?.configured ? 'ready' : 'check'}
                      </span>
                    </div>
                    <h3>{dashboard?.outputs?.snapshot?.whatsapp?.sender || 'kein Sender'}</h3>
                    <p>Retry Limit {dashboard?.outputs?.snapshot?.whatsapp?.retryLimit || 0}</p>
                  </article>
                </div>
                <div className="engine-list">
                  {(dashboard?.outputs?.latestQueues || []).length ? (
                    dashboard.outputs.latestQueues.map((queue) => (
                      <article key={queue.id} className="engine-list-item">
                        <div className="engine-card-head">
                          <strong>{queue.payload?.title || `Queue ${queue.id}`}</strong>
                          <span className={`status-chip ${getToneClass(queue.status)}`}>{queue.status}</span>
                        </div>
                        <p>
                          {queue.targets?.map((target) => `${target.channel_type}:${target.status}`).join(' | ') || 'Keine Targets'} |{' '}
                          {formatDateTime(queue.created_at)}
                        </p>
                      </article>
                    ))
                  ) : (
                    <p className="engine-empty">Noch keine Deal-Engine Outputs vorhanden.</p>
                  )}
                </div>
              </section>
            </section>

            <section className="card engine-panel">
              <div className="engine-panel-header">
                <div>
                  <p className="section-title">Analyse Engine</p>
                  <h2 className="page-title">Kompletter Deal-Durchlauf</h2>
                </div>
                <span className="engine-header-note">Amazon Direct kann Markt + KI nutzen, andere Seller fallen standardmaessig auf Keepa zurueck</span>
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
                  <input
                    type="checkbox"
                    checked={form.isBrandProduct}
                    onChange={(event) => updateForm('isBrandProduct', event.target.checked)}
                  />
                </label>
                <label className="engine-checkbox">
                  <span>No-Name</span>
                  <input type="checkbox" checked={form.isNoName} onChange={(event) => updateForm('isNoName', event.target.checked)} />
                </label>
                <label className="engine-checkbox">
                  <span>China Produkt</span>
                  <input
                    type="checkbox"
                    checked={form.isChinaProduct}
                    onChange={(event) => updateForm('isChinaProduct', event.target.checked)}
                  />
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
                <button
                  type="button"
                  className="secondary"
                  onClick={() => setForm(buildFormFromSample(samplePayload || {}))}
                >
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
                  <span className="engine-header-note">
                    Letztes Ergebnis {currentResult ? formatDateTime(currentResult.createdAt) : '-'}
                  </span>
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
                          <p className="section-title">Seller</p>
                          <span className="status-chip info">{currentResult.analysis?.seller?.sellerClass || currentResult.sellerArea || 'UNKNOWN'}</span>
                        </div>
                        <h3>{currentResult.analysis?.seller?.sellerType || currentResult.sellerArea || '-'}</h3>
                        <p>
                          Verkauf Amazon {currentResult.analysis?.seller?.soldByAmazonLabel || '-'} | Versand Amazon{' '}
                          {currentResult.analysis?.seller?.shippedByAmazonLabel || '-'}
                        </p>
                      </article>
                      <article className="engine-card engine-tone-info">
                        <div className="engine-card-head">
                          <p className="section-title">Keepa</p>
                          <span className="status-chip info">{currentResult.analysis?.fallbackUsed ? 'fallback' : 'idle'}</span>
                        </div>
                        <h3>{currentResult.keepaScore ?? '-'}</h3>
                        <p>avg90 {currentResult.keepaDiscount90 ?? '-'}% | avg180 {currentResult.keepaDiscount180 ?? '-'}%</p>
                      </article>
                      <article className="engine-card engine-tone-info">
                        <div className="engine-card-head">
                          <p className="section-title">Output</p>
                          <span className={`status-chip ${getToneClass(currentResult.outputStatus)}`}>{currentResult.outputStatus}</span>
                        </div>
                        <h3>Queue {currentResult.outputQueueId ?? '-'}</h3>
                        <p>{currentResult.outputTargetCount || 0} Targets | {currentResult.dayPart}</p>
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
        )}
      </div>
    </Layout>
  );
}

export default DealEnginePage;
