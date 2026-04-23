import { useEffect, useMemo, useState } from 'react';
import { NavLink, useLocation, useNavigate } from 'react-router-dom';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';
import TelegramGroupManager from '../components/telegram/TelegramGroupManager';
import TelegramUserClientPanel from '../components/telegram/TelegramUserClientPanel';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';

const copybotTabs = [
  { label: 'Uebersicht', path: '/copybot' },
  { label: 'Telegram Quellen', path: '/copybot/telegram-sources' },
  { label: 'WhatsApp Quellen', path: '/copybot/whatsapp-sources' },
  { label: 'Preispruef-Logik', path: '/copybot/pricing-rules' },
  { label: 'Sampling & Qualitaet', path: '/copybot/sampling' },
  { label: 'Review Queue', path: '/copybot/review' },
  { label: 'Logs', path: '/copybot/logs' }
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

function CopybotPage() {
  const { user } = useAuth();
  const isAdmin = user?.role === 'admin';
  const location = useLocation();
  const navigate = useNavigate();
  const currentTab = useMemo(() => {
    const match = copybotTabs.find((item) => item.path === location.pathname);
    return match?.path || '/copybot';
  }, [location.pathname]);

  const [overview, setOverview] = useState(null);
  const [sources, setSources] = useState([]);
  const [pricingRules, setPricingRules] = useState([]);
  const [samplingRules, setSamplingRules] = useState([]);
  const [reviewItems, setReviewItems] = useState([]);
  const [logs, setLogs] = useState([]);
  const [status, setStatus] = useState('');
  const [telegramReaderSessionName, setTelegramReaderSessionName] = useState('default-user');
  const [loading, setLoading] = useState(true);
  const [sourceForm, setSourceForm] = useState({
    id: 0,
    name: '',
    platform: 'telegram',
    source_type: 'manual',
    is_active: true,
    priority: 100,
    pricing_rule_id: 1,
    sampling_rule_id: 1,
    success_rate: '',
    notes: ''
  });
  const [pricingForm, setPricingForm] = useState({
    id: 0,
    name: '',
    is_active: true,
    keepa_required: false,
    idealo_required: false,
    autopost_above_score: 85,
    manual_review_below_score: 45,
    allow_amazon: true,
    min_discount_amazon: 15,
    min_score_amazon: 70,
    sampling_amazon: 100,
    allow_fba: true,
    min_discount_fba: 20,
    min_score_fba: 75,
    sampling_fba: 60,
    allow_fbm: true,
    min_discount_fbm: 40,
    min_score_fbm: 82,
    sampling_fbm: 20,
    fbm_requires_manual_review: true,
    min_seller_rating_fbm: '',
    fake_drop_filter_enabled: false,
    coupon_only_penalty: 5,
    variant_switch_penalty: 8,
    marketplace_switch_penalty: 6,
    manual_blacklist_keywords: '',
    manual_whitelist_brands: ''
  });
  const [samplingForm, setSamplingForm] = useState({
    id: 0,
    name: '',
    is_active: true,
    default_sampling: 100,
    sampling_amazon: 100,
    sampling_fba: 100,
    sampling_fbm: 100,
    daily_limit: '',
    min_score: '',
    min_discount: '',
    notes: ''
  });

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

  async function loadAll() {
    setLoading(true);
    try {
      const [overviewData, pricingData, samplingData, sourceData, reviewData, logData] = await Promise.all([
        apiFetch('/api/copybot/overview'),
        apiFetch('/api/copybot/pricing-rules'),
        apiFetch('/api/copybot/sampling-rules'),
        apiFetch('/api/copybot/sources'),
        apiFetch('/api/copybot/review-queue'),
        apiFetch('/api/copybot/logs')
      ]);

      setOverview(overviewData);
      setPricingRules(pricingData.items || []);
      setSamplingRules(samplingData.items || []);
      setSources(sourceData.items || []);
      setReviewItems(reviewData.items || []);
      setLogs(logData.items || []);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Copybot-Daten konnten nicht geladen werden.');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadAll();
  }, [user?.role]);

  useEffect(() => {
    if (!pricingRules.length) {
      return;
    }

    setSourceForm((prev) => ({
      ...prev,
      pricing_rule_id: prev.pricing_rule_id || pricingRules[0].id
    }));
  }, [pricingRules]);

  useEffect(() => {
    if (!samplingRules.length) {
      return;
    }

    setSourceForm((prev) => ({
      ...prev,
      sampling_rule_id: prev.sampling_rule_id || samplingRules[0].id
    }));
  }, [samplingRules]);

  useEffect(() => {
    if (sourceForm.id) {
      return;
    }

    setSourceForm((prev) => ({
      ...prev,
      platform: currentTab === '/copybot/whatsapp-sources' ? 'whatsapp' : 'telegram'
    }));
  }, [currentTab, sourceForm.id]);

  const filteredSources = useMemo(() => {
    if (currentTab === '/copybot/telegram-sources') {
      return sources.filter((item) => item.platform === 'telegram');
    }

    if (currentTab === '/copybot/whatsapp-sources') {
      return sources.filter((item) => item.platform === 'whatsapp');
    }

    return sources;
  }, [currentTab, sources]);

  async function handleToggleCopybot() {
    if (!isAdmin || !overview) {
      return;
    }

    try {
      const data = await apiFetch('/api/copybot/settings', {
        method: 'PUT',
        body: JSON.stringify({ copybotEnabled: !overview.copybotEnabled })
      });

      setOverview((prev) => (prev ? { ...prev, copybotEnabled: Boolean(data.copybotEnabled) } : prev));
      setStatus(data.copybotEnabled ? 'Copybot global aktiviert.' : 'Copybot global deaktiviert.');
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Copybot-Status konnte nicht gespeichert werden.');
    }
  }

  async function handleSaveSource() {
    if (!isAdmin) {
      return;
    }

    if (!sourceForm.name.trim()) {
      setStatus('Bitte zuerst einen Quellnamen eintragen.');
      return;
    }

    if (!pricingRules.length) {
      setStatus('Es ist mindestens eine Preispruef-Logik erforderlich.');
      return;
    }

    try {
      const path = sourceForm.id ? `/api/copybot/sources/${sourceForm.id}` : '/api/copybot/sources';
      const method = sourceForm.id ? 'PUT' : 'POST';
      await apiFetch(path, {
        method,
        body: JSON.stringify(sourceForm)
      });

      setStatus(sourceForm.id ? 'Quelle aktualisiert.' : 'Quelle angelegt.');
      setSourceForm({
        id: 0,
        name: '',
        platform: currentTab === '/copybot/whatsapp-sources' ? 'whatsapp' : 'telegram',
        source_type: 'manual',
        is_active: true,
        priority: 100,
        pricing_rule_id: pricingRules[0]?.id || 1,
        sampling_rule_id: samplingRules[0]?.id || 1,
        success_rate: '',
        notes: ''
      });
      void loadAll();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Quelle konnte nicht gespeichert werden.');
    }
  }

  async function handleToggleSource(item) {
    try {
      await apiFetch(`/api/copybot/sources/${item.id}/active`, {
        method: 'PATCH',
        body: JSON.stringify({ isActive: item.is_active !== 1 })
      });
      setStatus(`Quelle ${item.is_active === 1 ? 'deaktiviert' : 'aktiviert'}.`);
      void loadAll();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Quellenstatus konnte nicht aktualisiert werden.');
    }
  }

  async function handleDeleteSource(item) {
    try {
      await apiFetch(`/api/copybot/sources/${item.id}`, { method: 'DELETE' });
      setStatus('Quelle deaktiviert.');
      void loadAll();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Quelle konnte nicht deaktiviert werden.');
    }
  }

  async function handleTestSource(item) {
    try {
      await apiFetch(`/api/copybot/sources/${item.id}/test`, {
        method: 'POST',
        body: JSON.stringify({})
      });
      setStatus('Quellentest ausgefuehrt.');
      void loadAll();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Quellentest fehlgeschlagen.');
    }
  }

  async function handleSavePricingRule() {
    if (!pricingForm.name.trim()) {
      setStatus('Bitte zuerst einen Namen fuer die Preispruef-Logik eintragen.');
      return;
    }

    const path = pricingForm.id
      ? `/api/copybot/pricing-rules/${pricingForm.id}`
      : '/api/copybot/pricing-rules';
    const method = pricingForm.id ? 'PUT' : 'POST';

    try {
      await apiFetch(path, {
        method,
        body: JSON.stringify(pricingForm)
      });
      setStatus(pricingForm.id ? 'Preispruef-Logik aktualisiert.' : 'Preispruef-Logik angelegt.');
      setPricingForm({
        id: 0,
        name: '',
        is_active: true,
        keepa_required: false,
        idealo_required: false,
        autopost_above_score: 85,
        manual_review_below_score: 45,
        allow_amazon: true,
        min_discount_amazon: 15,
        min_score_amazon: 70,
        sampling_amazon: 100,
        allow_fba: true,
        min_discount_fba: 20,
        min_score_fba: 75,
        sampling_fba: 60,
        allow_fbm: true,
        min_discount_fbm: 40,
        min_score_fbm: 82,
        sampling_fbm: 20,
        fbm_requires_manual_review: true,
        min_seller_rating_fbm: '',
        fake_drop_filter_enabled: false,
        coupon_only_penalty: 5,
        variant_switch_penalty: 8,
        marketplace_switch_penalty: 6,
        manual_blacklist_keywords: '',
        manual_whitelist_brands: ''
      });
      void loadAll();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Preispruef-Logik konnte nicht gespeichert werden.');
    }
  }

  async function handleSaveSamplingRule() {
    if (!samplingForm.name.trim()) {
      setStatus('Bitte zuerst einen Namen fuer die Sampling-Regel eintragen.');
      return;
    }

    const path = samplingForm.id
      ? `/api/copybot/sampling-rules/${samplingForm.id}`
      : '/api/copybot/sampling-rules';
    const method = samplingForm.id ? 'PUT' : 'POST';

    try {
      await apiFetch(path, {
        method,
        body: JSON.stringify(samplingForm)
      });
      setStatus(samplingForm.id ? 'Sampling-Regel aktualisiert.' : 'Sampling-Regel angelegt.');
      setSamplingForm({
        id: 0,
        name: '',
        is_active: true,
        default_sampling: 100,
        sampling_amazon: 100,
        sampling_fba: 100,
        sampling_fbm: 100,
        daily_limit: '',
        min_score: '',
        min_discount: '',
        notes: ''
      });
      void loadAll();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Sampling-Regel konnte nicht gespeichert werden.');
    }
  }

  async function handleReviewAction(id, action) {
    try {
      await apiFetch(`/api/copybot/review-queue/${id}/${action}`, {
        method: 'POST',
        body: JSON.stringify({})
      });
      setStatus(action === 'approve' ? 'Deal freigegeben.' : 'Deal verworfen.');
      void loadAll();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Review-Aktion fehlgeschlagen.');
    }
  }

  function renderOverview() {
    return (
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '1rem' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
            <div>
              <p className="section-title">Copybot</p>
              <h1 className="page-title">Uebersicht</h1>
              <p className="page-subtitle">Quellenbasiertes Deal-Aggregat mit Preispruefung, Sampling und Review.</p>
            </div>
            <button className="primary" onClick={() => void handleToggleCopybot()} disabled={!isAdmin || !overview}>
              {overview?.copybotEnabled ? 'Copybot deaktivieren' : 'Copybot aktivieren'}
            </button>
          </div>
          <div className="responsive-grid">
            {[
              ['Telegram Quellen aktiv', overview?.activeTelegramSources ?? 0],
              ['WhatsApp Quellen aktiv', overview?.activeWhatsappSources ?? 0],
              ['Regeln', overview?.pricingRulesCount ?? 0],
              ['Deals in Review', overview?.reviewCount ?? 0],
              ['Auto freigegeben', overview?.approvedCount ?? 0],
              ['Blockiert / verworfen', overview?.rejectedCount ?? 0]
            ].map(([label, value]) => (
              <article key={label} className="card" style={{ padding: '1rem' }}>
                <p className="section-title">{label}</p>
                <h3 style={{ margin: 0, fontSize: '1.8rem' }}>{value}</h3>
              </article>
            ))}
          </div>
          <div className="split-row">
            <div className="card" style={{ padding: '1rem' }}>
              <p className="section-title">Letzte Quelle</p>
              <strong>{overview?.lastProcessedSource?.name || '-'}</strong>
              <p className="text-muted" style={{ marginBottom: 0 }}>
                {overview?.lastProcessedSource
                  ? `${overview.lastProcessedSource.platform} · ${formatDateTime(overview.lastProcessedSource.last_import_at)}`
                  : 'Noch keine Quelle verarbeitet'}
              </p>
            </div>
            <div className="card" style={{ padding: '1rem' }}>
              <p className="section-title">Globaler Status</p>
              <strong>{overview?.copybotEnabled ? 'Aktiv' : 'Deaktiviert'}</strong>
              <p className="text-muted" style={{ marginBottom: 0 }}>
                Wenn deaktiviert, werden keine neuen Quellen verarbeitet oder Deals importiert.
              </p>
            </div>
          </div>
        </section>

        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Letzte verarbeitete Deals</p>
          <div style={{ display: 'grid', gap: '0.75rem' }}>
            {(overview?.lastProcessedDeals || []).map((item) => (
              <div key={item.id} className="radio-card" style={{ justifyContent: 'space-between', alignItems: 'start' }}>
                <div>
                  <strong>{item.title}</strong>
                  <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>
                    {item.source_name} · {item.platform} · {item.seller_type}
                  </p>
                </div>
                <div style={{ textAlign: 'right' }}>
                  <span className="badge">{item.status}</span>
                  <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>
                    Score {item.score} · Rabatt {item.detected_discount}%
                  </p>
                </div>
              </div>
            ))}
            {!overview?.lastProcessedDeals?.length && <p className="text-muted">Noch keine verarbeiteten Deals vorhanden.</p>}
          </div>
        </section>
      </div>
    );
  }

  function renderSources() {
    if (currentTab === '/copybot/telegram-sources') {
      return (
        <div style={{ display: 'grid', gap: '1rem' }}>
          <section className="card" style={{ padding: '1.25rem' }}>
            <p className="section-title">Telegram Quellen</p>
            <h1 className="page-title">Telegram Gruppen</h1>
            <p className="page-subtitle" style={{ marginBottom: 0 }}>
              Gruppen einfach eintragen, aktivieren und nur bei Bedarf erweitern.
            </p>
          </section>

          <TelegramGroupManager
            onStatusChange={setStatus}
            onSessionNameChange={setTelegramReaderSessionName}
          />
          <TelegramUserClientPanel
            onStatusChange={setStatus}
            initialSessionName={telegramReaderSessionName}
          />
        </div>
      );
    }

    return (
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">
            {currentTab === '/copybot/whatsapp-sources' ? 'WhatsApp Quellen' : 'Telegram Login & Quellen'}
          </p>
          <h1 className="page-title">
            {currentTab === '/copybot/whatsapp-sources' ? 'WhatsApp Quellen' : 'Telegram Quellen'}
          </h1>
        </section>

        {isAdmin && (
          <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '0.75rem' }}>
            <div className="form-row">
              <input
                placeholder="Quellenname"
                value={sourceForm.name}
                onChange={(event) => setSourceForm((prev) => ({ ...prev, name: event.target.value }))}
              />
              <select
                value={sourceForm.source_type}
                onChange={(event) => setSourceForm((prev) => ({ ...prev, source_type: event.target.value }))}
              >
                <option value="group">gruppe</option>
                <option value="channel">kanal</option>
                <option value="manual">manuelle quelle</option>
                <option value="importer">anderer importer</option>
              </select>
              <input
                type="number"
                placeholder="Prioritaet"
                value={sourceForm.priority}
                onChange={(event) => setSourceForm((prev) => ({ ...prev, priority: Number(event.target.value || 100) }))}
              />
            </div>
            <div className="form-row">
              <select
                value={sourceForm.platform}
                onChange={(event) =>
                  setSourceForm((prev) => ({ ...prev, platform: event.target.value }))
                }
              >
                <option value="telegram">telegram</option>
                <option value="whatsapp">whatsapp</option>
              </select>
              <select
                value={sourceForm.pricing_rule_id}
                onChange={(event) =>
                  setSourceForm((prev) => ({ ...prev, pricing_rule_id: Number(event.target.value) }))
                }
              >
                {pricingRules.map((rule) => (
                  <option key={rule.id} value={rule.id}>
                    {rule.name}
                  </option>
                ))}
              </select>
              <select
                value={sourceForm.sampling_rule_id}
                onChange={(event) =>
                  setSourceForm((prev) => ({ ...prev, sampling_rule_id: Number(event.target.value) }))
                }
              >
                {samplingRules.map((rule) => (
                  <option key={rule.id} value={rule.id}>
                    {rule.name}
                  </option>
                ))}
              </select>
            </div>
            <div className="form-row">
              <input
                placeholder="Erfolgsquote optional"
                value={sourceForm.success_rate}
                onChange={(event) => setSourceForm((prev) => ({ ...prev, success_rate: event.target.value }))}
              />
              <input
                placeholder="Bemerkung"
                value={sourceForm.notes}
                onChange={(event) => setSourceForm((prev) => ({ ...prev, notes: event.target.value }))}
              />
              <label className="checkbox-card">
                <span>Quelle aktiv</span>
                <input
                  type="checkbox"
                  checked={sourceForm.is_active}
                  onChange={(event) => setSourceForm((prev) => ({ ...prev, is_active: event.target.checked }))}
                />
              </label>
            </div>
            <button className="primary" onClick={() => void handleSaveSource()}>
              {sourceForm.id ? 'Quelle speichern' : 'Neue Quelle anlegen'}
            </button>
          </section>
        )}

        <section className="card" style={{ padding: '1.25rem' }}>
          <div style={{ display: 'grid', gap: '0.75rem' }}>
            {filteredSources.map((item) => (
              <div key={item.id} className="radio-card" style={{ display: 'grid', gap: '0.75rem' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
                  <div>
                    <strong>{item.name}</strong>
                    <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>
                      #{item.id} · {item.source_type} · Prioritaet {item.priority}
                    </p>
                  </div>
                  <span className={`status-chip ${item.is_active === 1 ? 'success' : 'warning'}`}>
                    {item.is_active === 1 ? 'aktiv' : 'inaktiv'}
                  </span>
                </div>
                <div className="split-row">
                  <p className="text-muted" style={{ margin: 0 }}>Preislogik: {item.pricing_rule_name}</p>
                  <p className="text-muted" style={{ margin: 0 }}>Sampling: {item.sampling_rule_name || '-'}</p>
                  <p className="text-muted" style={{ margin: 0 }}>Letzter Import: {formatDateTime(item.last_import_at)}</p>
                </div>
                {item.notes && <p className="text-muted" style={{ margin: 0 }}>{item.notes}</p>}
                {isAdmin && (
                  <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap' }}>
                    <button className="secondary" onClick={() => setSourceForm({
                      id: item.id,
                      name: item.name,
                      platform: item.platform,
                      source_type: item.source_type,
                      is_active: item.is_active === 1,
                      priority: item.priority,
                      pricing_rule_id: item.pricing_rule_id,
                      sampling_rule_id: item.sampling_rule_id || samplingRules[0]?.id || 1,
                      success_rate: item.success_rate ? String(item.success_rate) : '',
                      notes: item.notes || ''
                    })}>
                      Bearbeiten
                    </button>
                    <button className="secondary" onClick={() => void handleToggleSource(item)}>
                      {item.is_active === 1 ? 'Deaktivieren' : 'Aktivieren'}
                    </button>
                    <button className="secondary" onClick={() => void handleTestSource(item)}>Einzeln testen</button>
                    <button className="secondary" onClick={() => void handleDeleteSource(item)}>Loeschen / deaktivieren</button>
                  </div>
                )}
              </div>
            ))}
            {!filteredSources.length && <p className="text-muted">Noch keine Quellen vorhanden.</p>}
          </div>
        </section>
      </div>
    );
  }

  function renderPricingRules() {
    return (
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Preispruef-Logik</p>
          <h1 className="page-title">Seller-Typ-spezifische Bewertung</h1>
        </section>
        {isAdmin && (
          <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '0.75rem' }}>
            <div className="form-row">
              <input
                placeholder="Name"
                value={pricingForm.name}
                onChange={(event) => setPricingForm((prev) => ({ ...prev, name: event.target.value }))}
              />
              <input
                type="number"
                placeholder="Auto-Post ab Score"
                value={pricingForm.autopost_above_score}
                onChange={(event) =>
                  setPricingForm((prev) => ({ ...prev, autopost_above_score: Number(event.target.value || 0) }))
                }
              />
              <input
                type="number"
                placeholder="Review unter Score"
                value={pricingForm.manual_review_below_score}
                onChange={(event) =>
                  setPricingForm((prev) => ({ ...prev, manual_review_below_score: Number(event.target.value || 0) }))
                }
              />
            </div>
            <div className="split-row">
              <label className="checkbox-card"><span>Keepa Pflicht</span><input type="checkbox" checked={pricingForm.keepa_required} onChange={(event) => setPricingForm((prev) => ({ ...prev, keepa_required: event.target.checked }))} /></label>
              <label className="checkbox-card"><span>Idealo Pflicht</span><input type="checkbox" checked={pricingForm.idealo_required} onChange={(event) => setPricingForm((prev) => ({ ...prev, idealo_required: event.target.checked }))} /></label>
              <label className="checkbox-card"><span>Fake-Drop Filter</span><input type="checkbox" checked={pricingForm.fake_drop_filter_enabled} onChange={(event) => setPricingForm((prev) => ({ ...prev, fake_drop_filter_enabled: event.target.checked }))} /></label>
            </div>
            <div className="responsive-grid">
              {[
                ['AMAZON', 'allow_amazon', 'min_discount_amazon', 'min_score_amazon', 'sampling_amazon'],
                ['FBA', 'allow_fba', 'min_discount_fba', 'min_score_fba', 'sampling_fba'],
                ['FBM', 'allow_fbm', 'min_discount_fbm', 'min_score_fbm', 'sampling_fbm']
              ].map(([sellerType, allowKey, discountKey, scoreKey, samplingKey]) => (
                <div key={sellerType} className="card" style={{ padding: '1rem', display: 'grid', gap: '0.75rem' }}>
                  <strong>{sellerType}</strong>
                  <label className="checkbox-card">
                    <span>Erlauben</span>
                    <input
                      type="checkbox"
                      checked={Boolean(pricingForm[allowKey])}
                      onChange={(event) =>
                        setPricingForm((prev) => ({ ...prev, [allowKey]: event.target.checked }))
                      }
                    />
                  </label>
                  <input type="number" placeholder="Min Rabatt" value={Number(pricingForm[discountKey])} onChange={(event) => setPricingForm((prev) => ({ ...prev, [discountKey]: Number(event.target.value || 0) }))} />
                  <input type="number" placeholder="Min Score" value={Number(pricingForm[scoreKey])} onChange={(event) => setPricingForm((prev) => ({ ...prev, [scoreKey]: Number(event.target.value || 0) }))} />
                  <input type="number" placeholder="Sampling %" value={Number(pricingForm[samplingKey])} onChange={(event) => setPricingForm((prev) => ({ ...prev, [samplingKey]: Number(event.target.value || 0) }))} />
                </div>
              ))}
            </div>
            <div className="form-row">
              <input type="number" placeholder="Coupon-Penalty" value={pricingForm.coupon_only_penalty} onChange={(event) => setPricingForm((prev) => ({ ...prev, coupon_only_penalty: Number(event.target.value || 0) }))} />
              <input type="number" placeholder="Variant-Switch-Penalty" value={pricingForm.variant_switch_penalty} onChange={(event) => setPricingForm((prev) => ({ ...prev, variant_switch_penalty: Number(event.target.value || 0) }))} />
              <input type="number" placeholder="Marketplace-Switch-Penalty" value={pricingForm.marketplace_switch_penalty} onChange={(event) => setPricingForm((prev) => ({ ...prev, marketplace_switch_penalty: Number(event.target.value || 0) }))} />
            </div>
            <div className="form-row">
              <input placeholder="Blacklist Keywords, komma-getrennt" value={pricingForm.manual_blacklist_keywords} onChange={(event) => setPricingForm((prev) => ({ ...prev, manual_blacklist_keywords: event.target.value }))} />
              <input placeholder="Whitelist Brands, komma-getrennt" value={pricingForm.manual_whitelist_brands} onChange={(event) => setPricingForm((prev) => ({ ...prev, manual_whitelist_brands: event.target.value }))} />
              <input placeholder="Min Seller Rating FBM optional" value={pricingForm.min_seller_rating_fbm} onChange={(event) => setPricingForm((prev) => ({ ...prev, min_seller_rating_fbm: event.target.value }))} />
            </div>
            <label className="checkbox-card">
              <span>FBM immer manuell pruefen</span>
              <input type="checkbox" checked={pricingForm.fbm_requires_manual_review} onChange={(event) => setPricingForm((prev) => ({ ...prev, fbm_requires_manual_review: event.target.checked }))} />
            </label>
            <button className="primary" onClick={() => void handleSavePricingRule()}>
              {pricingForm.id ? 'Preislogik speichern' : 'Neue Preislogik anlegen'}
            </button>
          </section>
        )}
        <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '0.75rem' }}>
          {pricingRules.map((rule) => (
            <div key={rule.id} className="radio-card" style={{ display: 'grid', gap: '0.5rem' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
                <strong>{rule.name}</strong>
                <span className={`status-chip ${rule.is_active === 1 ? 'success' : 'warning'}`}>{rule.is_active === 1 ? 'aktiv' : 'inaktiv'}</span>
              </div>
              <p className="text-muted" style={{ margin: 0 }}>
                Auto-Post ab {rule.autopost_above_score} · Review unter {rule.manual_review_below_score}
              </p>
              <p className="text-muted" style={{ margin: 0 }}>
                AMAZON {rule.min_discount_amazon}%/{rule.min_score_amazon} · FBA {rule.min_discount_fba}%/{rule.min_score_fba} · FBM {rule.min_discount_fbm}%/{rule.min_score_fbm}
              </p>
              {isAdmin && <button className="secondary" onClick={() => setPricingForm({
                id: rule.id,
                name: rule.name,
                is_active: rule.is_active === 1,
                keepa_required: rule.keepa_required === 1,
                idealo_required: rule.idealo_required === 1,
                autopost_above_score: rule.autopost_above_score,
                manual_review_below_score: rule.manual_review_below_score,
                allow_amazon: rule.allow_amazon === 1,
                min_discount_amazon: rule.min_discount_amazon,
                min_score_amazon: rule.min_score_amazon,
                sampling_amazon: rule.sampling_amazon,
                allow_fba: rule.allow_fba === 1,
                min_discount_fba: rule.min_discount_fba,
                min_score_fba: rule.min_score_fba,
                sampling_fba: rule.sampling_fba,
                allow_fbm: rule.allow_fbm === 1,
                min_discount_fbm: rule.min_discount_fbm,
                min_score_fbm: rule.min_score_fbm,
                sampling_fbm: rule.sampling_fbm,
                fbm_requires_manual_review: rule.fbm_requires_manual_review === 1,
                min_seller_rating_fbm: rule.min_seller_rating_fbm ? String(rule.min_seller_rating_fbm) : '',
                fake_drop_filter_enabled: rule.fake_drop_filter_enabled === 1,
                coupon_only_penalty: rule.coupon_only_penalty,
                variant_switch_penalty: rule.variant_switch_penalty,
                marketplace_switch_penalty: rule.marketplace_switch_penalty,
                manual_blacklist_keywords: rule.manual_blacklist_keywords.join(', '),
                manual_whitelist_brands: rule.manual_whitelist_brands.join(', ')
              })}>Bearbeiten</button>}
            </div>
          ))}
        </section>
      </div>
    );
  }

  function renderSampling() {
    return (
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Sampling & Qualitaet</p>
          <h1 className="page-title">Quellensteuerung und Tageslimits</h1>
        </section>
        {isAdmin && (
          <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '0.75rem' }}>
            <div className="form-row">
              <input placeholder="Name" value={samplingForm.name} onChange={(event) => setSamplingForm((prev) => ({ ...prev, name: event.target.value }))} />
              <input type="number" placeholder="Default Sampling %" value={samplingForm.default_sampling} onChange={(event) => setSamplingForm((prev) => ({ ...prev, default_sampling: Number(event.target.value || 0) }))} />
              <input type="number" placeholder="Tageslimit optional" value={samplingForm.daily_limit} onChange={(event) => setSamplingForm((prev) => ({ ...prev, daily_limit: event.target.value }))} />
            </div>
            <div className="form-row">
              <input type="number" placeholder="AMAZON %" value={samplingForm.sampling_amazon} onChange={(event) => setSamplingForm((prev) => ({ ...prev, sampling_amazon: Number(event.target.value || 0) }))} />
              <input type="number" placeholder="FBA %" value={samplingForm.sampling_fba} onChange={(event) => setSamplingForm((prev) => ({ ...prev, sampling_fba: Number(event.target.value || 0) }))} />
              <input type="number" placeholder="FBM %" value={samplingForm.sampling_fbm} onChange={(event) => setSamplingForm((prev) => ({ ...prev, sampling_fbm: Number(event.target.value || 0) }))} />
            </div>
            <div className="form-row">
              <input placeholder="Min Score optional" value={samplingForm.min_score} onChange={(event) => setSamplingForm((prev) => ({ ...prev, min_score: event.target.value }))} />
              <input placeholder="Min Rabatt optional" value={samplingForm.min_discount} onChange={(event) => setSamplingForm((prev) => ({ ...prev, min_discount: event.target.value }))} />
              <input placeholder="Notiz" value={samplingForm.notes} onChange={(event) => setSamplingForm((prev) => ({ ...prev, notes: event.target.value }))} />
            </div>
            <label className="checkbox-card"><span>Regel aktiv</span><input type="checkbox" checked={samplingForm.is_active} onChange={(event) => setSamplingForm((prev) => ({ ...prev, is_active: event.target.checked }))} /></label>
            <button className="primary" onClick={() => void handleSaveSamplingRule()}>
              {samplingForm.id ? 'Sampling speichern' : 'Neue Sampling-Regel anlegen'}
            </button>
          </section>
        )}
        <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '0.75rem' }}>
          {samplingRules.map((rule) => (
            <div key={rule.id} className="radio-card" style={{ display: 'grid', gap: '0.4rem' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
                <strong>{rule.name}</strong>
                <span className={`status-chip ${rule.is_active === 1 ? 'success' : 'warning'}`}>{rule.is_active === 1 ? 'aktiv' : 'inaktiv'}</span>
              </div>
              <p className="text-muted" style={{ margin: 0 }}>
                AMAZON {rule.sampling_amazon}% · FBA {rule.sampling_fba}% · FBM {rule.sampling_fbm}% · Default {rule.default_sampling}%
              </p>
              <p className="text-muted" style={{ margin: 0 }}>
                Tageslimit {rule.daily_limit ?? '-'} · Min Score {rule.min_score ?? '-'} · Min Rabatt {rule.min_discount ?? '-'}
              </p>
              {isAdmin && <button className="secondary" onClick={() => setSamplingForm({
                id: rule.id,
                name: rule.name,
                is_active: rule.is_active === 1,
                default_sampling: rule.default_sampling,
                sampling_amazon: rule.sampling_amazon,
                sampling_fba: rule.sampling_fba,
                sampling_fbm: rule.sampling_fbm,
                daily_limit: rule.daily_limit ? String(rule.daily_limit) : '',
                min_score: rule.min_score ? String(rule.min_score) : '',
                min_discount: rule.min_discount ? String(rule.min_discount) : '',
                notes: rule.notes || ''
              })}>Bearbeiten</button>}
            </div>
          ))}
        </section>
      </div>
    );
  }

  function renderReviewQueue() {
    return (
      <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '0.75rem' }}>
        <div>
          <p className="section-title">Review Queue</p>
          <h1 className="page-title">Manuelle Freigabe</h1>
        </div>
        {reviewItems.map((item) => (
          <div key={item.id} className="radio-card" style={{ display: 'grid', gap: '0.75rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
              <div>
                <strong>{item.title}</strong>
                <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>
                  Quelle {item.source_name} · Seller {item.seller_type} · Quelle {item.source_is_active === 1 ? 'aktiv' : 'inaktiv'}
                </p>
              </div>
              <span className="badge">Score {item.score}</span>
            </div>
            <div className="split-row">
              <p className="text-muted" style={{ margin: 0 }}>Preis: {item.current_price ?? '-'} EUR</p>
              <p className="text-muted" style={{ margin: 0 }}>Rabatt: {item.detected_discount}%</p>
              <p className="text-muted" style={{ margin: 0 }}>Keepa: {item.keepa_result?.status || '-'}</p>
            </div>
            <div className="split-row">
              <p className="text-muted" style={{ margin: 0 }}>Idealo: {item.comparison_result?.status || '-'}</p>
              <p className="text-muted" style={{ margin: 0 }}>Letztes Posting: Repost-Schutz in History aktiv</p>
              <p className="text-muted" style={{ margin: 0 }}>{item.decision_reason || '-'}</p>
            </div>
            <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap' }}>
              {isAdmin && <button className="primary" onClick={() => void handleReviewAction(item.id, 'approve')}>Freigeben</button>}
              {isAdmin && <button className="secondary" onClick={() => void handleReviewAction(item.id, 'reject')}>Verwerfen</button>}
              <button className="secondary" onClick={() => navigate('/generator')}>In Generator oeffnen</button>
            </div>
          </div>
        ))}
        {!reviewItems.length && <p className="text-muted">Keine Deals in der Review Queue.</p>}
      </section>
    );
  }

  function renderLogs() {
    return (
      <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '0.75rem' }}>
        <div>
          <p className="section-title">Logs</p>
          <h1 className="page-title">Verarbeitung und Entscheidungen</h1>
        </div>
        {logs.map((item) => (
          <div key={item.id} className="radio-card" style={{ justifyContent: 'space-between', alignItems: 'start' }}>
            <div>
              <strong>{item.event_type}</strong>
              <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>{item.message}</p>
            </div>
            <div style={{ textAlign: 'right' }}>
              <span className={`status-chip ${item.level === 'warning' ? 'warning' : 'info'}`}>{item.level}</span>
              <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>{item.source_name || '-'} · {formatDateTime(item.created_at)}</p>
            </div>
          </div>
        ))}
        {!logs.length && <p className="text-muted">Noch keine Logs vorhanden.</p>}
      </section>
    );
  }

  function renderCurrentTab() {
    if (currentTab === '/copybot/telegram-sources' || currentTab === '/copybot/whatsapp-sources') {
      return renderSources();
    }

    if (currentTab === '/copybot/pricing-rules') {
      return renderPricingRules();
    }

    if (currentTab === '/copybot/sampling') {
      return renderSampling();
    }

    if (currentTab === '/copybot/review') {
      return renderReviewQueue();
    }

    if (currentTab === '/copybot/logs') {
      return renderLogs();
    }

    return renderOverview();
  }

  return (
    <Layout>
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1rem' }}>
          <nav style={{ display: 'flex', gap: '0.65rem', flexWrap: 'wrap' }}>
            {copybotTabs.map((item) => (
              <NavLink
                key={item.path}
                to={item.path}
                className={({ isActive }) => (isActive ? 'status-chip success' : 'status-chip info')}
              >
                {item.label}
              </NavLink>
            ))}
          </nav>
        </section>

        {status && (
          <section className="card" style={{ padding: '1rem' }}>
            <p style={{ margin: 0 }}>{status}</p>
          </section>
        )}

        {loading ? <section className="card" style={{ padding: '1.25rem' }}>Laedt Copybot-Daten...</section> : renderCurrentTab()}
      </div>
    </Layout>
  );
}

export default CopybotPage;
