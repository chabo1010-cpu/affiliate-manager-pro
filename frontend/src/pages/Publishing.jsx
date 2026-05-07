import { useEffect, useMemo, useState } from 'react';
import { NavLink, useLocation } from 'react-router-dom';
import Layout from '../components/layout/Layout';
import { useAuth } from '../context/AuthContext';
import './Publishing.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';
const PUBLISHING_LOAD_TIMEOUT_MS = 2000;

const tabs = [
  { label: 'Queue', path: '/publishing' },
  { label: 'Worker Status', path: '/publishing/workers' },
  { label: 'Telegram Bot', path: '/publishing/telegram' },
  { label: 'WhatsApp Output', path: '/publishing/whatsapp' },
  { label: 'Facebook Worker', path: '/publishing/facebook' },
  { label: 'Logs', path: '/publishing/logs' }
];

function getDefaultTelegramBotConfig() {
  return {
    enabled: true,
    defaultRetryLimit: 3,
    tokenConfigured: false,
    fallbackChatConfigured: false,
    envFallbackActive: false,
    targets: [],
    effectiveTargets: []
  };
}

function getDefaultWhatsappClientConfig() {
  return {
    enabled: false,
    endpointConfigured: false,
    providerConfigured: false,
    providerMode: 'playwright',
    providerLabel: 'Playwright Worker',
    senderConfigured: false,
    sender: '',
    retryLimit: 3,
    configuredTargets: 0,
    publishTargets: 0,
    workerEnabled: false,
    workerStatus: 'stopped',
    connectionStatus: 'not_connected',
    sessionValid: false,
    qrRequired: false,
    qrCodeDataUrl: '',
    healthStatus: 'waiting',
    controlEndpointConfigured: false,
    browserChannel: '',
    browserExecutablePath: '',
    lastHealthCheckAt: null,
    channelReachable: false,
    lastRestartAt: null,
    lastSuccessfulPostAt: null,
    lastConnectedAt: null,
    sessionSavedAt: null,
    lastError: '',
    lastErrorAt: null,
    errorCount: 0,
    profileWritable: false,
    workerPid: null,
    loginMonitorActive: false,
    loginTimeoutMs: 120000,
    browserProfileDir: '',
    profileBackupDir: '',
    sessionResetAt: null,
    channelNavigationStatus: '',
    channelAdminStatus: '',
    lastChannelTargetRef: '',
    lastChannelDebugAt: null,
    lastChannelDebugMessage: '',
    lastChannelDebugArtifacts: [],
    lastChannelPreferredSelector: '',
    lastChannelComposerCandidates: [],
    lastChannelDomSnapshotPath: '',
    lastChannelHtmlSnapshotPath: '',
    lastChannelScreenshotPath: '',
    currentUrl: '',
    currentTitle: '',
    sendCooldownMs: 4000,
    alertsEnabled: true,
    alertTargetRef: '@WhatsappStatusFehler',
    alertTargetType: 'SYSTEM_ALERT_CHANNEL',
    queue: {
      open: 0,
      pending: 0,
      retry: 0,
      sending: 0,
      failed: 0,
      sent: 0
    }
  };
}

function getDefaultWhatsappTargetConfig() {
  return {
    targets: [],
    effectiveTargets: [],
    defaultTargetRef: ''
  };
}

function getDefaultFacebookSettings() {
  return {
    facebookEnabled: false,
    facebookSessionMode: 'persistent',
    facebookDefaultRetryLimit: 3,
    facebookDefaultTarget: ''
  };
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

function getStatusTone(value) {
  const normalized = String(value || '').toLowerCase();
  if (
    normalized.includes('failed') ||
    normalized.includes('error') ||
    normalized.includes('fehlt') ||
    normalized.includes('abgelaufen')
  ) {
    return 'danger';
  }
  if (
    normalized.includes('retry') ||
    normalized.includes('pending') ||
    normalized.includes('hold') ||
    normalized.includes('deaktiviert') ||
    normalized.includes('skipped') ||
    normalized.includes('qr') ||
    normalized.includes('gestoppt')
  ) {
    return 'warning';
  }
  if (
    normalized.includes('aktiv') ||
    normalized.includes('ready') ||
    normalized.includes('sent') ||
    normalized.includes('vorhanden') ||
    normalized.includes('verbunden')
  ) {
    return 'success';
  }
  return 'info';
}

function getTelegramChannelKindLabel(kind) {
  return kind === 'live' ? 'LIVE KANAL' : kind === 'test' ? 'TESTGRUPPE' : 'ZIELKANAL';
}

function getTelegramTargetStatus(target = {}) {
  const targetKind = target.targetKind || 'custom';

  if (target.isActive && target.useForPublishing) {
    return {
      label: 'aktiv',
      tone: targetKind === 'live' ? 'danger' : 'success'
    };
  }

  if (!target.useForPublishing) {
    return {
      label: 'nur gespeichert',
      tone: 'info'
    };
  }

  return {
    label: 'deaktiviert',
    tone: targetKind === 'live' || target.requiresManualActivation ? 'danger' : 'warning'
  };
}

function getWhatsappConnectionLabel(status = '') {
  const normalized = String(status || '').toLowerCase();
  if (normalized === 'connected') {
    return 'verbunden';
  }
  if (normalized === 'qr_required') {
    return 'QR erforderlich';
  }
  if (normalized === 'session_expired') {
    return 'Session abgelaufen';
  }
  if (normalized === 'recovering') {
    return 'Recovery laeuft';
  }
  if (normalized === 'error') {
    return 'Fehler';
  }
  return 'nicht verbunden';
}

function getWhatsappWorkerLabel(status = '') {
  const normalized = String(status || '').toLowerCase();
  if (normalized === 'running') {
    return 'Worker laeuft';
  }
  if (normalized === 'recovering') {
    return 'Recovery laeuft';
  }
  if (normalized === 'error') {
    return 'Fehler';
  }
  return 'Worker gestoppt';
}

function getWhatsappTargetKind(target = {}) {
  const targetType = String(target.targetType || '').toUpperCase();
  if (targetType === 'WHATSAPP_TEST_CHANNEL') {
    return 'test';
  }
  if (target.requiresManualActivation) {
    return 'live';
  }
  return 'standard';
}

function getWhatsappTargetTypeLabel(target = {}) {
  const targetKind = getWhatsappTargetKind(target);
  if (targetKind === 'test') {
    return 'TEST';
  }
  if (targetKind === 'live') {
    return 'LIVE';
  }
  return target.targetType || 'WHATSAPP_CHANNEL';
}

function getWhatsappTargetStatus(target = {}) {
  const targetKind = getWhatsappTargetKind(target);
  if (target.isActive && target.useForPublishing) {
    return {
      label: 'aktiv',
      tone: targetKind === 'live' ? 'danger' : 'success'
    };
  }

  if (!target.useForPublishing) {
    return {
      label: 'nur gespeichert',
      tone: 'info'
    };
  }

  return {
    label: 'deaktiviert',
    tone: targetKind === 'live' ? 'danger' : 'warning'
  };
}

function getWhatsappChannelNavigationLabel(status = '') {
  const normalized = String(status || '').toUpperCase();
  if (normalized === 'WHATSAPP_CHANNEL_COMPOSER_FOUND') {
    return 'Composer gefunden';
  }
  if (normalized === 'WHATSAPP_CHANNEL_FOUND') {
    return 'Channel geoeffnet';
  }
  if (normalized === 'WHATSAPP_CHANNEL_NAVIGATION_NOT_IMPLEMENTED') {
    return 'Channel-Navigation fehlgeschlagen';
  }
  if (normalized === 'WHATSAPP_CHANNEL_COMPOSER_NOT_FOUND') {
    return 'Composer noch nicht gefunden';
  }
  if (normalized === 'WHATSAPP_CHANNEL_NO_ADMIN_RIGHTS') {
    return 'Keine Admin-Rechte erkannt';
  }
  return status || '-';
}

function getWhatsappAdminStatusLabel(status = '') {
  const normalized = String(status || '').toUpperCase();
  if (normalized === 'ADMIN_CONTROLS_VISIBLE' || normalized === 'ADMIN_READY') {
    return 'Admin-Steuerung sichtbar';
  }
  if (normalized === 'WHATSAPP_CHANNEL_NO_ADMIN_RIGHTS') {
    return 'Keine Posting-Admin-Rechte';
  }
  if (normalized === 'UNKNOWN' || normalized === '') {
    return '-';
  }
  return status;
}

function PublishingPage() {
  const { user } = useAuth();
  const isAdmin = user?.role === 'admin';
  const location = useLocation();
  const currentTab = useMemo(() => tabs.find((item) => item.path === location.pathname)?.path || '/publishing', [location.pathname]);

  const [queue, setQueue] = useState([]);
  const [logs, setLogs] = useState([]);
  const [workerStatus, setWorkerStatus] = useState(null);
  const [status, setStatus] = useState('');
  const [loading, setLoading] = useState(true);
  const [queueLoading, setQueueLoading] = useState(false);
  const [reloadKey, setReloadKey] = useState(0);
  const [telegramBotConfig, setTelegramBotConfig] = useState(getDefaultTelegramBotConfig);
  const [whatsappClientConfig, setWhatsappClientConfig] = useState(getDefaultWhatsappClientConfig);
  const [whatsappTargetConfig, setWhatsappTargetConfig] = useState(getDefaultWhatsappTargetConfig);
  const [facebookSettings, setFacebookSettings] = useState(getDefaultFacebookSettings);
  const [testingTelegramTargetId, setTestingTelegramTargetId] = useState(null);
  const [testingWhatsappTargetId, setTestingWhatsappTargetId] = useState(null);
  const [sendingWhatsappTargetId, setSendingWhatsappTargetId] = useState(null);
  const [whatsappRuntimeBusy, setWhatsappRuntimeBusy] = useState('');
  const [whatsappRecentQueue, setWhatsappRecentQueue] = useState([]);
  const [whatsappRecentLogs, setWhatsappRecentLogs] = useState([]);

  async function apiFetch(path, options = {}, config = {}) {
    const timeoutMs = Number(config.timeoutMs || PUBLISHING_LOAD_TIMEOUT_MS);
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(`${API_BASE_URL}${path}`, {
        ...options,
        signal: controller.signal,
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
          'X-User-Role': user?.role || '',
          ...(options.headers || {})
        }
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(data?.error || `Request fehlgeschlagen (${response.status}).`);
      }
      return data;
    } catch (error) {
      if (error?.name === 'AbortError') {
        console.warn('[PUBLISHING_LOAD_TIMEOUT]', {
          tab: currentTab,
          path,
          timeoutMs
        });
        throw new Error(`Timeout nach ${timeoutMs}ms: ${path}`);
      }
      throw error;
    } finally {
      window.clearTimeout(timeoutId);
    }
  }

  useEffect(() => {
    let cancelled = false;

    async function loadCurrentTab() {
      const startedAt = performance.now();
      const requests = [{ key: 'workerStatus', path: '/api/publishing/workers/status' }];

      if (currentTab === '/publishing/logs') {
        requests.push({ key: 'logs', path: '/api/publishing/logs?limit=80' });
      }

      if (currentTab === '/publishing/telegram' && isAdmin) {
        requests.push({ key: 'telegramBotConfig', path: '/api/publishing/telegram-bot-client' });
      }

      if (currentTab === '/publishing/whatsapp' && isAdmin) {
        requests.push({ key: 'whatsappRuntime', path: '/api/publishing/whatsapp-runtime' });
      }

      console.info('[PUBLISHING_LOAD_START]', {
        tab: currentTab,
        requests: requests.map((item) => item.path)
      });

      setLoading(true);
      setStatus('');
      if (currentTab !== '/publishing') {
        setQueueLoading(false);
      }

      try {
        const results = await Promise.allSettled(requests.map((item) => apiFetch(item.path)));
        if (cancelled) {
          return;
        }

        requests.forEach((request, index) => {
          const result = results[index];
          if (result.status !== 'fulfilled') {
            return;
          }

          if (request.key === 'workerStatus') {
            setWorkerStatus(result.value);
            setWhatsappClientConfig({
              ...getDefaultWhatsappClientConfig(),
              ...(result.value?.whatsapp || {})
            });
            setFacebookSettings({
              facebookEnabled: Boolean(result.value?.facebook?.enabled),
              facebookSessionMode: result.value?.facebook?.sessionMode || 'persistent',
              facebookDefaultRetryLimit: Number(result.value?.facebook?.retryLimit || 3),
              facebookDefaultTarget: result.value?.facebook?.defaultTarget || ''
            });
          }

          if (request.key === 'logs') {
            setLogs(result.value?.items || []);
          }

          if (request.key === 'telegramBotConfig') {
            setTelegramBotConfig({
              ...getDefaultTelegramBotConfig(),
              ...(result.value?.item || {})
            });
          }

          if (request.key === 'whatsappTargetConfig') {
            setWhatsappTargetConfig({
              ...getDefaultWhatsappTargetConfig(),
              ...(result.value?.item || {})
            });
          }

          if (request.key === 'whatsappRuntime') {
            const snapshot = result.value?.item || {};
            setWhatsappClientConfig({
              ...getDefaultWhatsappClientConfig(),
              ...(snapshot.runtime || {}),
              ...(snapshot.workerStatus || {})
            });
            setWhatsappTargetConfig({
              ...getDefaultWhatsappTargetConfig(),
              ...(snapshot.targets || {})
            });
            setWhatsappRecentQueue(snapshot.recentQueue || []);
            setWhatsappRecentLogs(snapshot.recentLogs || []);
          }
        });

        const errors = results
          .filter((item) => item.status === 'rejected')
          .map((item) => (item.reason instanceof Error ? item.reason.message : 'Publishing konnte nicht geladen werden.'));

        if (errors.length) {
          setStatus(errors[0]);
          console.error('[PUBLISHING_LOAD_ERROR]', {
            tab: currentTab,
            errors
          });
        }

        console.info('[PUBLISHING_LOAD_DONE]', {
          tab: currentTab,
          durationMs: Math.round(performance.now() - startedAt),
          loadedCount: results.filter((item) => item.status === 'fulfilled').length,
          failedCount: results.filter((item) => item.status === 'rejected').length
        });
      } catch (error) {
        if (!cancelled) {
          const message = error instanceof Error ? error.message : 'Publishing-Daten konnten nicht geladen werden.';
          setStatus(message);
          console.error('[PUBLISHING_LOAD_ERROR]', {
            tab: currentTab,
            errorMessage: message
          });
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    async function loadQueuePreview() {
      if (currentTab !== '/publishing') {
        return;
      }

      setQueueLoading(true);

      try {
        const data = await apiFetch('/api/publishing/queue?limit=18', {}, { timeoutMs: 3000 });
        if (!cancelled) {
          setQueue(data?.items || []);
        }
      } catch (error) {
        if (!cancelled) {
          const message = error instanceof Error ? error.message : 'Queue konnte nicht geladen werden.';
          setStatus((current) => current || message);
          console.error('[PUBLISHING_LOAD_ERROR]', {
            tab: currentTab,
            errorMessage: message,
            section: 'queue'
          });
        }
      } finally {
        if (!cancelled) {
          setQueueLoading(false);
        }
      }
    }

    void loadCurrentTab();
    void loadQueuePreview();

    return () => {
      cancelled = true;
    };
  }, [currentTab, isAdmin, reloadKey, user?.role]);

  const queueSummary = useMemo(
    () =>
      (workerStatus?.channels || []).reduce(
        (summary, item) => ({
          pending: summary.pending + Number(item.pending ?? item.waiting ?? 0),
          sending: summary.sending + Number(item.sending ?? item.processing ?? 0),
          sent: summary.sent + Number(item.sent ?? item.posted ?? 0),
          retry: summary.retry + Number(item.retry || 0),
          failed: summary.failed + Number(item.failed || 0)
        }),
        { pending: 0, sending: 0, sent: 0, retry: 0, failed: 0 }
      ),
    [workerStatus]
  );

  const summaryCards = useMemo(
    () => [
      {
        title: 'Queue offen',
        value: `${queueSummary.pending + queueSummary.sending + queueSummary.retry}`,
        detail: `Pending ${queueSummary.pending} | Sending ${queueSummary.sending} | Retry ${queueSummary.retry}`,
        tone: queueSummary.failed > 0 ? 'warning' : queueSummary.pending + queueSummary.sending + queueSummary.retry > 0 ? 'info' : 'success'
      },
      {
        title: 'Telegram Bot',
        value: workerStatus?.telegramBot?.publishTargets || 0,
        detail: workerStatus?.telegramBot?.tokenConfigured ? 'Ziele aktiv' : 'Token fehlt',
        tone: workerStatus?.telegramBot?.tokenConfigured ? 'success' : 'warning'
      },
      {
        title: 'WhatsApp',
        value: getWhatsappConnectionLabel(workerStatus?.whatsapp?.connectionStatus),
        detail: `${getWhatsappWorkerLabel(workerStatus?.whatsapp?.workerStatus)} | Queue ${workerStatus?.whatsapp?.queue?.open || 0}`,
        tone: getStatusTone(workerStatus?.whatsapp?.healthStatus || workerStatus?.whatsapp?.connectionStatus)
      },
      {
        title: 'Facebook Worker',
        value: workerStatus?.facebook?.enabled ? 'Aktiv' : 'Aus',
        detail: workerStatus?.facebook?.defaultTarget || 'Kein Default-Ziel',
        tone: workerStatus?.facebook?.enabled ? 'info' : 'warning'
      }
    ],
    [queueSummary, workerStatus]
  );

  async function runWorkers(channelType) {
    if (!isAdmin) {
      return;
    }

    try {
      const data = await apiFetch('/api/publishing/workers/run', {
        method: 'POST',
        body: JSON.stringify({ channelType: channelType || null })
      });
      setStatus(`${data.items?.length || 0} Worker-Aufgaben verarbeitet.`);
      setReloadKey((current) => current + 1);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Worker konnten nicht gestartet werden.');
    }
  }

  async function retryQueue(id) {
    try {
      await apiFetch(`/api/publishing/queue/${id}/retry`, {
        method: 'POST',
        body: JSON.stringify({})
      });
      setStatus('Queue-Eintrag fuer Retry markiert.');
      setReloadKey((current) => current + 1);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Queue-Retry fehlgeschlagen.');
    }
  }

  async function saveFacebookWorker() {
    if (!isAdmin) {
      return;
    }

    try {
      await apiFetch('/api/publishing/facebook-worker', {
        method: 'PUT',
        body: JSON.stringify(facebookSettings)
      });
      setStatus('Facebook Worker gespeichert.');
      setReloadKey((current) => current + 1);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Facebook Worker konnte nicht gespeichert werden.');
    }
  }

  function addTelegramTarget() {
    setTelegramBotConfig((prev) => ({
      ...prev,
      targets: [
        ...(prev.targets || []),
        {
          id: `new-${Date.now()}-${prev.targets?.length || 0}`,
          name: '',
          chatId: '',
          isActive: false,
          useForPublishing: false,
          targetKind: 'custom',
          isSystem: false,
          requiresManualActivation: false,
          lastDeliveryStatus: 'never',
          lastSentAt: null,
          lastError: '',
          lastErrorAt: null
        }
      ]
    }));
  }

  function updateTelegramTarget(index, patch) {
    setTelegramBotConfig((prev) => ({
      ...prev,
      targets: (prev.targets || []).map((target, targetIndex) => (targetIndex === index ? { ...target, ...patch } : target))
    }));
  }

  function removeTelegramTarget(index) {
    setTelegramBotConfig((prev) => ({
      ...prev,
      targets: (prev.targets || []).filter((_, targetIndex) => targetIndex !== index)
    }));
  }

  function addWhatsappTarget() {
    setWhatsappTargetConfig((prev) => ({
      ...prev,
      targets: [
        ...(prev.targets || []),
        {
          id: `new-${Date.now()}-${prev.targets?.length || 0}`,
          name: '',
          targetRef: '',
          targetLabel: '',
          targetType: 'WHATSAPP_CHANNEL',
          channelUrl: '',
          isActive: false,
          useForPublishing: false,
          isSystem: false,
          requiresManualActivation: false,
          lastSentAt: null,
          lastError: '',
          lastErrorAt: null,
          lastDeliveryStatus: 'idle',
          lastTestedAt: null,
          sortOrder: 100
        }
      ]
    }));
  }

  function updateWhatsappTarget(index, patch) {
    setWhatsappTargetConfig((prev) => ({
      ...prev,
      targets: (prev.targets || []).map((target, targetIndex) => (targetIndex === index ? { ...target, ...patch } : target))
    }));
  }

  function removeWhatsappTarget(index) {
    setWhatsappTargetConfig((prev) => ({
      ...prev,
      targets: (prev.targets || []).filter((_, targetIndex) => targetIndex !== index)
    }));
  }

  async function saveTelegramBotClient() {
    if (!isAdmin) {
      return;
    }

    try {
      await apiFetch('/api/publishing/telegram-bot-client', {
        method: 'PUT',
        body: JSON.stringify({
          enabled: telegramBotConfig.enabled,
          defaultRetryLimit: telegramBotConfig.defaultRetryLimit,
          targets: (telegramBotConfig.targets || []).map((target) => ({
            id: typeof target.id === 'number' ? target.id : undefined,
            name: target.name,
            chatId: target.chatId,
            isActive: target.isActive,
            useForPublishing: target.useForPublishing,
            targetKind: target.targetKind || 'custom',
            isSystem: target.isSystem === true,
            requiresManualActivation: target.requiresManualActivation === true,
            lastDeliveryStatus: target.lastDeliveryStatus || 'never',
            lastSentAt: target.lastSentAt || null,
            lastError: target.lastError || ''
          }))
        })
      });
      setStatus('Telegram Bot Client gespeichert.');
      setReloadKey((current) => current + 1);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Telegram Bot Client konnte nicht gespeichert werden.');
    }
  }

  async function testTelegramTarget(target) {
    if (!isAdmin || !target?.id) {
      return;
    }

    setTestingTelegramTargetId(target.id);
    setStatus('');

    try {
      const data = await apiFetch(`/api/publishing/telegram-bot-client/targets/${target.id}/test`, {
        method: 'POST',
        body: JSON.stringify({})
      });
      const delivery = data?.item || null;
      setStatus(
        delivery?.messageId
          ? `Testversand erfolgreich: ${target.name} (${delivery.messageId}).`
          : `Testversand erfolgreich: ${target.name}.`
      );
      setReloadKey((current) => current + 1);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Telegram-Testversand fehlgeschlagen.');
      setReloadKey((current) => current + 1);
    } finally {
      setTestingTelegramTargetId(null);
    }
  }

  async function saveWhatsappTargets() {
    if (!isAdmin) {
      return;
    }

    try {
      await apiFetch('/api/publishing/whatsapp-output-targets', {
        method: 'PUT',
        body: JSON.stringify({
          targets: (whatsappTargetConfig.targets || []).map((target) => ({
            id: typeof target.id === 'number' ? target.id : undefined,
            name: target.name,
            targetRef: target.targetRef,
            targetLabel: target.targetLabel,
            targetType: target.targetType,
            channelUrl: target.channelUrl,
            isActive: target.isActive,
            useForPublishing: target.useForPublishing,
            isSystem: target.isSystem,
            requiresManualActivation: target.requiresManualActivation,
            lastSentAt: target.lastSentAt || null,
            lastError: target.lastError || '',
            lastErrorAt: target.lastErrorAt || null,
            lastDeliveryStatus: target.lastDeliveryStatus || 'idle',
            lastTestedAt: target.lastTestedAt || null,
            sortOrder: target.sortOrder || 100
          }))
        })
      });
      setStatus('WhatsApp Zielgruppen gespeichert.');
      setReloadKey((current) => current + 1);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'WhatsApp Zielgruppen konnten nicht gespeichert werden.');
    }
  }

  async function saveWhatsappRuntime() {
    if (!isAdmin) {
      return;
    }

    setWhatsappRuntimeBusy('save_settings');

    try {
      await apiFetch('/api/publishing/whatsapp-runtime/settings', {
        method: 'PUT',
        body: JSON.stringify({
          workerEnabled: whatsappClientConfig.workerEnabled,
          alertsEnabled: whatsappClientConfig.alertsEnabled,
          alertTargetRef: whatsappClientConfig.alertTargetRef,
          sendCooldownMs: whatsappClientConfig.sendCooldownMs
        })
      });
      setStatus('WhatsApp Runtime gespeichert.');
      setReloadKey((current) => current + 1);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'WhatsApp Runtime konnte nicht gespeichert werden.');
    } finally {
      setWhatsappRuntimeBusy('');
    }
  }

  async function runWhatsappRuntimeAction(action, payload = {}, successMessage = 'WhatsApp Aktion ausgefuehrt.') {
    if (!isAdmin) {
      return;
    }

    setWhatsappRuntimeBusy(action);
    setStatus('');

    try {
      let timeoutMs = 10000;
      if (action === 'connect' || action === 'manual_channel_debug_wait') {
        timeoutMs = 130000;
      } else if (action === 'channel_debug' || action === 'test_channel') {
        timeoutMs = 45000;
      } else if (action === 'manual_channel_debug_capture' || action === 'reset_session' || action === 'start_worker') {
        timeoutMs = 20000;
      }
      const data = await apiFetch(`/api/publishing/whatsapp-runtime/actions/${action}`, {
        method: 'POST',
        body: JSON.stringify(payload)
      }, { timeoutMs });
      setStatus(successMessage);
      if (data?.item?.runtime) {
        setWhatsappClientConfig((prev) => ({
          ...prev,
          ...(data.item.runtime || {})
        }));
      }
      setReloadKey((current) => current + 1);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'WhatsApp Aktion fehlgeschlagen.');
      setReloadKey((current) => current + 1);
    } finally {
      setWhatsappRuntimeBusy('');
    }
  }

  async function resetWhatsappSession() {
    if (!isAdmin) {
      return;
    }

    if (typeof window !== 'undefined' && !window.confirm('WhatsApp Session wirklich zuruecksetzen? Das Browser-Profil wird neu aufgebaut und ein neuer QR Login wird noetig.')) {
      return;
    }

    await runWhatsappRuntimeAction(
      'reset_session',
      {},
      'WhatsApp Session wurde zurueckgesetzt. Ein neuer QR Login kann gestartet werden.'
    );
  }

  async function testWhatsappTarget(target) {
    if (!isAdmin || !target?.id) {
      return;
    }

    setTestingWhatsappTargetId(target.id);
    setStatus('');

    try {
      await apiFetch(
        '/api/publishing/whatsapp-runtime/actions/test_channel',
        {
          method: 'POST',
          body: JSON.stringify({
            targetId: target.id,
            targetRef: target.targetRef,
            targetLabel: target.targetLabel || target.name,
            channelUrl: target.channelUrl || target.targetRef
          })
        },
        { timeoutMs: 45000 }
      );
      setStatus(`WhatsApp Kanaltest erfolgreich: ${target.name || target.targetLabel || target.targetRef}.`);
      setReloadKey((current) => current + 1);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'WhatsApp Kanaltest fehlgeschlagen.');
      setReloadKey((current) => current + 1);
    } finally {
      setTestingWhatsappTargetId(null);
    }
  }

  const preferredWhatsappDebugTarget =
    (whatsappTargetConfig.targets || []).find((target) => getWhatsappTargetKind(target) === 'test') ||
    (whatsappTargetConfig.targets || [])[0] ||
    null;

  async function sendWhatsappTargetTestPost(target) {
    if (!isAdmin || !target?.id) {
      return;
    }

    setSendingWhatsappTargetId(target.id);
    setStatus('');

    try {
      const data = await apiFetch(`/api/publishing/whatsapp-output-targets/${target.id}/test-post`, {
        method: 'POST',
        body: JSON.stringify({})
      }, { timeoutMs: 15000 });
      const result = data?.item?.results?.[0] || {};
      if (result?.status === 'sent') {
        setStatus(`WhatsApp Testpost gesendet: ${target.name || target.targetLabel || target.targetRef}.`);
      } else if (result?.status === 'skipped') {
        setStatus(result?.reason || `WhatsApp Testpost uebersprungen: ${target.name || target.targetLabel || target.targetRef}.`);
      } else {
        setStatus(`WhatsApp Testpost verarbeitet: ${target.name || target.targetLabel || target.targetRef}.`);
      }
      setReloadKey((current) => current + 1);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'WhatsApp Testpost fehlgeschlagen.');
      setReloadKey((current) => current + 1);
    } finally {
      setSendingWhatsappTargetId(null);
    }
  }

  function renderQueueTab() {
    return (
      <section className="card publishing-panel">
        <div className="publishing-panel-header">
          <div>
            <p className="section-title">Queue</p>
            <h2 className="page-title">Offene Jobs und Retry-Faelle</h2>
          </div>
          {isAdmin ? (
            <button type="button" className="primary" onClick={() => void runWorkers()}>
              Alle Worker starten
            </button>
          ) : null}
        </div>
        {queue.length ? (
          <div className="publishing-feed">
            {queue.map((item) => (
              <article key={item.id} className="publishing-feed-item">
                <div className="publishing-item-head">
                  <strong>{item.payload?.title || `Queue ${item.id}`}</strong>
                  <span className={`status-chip ${getStatusTone(item.status)}`}>{item.status}</span>
                </div>
                <p>
                  {item.source_type} | Retry {item.retry_count} | erstellt {formatDateTime(item.created_at)}
                </p>
                <p>
                  {(item.targets || [])
                    .map((target) => {
                      const targetLabel = target.target_label ? `/${target.target_label}` : '';
                      return `${target.channel_type}${targetLabel}:${target.status}`;
                    })
                    .join(' | ') || 'Keine Targets'}
                </p>
                {isAdmin ? (
                  <button type="button" className="secondary" onClick={() => void retryQueue(item.id)}>
                    Erneut senden
                  </button>
                ) : null}
              </article>
            ))}
          </div>
        ) : queueLoading ? (
          <p className="publishing-empty">Queue wird nachgeladen...</p>
        ) : (
          <p className="publishing-empty">Keine Daten vorhanden</p>
        )}
      </section>
    );
  }

  function renderWorkersTab() {
    return (
      <section className="card publishing-panel">
        <div className="publishing-panel-header">
          <div>
            <p className="section-title">Worker Status</p>
            <h2 className="page-title">Dispatcher und Kanal-Worker</h2>
          </div>
          <span className="publishing-note">Nur Statusdaten, keine schweren Listen</span>
        </div>
        <div className="publishing-grid">
          {(workerStatus?.channels || []).map((item) => (
            <article key={item.channel_type} className="publishing-stat-card">
              <div className="publishing-item-head">
                <strong>{item.channel_type}</strong>
                {isAdmin ? (
                  <button type="button" className="secondary" onClick={() => void runWorkers(item.channel_type)}>
                    Starten
                  </button>
                ) : null}
              </div>
              <p>pending {item.pending ?? item.waiting ?? 0}</p>
              <p>sending {item.sending ?? item.processing ?? 0}</p>
              <p>retry {item.retry || 0}</p>
              <p>sent {item.sent ?? item.posted ?? 0}</p>
              <p>failed {item.failed || 0}</p>
            </article>
          ))}
          <article className="publishing-stat-card">
            <strong>Telegram Bot Client</strong>
            <p>{workerStatus?.telegramBot?.publishTargets || 0} aktive Ziele</p>
            <p>Retry Limit {workerStatus?.telegramBot?.retryLimit || 0}</p>
            <p>Token {workerStatus?.telegramBot?.tokenConfigured ? 'vorhanden' : 'fehlt'}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>WhatsApp Output</strong>
            <p>{getWhatsappConnectionLabel(workerStatus?.whatsapp?.connectionStatus)}</p>
            <p>{getWhatsappWorkerLabel(workerStatus?.whatsapp?.workerStatus)}</p>
            <p>Health {workerStatus?.whatsapp?.healthStatus || '-'}</p>
            <p>Queue offen {workerStatus?.whatsapp?.queue?.open || 0}</p>
          </article>
        </div>
      </section>
    );
  }

  function renderTelegramTab() {
    const liveTargetCount = (telegramBotConfig.targets || []).filter((target) => target.targetKind === 'live').length;
    const envFallbackActive = telegramBotConfig.fallbackChatConfigured && !(telegramBotConfig.targets || []).length;

    return (
      <section className="card publishing-panel">
        <div className="publishing-panel-header">
          <div>
            <p className="section-title">Telegram Bot</p>
            <h2 className="page-title">Output-Ziele, Live-Schutz und Testversand</h2>
          </div>
          {isAdmin ? (
            <button type="button" className="secondary" onClick={addTelegramTarget}>
              Zielgruppe hinzufuegen
            </button>
          ) : null}
        </div>

        {!isAdmin ? <p className="publishing-empty">Nur Admin kann den Telegram Bot Client konfigurieren.</p> : null}

        <label className="checkbox-card">
          <span>Telegram Output aktiv</span>
          <input
            type="checkbox"
            checked={telegramBotConfig.enabled}
            disabled={!isAdmin}
            onChange={(event) => setTelegramBotConfig((prev) => ({ ...prev, enabled: event.target.checked }))}
          />
        </label>

        <div className="form-row">
          <input
            type="number"
            min="0"
            value={telegramBotConfig.defaultRetryLimit}
            disabled={!isAdmin}
            placeholder="Retry Limit"
            onChange={(event) =>
              setTelegramBotConfig((prev) => ({
                ...prev,
                defaultRetryLimit: Number(event.target.value || 0)
              }))
            }
          />
        </div>

        <div className="publishing-grid">
          <article className="publishing-stat-card">
            <strong>Bot Token</strong>
            <p>{telegramBotConfig.tokenConfigured ? 'vorhanden' : 'fehlt'}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>ENV Fallback</strong>
            <p>
              {envFallbackActive
                ? 'aktiv, weil noch keine persistente Zielgruppe vorhanden ist'
                : telegramBotConfig.fallbackChatConfigured
                  ? 'vorhanden, aber persistente Ziele haben Vorrang'
                  : 'kein ENV Chat gesetzt'}
            </p>
          </article>
          <article className="publishing-stat-card">
            <strong>Aktive Publisher-Ziele</strong>
            <p>{telegramBotConfig.effectiveTargets?.length || 0} Ziele werden angesteuert.</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Live Kanaele</strong>
            <p>{liveTargetCount} kritisch markierte Zielgruppe(n)</p>
          </article>
        </div>

        {(telegramBotConfig.targets || []).length ? (
          <div className="publishing-feed">
            {(telegramBotConfig.targets || []).map((target, index) => (
              <article
                key={target.id || index}
                className={`publishing-feed-item publishing-target-card ${target.targetKind === 'live' ? 'publishing-target-live' : ''}`}
              >
                <div className="publishing-item-head">
                  <div className="publishing-target-heading">
                    <strong>{target.name || `Telegram Ziel ${index + 1}`}</strong>
                    <p className="publishing-target-ref">{target.chatId || 'Chat ID fehlt'}</p>
                  </div>
                  <div className="publishing-target-chip-row">
                    <span className={`status-chip ${target.targetKind === 'live' ? 'danger' : 'info'}`}>
                      {getTelegramChannelKindLabel(target.targetKind)}
                    </span>
                    <span className={`status-chip ${getTelegramTargetStatus(target).tone}`}>
                      {getTelegramTargetStatus(target).label}
                    </span>
                  </div>
                </div>

                {target.targetKind === 'live' ? (
                  <div className="publishing-live-warning">
                    <strong>LIVE KANAL</strong>
                    <span>{target.isActive ? 'Aktiv' : 'Deaktiviert'}</span>
                    <p>{target.isActive ? 'Live-Sendung ist bewusst freigegeben.' : 'Erst manuell aktivieren.'}</p>
                  </div>
                ) : null}

                <div className="publishing-target-audit">
                  <p>
                    <strong>Letzte Sendung:</strong> {formatDateTime(target.lastSentAt)}
                  </p>
                  <p>
                    <strong>Letzter Fehler:</strong>{' '}
                    {target.lastError ? `${formatDateTime(target.lastErrorAt)} | ${target.lastError}` : '-'}
                  </p>
                </div>

                <div className="form-row">
                  <input
                    value={target.name || ''}
                    disabled={!isAdmin}
                    placeholder="Name der Zielgruppe"
                    onChange={(event) => updateTelegramTarget(index, { name: event.target.value })}
                  />
                  <input
                    value={target.chatId || ''}
                    disabled={!isAdmin}
                    placeholder="Chat ID / Channel ID"
                    onChange={(event) => updateTelegramTarget(index, { chatId: event.target.value })}
                  />
                  <select
                    value={target.targetKind || 'custom'}
                    disabled={!isAdmin}
                    onChange={(event) => {
                      const nextTargetKind = event.target.value;
                      updateTelegramTarget(index, {
                        targetKind: nextTargetKind,
                        requiresManualActivation: nextTargetKind === 'live',
                        ...(nextTargetKind === 'live'
                          ? {
                              isActive: false,
                              useForPublishing: false
                            }
                          : {})
                      });
                    }}
                  >
                    <option value="custom">Standard</option>
                    <option value="test">Testkanal</option>
                    <option value="live">Live-Kanal</option>
                  </select>
                </div>
                <div className="publishing-split">
                  <label className="checkbox-card">
                    <span>Ziel aktiv</span>
                    <input
                      type="checkbox"
                      checked={Boolean(target.isActive)}
                      disabled={!isAdmin}
                      onChange={(event) => updateTelegramTarget(index, { isActive: event.target.checked })}
                    />
                  </label>
                  <label className="checkbox-card">
                    <span>Fuer Publishing verwenden</span>
                    <input
                      type="checkbox"
                      checked={Boolean(target.useForPublishing)}
                      disabled={!isAdmin}
                      onChange={(event) => updateTelegramTarget(index, { useForPublishing: event.target.checked })}
                    />
                  </label>
                </div>
                {isAdmin ? (
                  <div className="publishing-target-actions">
                    <button
                      type="button"
                      className="secondary"
                      onClick={() => void testTelegramTarget(target)}
                      disabled={
                        testingTelegramTargetId === target.id ||
                        typeof target.id !== 'number' ||
                        !target.isActive ||
                        !target.useForPublishing
                      }
                    >
                      {testingTelegramTargetId === target.id ? 'Teste...' : 'Test senden'}
                    </button>
                    <button type="button" className="secondary" onClick={() => removeTelegramTarget(index)}>
                      Zielgruppe entfernen
                    </button>
                  </div>
                ) : null}
              </article>
            ))}
          </div>
        ) : (
          <p className="publishing-empty">Keine persistenten Telegram-Zielgruppen gespeichert.</p>
        )}

        {isAdmin ? (
          <button type="button" className="primary" onClick={() => void saveTelegramBotClient()}>
            Speichern
          </button>
        ) : null}
      </section>
    );
  }

  function renderWhatsappTab() {
    const recentQueueCount = whatsappRecentQueue.length;
    const recentLogCount = whatsappRecentLogs.length;

    return (
      <section className="card publishing-panel">
        <div className="publishing-panel-header">
          <div>
            <p className="section-title">WhatsApp Output</p>
            <h2 className="page-title">Session, Worker, Retry und Output-Steuerung ohne zweite UI</h2>
          </div>
          {isAdmin ? (
            <button type="button" className="secondary" onClick={addWhatsappTarget}>
              Gruppe hinzufuegen
            </button>
          ) : null}
        </div>
        <div className="publishing-grid">
          <article className="publishing-stat-card">
            <strong>Verbindung</strong>
            <p>{getWhatsappConnectionLabel(whatsappClientConfig.connectionStatus)}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Worker</strong>
            <p>{getWhatsappWorkerLabel(whatsappClientConfig.workerStatus)}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Health</strong>
            <p>{whatsappClientConfig.healthStatus || '-'}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Provider</strong>
            <p>{whatsappClientConfig.providerLabel || 'Playwright Worker'}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Session</strong>
            <p>{whatsappClientConfig.sessionValid ? 'gueltig' : whatsappClientConfig.qrRequired ? 'QR erforderlich' : 'ungueltig'}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Letzte Verbindung</strong>
            <p>{formatDateTime(whatsappClientConfig.lastConnectedAt)}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Letzter Post</strong>
            <p>{formatDateTime(whatsappClientConfig.lastSuccessfulPostAt)}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Letzter Fehler</strong>
            <p>{whatsappClientConfig.lastError ? `${formatDateTime(whatsappClientConfig.lastErrorAt)} | ${whatsappClientConfig.lastError}` : '-'}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Letzter Neustart</strong>
            <p>{formatDateTime(whatsappClientConfig.lastRestartAt)}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Queue</strong>
            <p>{whatsappClientConfig.queue?.open || 0} offen | Retry {whatsappClientConfig.queue?.retry || 0}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Letzter Check</strong>
            <p>{formatDateTime(whatsappClientConfig.lastHealthCheckAt)}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Profil</strong>
            <p>{whatsappClientConfig.profileWritable ? 'persistent beschreibbar' : 'nicht bestaetigt'}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Channel Navigation</strong>
            <p>{getWhatsappChannelNavigationLabel(whatsappClientConfig.channelNavigationStatus)}</p>
          </article>
          <article className="publishing-stat-card">
            <strong>Admin-Rechte</strong>
            <p>{getWhatsappAdminStatusLabel(whatsappClientConfig.channelAdminStatus)}</p>
          </article>
        </div>

        {!isAdmin ? <p className="publishing-empty">Nur Admin kann WhatsApp Zielgruppen verwalten.</p> : null}

        {isAdmin ? (
          <section className="publishing-grid">
            <article className="publishing-feed-item publishing-target-card">
              <div className="publishing-item-head">
                <div>
                  <strong>Runtime & Alerts</strong>
                  <p className="publishing-target-ref">
                    {`${whatsappClientConfig.providerLabel || 'Playwright Worker'} | ${
                      whatsappClientConfig.browserChannel || 'Chromium'
                    } | Session, Worker, Cooldown und Telegram-Stoerungsmeldungen`}
                  </p>
                  <p className="publishing-target-ref">
                    {whatsappClientConfig.browserProfileDir
                      ? `Profil: ${whatsappClientConfig.browserProfileDir}`
                      : 'Profilpfad wird nach dem Worker-Start angezeigt'}
                  </p>
                </div>
                <div className="publishing-target-chip-row">
                  <span className="status-chip info">{whatsappClientConfig.alertTargetType || 'SYSTEM_ALERT_CHANNEL'}</span>
                  <span className={`status-chip ${getStatusTone(whatsappClientConfig.healthStatus)}`}>
                    {whatsappClientConfig.healthStatus || '-'}
                  </span>
                </div>
              </div>

              <div className="publishing-split">
                <label className="checkbox-card">
                  <span>Worker aktiv</span>
                  <input
                    type="checkbox"
                    checked={Boolean(whatsappClientConfig.workerEnabled)}
                    onChange={(event) =>
                      setWhatsappClientConfig((prev) => ({
                        ...prev,
                        workerEnabled: event.target.checked
                      }))
                    }
                  />
                </label>
                <label className="checkbox-card">
                  <span>Telegram Alerts aktiv</span>
                  <input
                    type="checkbox"
                    checked={Boolean(whatsappClientConfig.alertsEnabled)}
                    onChange={(event) =>
                      setWhatsappClientConfig((prev) => ({
                        ...prev,
                        alertsEnabled: event.target.checked
                      }))
                    }
                  />
                </label>
              </div>

              <div className="form-row">
                <input
                  value={whatsappClientConfig.alertTargetRef || ''}
                  placeholder="Telegram Alert Kanal Username"
                  onChange={(event) =>
                    setWhatsappClientConfig((prev) => ({
                      ...prev,
                      alertTargetRef: event.target.value
                    }))
                  }
                />
                <input
                  type="number"
                  min="500"
                  step="500"
                  value={whatsappClientConfig.sendCooldownMs || 4000}
                  placeholder="Cooldown in ms"
                  onChange={(event) =>
                    setWhatsappClientConfig((prev) => ({
                      ...prev,
                      sendCooldownMs: Number(event.target.value || 4000)
                    }))
                  }
                />
              </div>

              <div className="publishing-target-audit">
                <p>
                  <strong>Worker PID:</strong> {whatsappClientConfig.workerPid || '-'}
                </p>
                <p>
                  <strong>Login Monitor:</strong>{' '}
                  {whatsappClientConfig.loginMonitorActive
                    ? `wartet auf QR Scan (${Math.round((whatsappClientConfig.loginTimeoutMs || 120000) / 1000)}s)`
                    : 'inaktiv'}
                </p>
                <p>
                  <strong>Session gespeichert:</strong> {formatDateTime(whatsappClientConfig.sessionSavedAt)}
                </p>
                <p>
                  <strong>Session Reset:</strong> {formatDateTime(whatsappClientConfig.sessionResetAt)}
                </p>
                <p>
                  <strong>Profil-Backup:</strong> {whatsappClientConfig.profileBackupDir || '-'}
                </p>
                <p>
                  <strong>Navigation:</strong> {getWhatsappChannelNavigationLabel(whatsappClientConfig.channelNavigationStatus)}
                </p>
                <p>
                  <strong>Admin:</strong> {getWhatsappAdminStatusLabel(whatsappClientConfig.channelAdminStatus)}
                </p>
                <p>
                  <strong>Debug-Ziel:</strong> {whatsappClientConfig.lastChannelTargetRef || '-'}
                </p>
                <p>
                  <strong>Composer-Selector:</strong> {whatsappClientConfig.lastChannelPreferredSelector || '-'}
                </p>
                <p>
                  <strong>Composer-Kandidaten:</strong>{' '}
                  {Array.isArray(whatsappClientConfig.lastChannelComposerCandidates)
                    ? whatsappClientConfig.lastChannelComposerCandidates.length
                    : 0}
                </p>
                <p>
                  <strong>Letzter Debug:</strong>{' '}
                  {whatsappClientConfig.lastChannelDebugMessage
                    ? `${formatDateTime(whatsappClientConfig.lastChannelDebugAt)} | ${whatsappClientConfig.lastChannelDebugMessage}`
                    : '-'}
                </p>
                <p>
                  <strong>Aktuelle URL:</strong> {whatsappClientConfig.currentUrl || '-'}
                </p>
                <p>
                  <strong>Screenshot:</strong> {whatsappClientConfig.lastChannelScreenshotPath || '-'}
                </p>
                <p>
                  <strong>DOM JSON:</strong> {whatsappClientConfig.lastChannelDomSnapshotPath || '-'}
                </p>
                <p>
                  <strong>HTML Snapshot:</strong> {whatsappClientConfig.lastChannelHtmlSnapshotPath || '-'}
                </p>
              </div>

              {whatsappClientConfig.lastChannelDebugArtifacts?.length ? (
                <div className="publishing-target-audit">
                  <p>
                    <strong>Debug-Artefakte:</strong>
                  </p>
                  {whatsappClientConfig.lastChannelDebugArtifacts.map((artifact, index) => (
                    <p key={`${artifact.step || 'artifact'}-${index}`}>
                      {(artifact.step || `Schritt ${index + 1}`) + ': '}
                      {artifact.screenshotPath || artifact.jsonPath || '-'}
                    </p>
                  ))}
                </div>
              ) : null}

              <div className="publishing-target-actions">
                <button
                  type="button"
                  className="secondary"
                  onClick={() => void runWhatsappRuntimeAction('connect', {}, 'WhatsApp Verbindung angefordert.')}
                  disabled={whatsappRuntimeBusy === 'connect'}
                >
                  {whatsappRuntimeBusy === 'connect' ? 'Verbinde...' : 'WhatsApp verbinden'}
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() =>
                    void runWhatsappRuntimeAction('refresh_session', {}, 'WhatsApp Session wird erneuert.')
                  }
                  disabled={whatsappRuntimeBusy === 'refresh_session'}
                >
                  {whatsappRuntimeBusy === 'refresh_session' ? 'Aktualisiere...' : 'Session erneuern'}
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() =>
                    void runWhatsappRuntimeAction('test_connection', {}, 'WhatsApp Verbindungstest abgeschlossen.')
                  }
                  disabled={whatsappRuntimeBusy === 'test_connection'}
                >
                  {whatsappRuntimeBusy === 'test_connection' ? 'Teste...' : 'Verbindung testen'}
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() => void runWhatsappRuntimeAction('health_check', {}, 'WhatsApp Health Check abgeschlossen.')}
                  disabled={whatsappRuntimeBusy === 'health_check'}
                >
                  {whatsappRuntimeBusy === 'health_check' ? 'Pruefe...' : 'Health Check'}
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() => void runWhatsappRuntimeAction('start_worker', {}, 'WhatsApp Worker gestartet.')}
                  disabled={whatsappRuntimeBusy === 'start_worker'}
                >
                  {whatsappRuntimeBusy === 'start_worker' ? 'Starte...' : 'Worker starten'}
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() => void runWhatsappRuntimeAction('stop_worker', {}, 'WhatsApp Worker gestoppt.')}
                  disabled={whatsappRuntimeBusy === 'stop_worker'}
                >
                  {whatsappRuntimeBusy === 'stop_worker' ? 'Stoppe...' : 'Worker stoppen'}
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() => void runWhatsappRuntimeAction('alert_test', {}, 'Telegram Alert Test gesendet.')}
                  disabled={whatsappRuntimeBusy === 'alert_test'}
                >
                  {whatsappRuntimeBusy === 'alert_test' ? 'Sende...' : 'Telegram Alert testen'}
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() =>
                    void runWhatsappRuntimeAction(
                      'channel_debug',
                      preferredWhatsappDebugTarget
                        ? {
                            targetId: preferredWhatsappDebugTarget.id,
                            targetRef: preferredWhatsappDebugTarget.targetRef,
                            targetLabel: preferredWhatsappDebugTarget.targetLabel || preferredWhatsappDebugTarget.name,
                            channelUrl: preferredWhatsappDebugTarget.channelUrl || preferredWhatsappDebugTarget.targetRef
                          }
                        : {},
                      'WhatsApp Channel Debug abgeschlossen.'
                    )
                  }
                  disabled={whatsappRuntimeBusy === 'channel_debug' || !preferredWhatsappDebugTarget}
                >
                  {whatsappRuntimeBusy === 'channel_debug' ? 'Debuggt...' : 'WhatsApp Channel Debug starten'}
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() =>
                    void runWhatsappRuntimeAction(
                      'manual_channel_debug_wait',
                      preferredWhatsappDebugTarget
                        ? {
                            targetId: preferredWhatsappDebugTarget.id,
                            targetRef: preferredWhatsappDebugTarget.targetRef,
                            targetLabel: preferredWhatsappDebugTarget.targetLabel || preferredWhatsappDebugTarget.name,
                            channelUrl: preferredWhatsappDebugTarget.channelUrl || preferredWhatsappDebugTarget.targetRef,
                            waitTimeoutMs: 120000,
                            pollIntervalMs: 1500
                          }
                        : {},
                      'Manueller WhatsApp Channel Debug abgeschlossen.'
                    )
                  }
                  disabled={whatsappRuntimeBusy === 'manual_channel_debug_wait' || !preferredWhatsappDebugTarget}
                >
                  {whatsappRuntimeBusy === 'manual_channel_debug_wait'
                    ? 'Warte auf Kanal...'
                    : 'Manuellen Channel Debug starten'}
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() =>
                    void runWhatsappRuntimeAction(
                      'manual_channel_debug_capture',
                      preferredWhatsappDebugTarget
                        ? {
                            targetId: preferredWhatsappDebugTarget.id,
                            targetRef: preferredWhatsappDebugTarget.targetRef,
                            targetLabel: preferredWhatsappDebugTarget.targetLabel || preferredWhatsappDebugTarget.name,
                            channelUrl: preferredWhatsappDebugTarget.channelUrl || preferredWhatsappDebugTarget.targetRef
                          }
                        : {},
                      'Aktueller WhatsApp Kanalzustand analysiert.'
                    )
                  }
                  disabled={whatsappRuntimeBusy === 'manual_channel_debug_capture' || !preferredWhatsappDebugTarget}
                >
                  {whatsappRuntimeBusy === 'manual_channel_debug_capture'
                    ? 'Analysiere...'
                    : 'Aktuellen Kanal analysieren'}
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() => void resetWhatsappSession()}
                  disabled={whatsappRuntimeBusy === 'reset_session'}
                >
                  {whatsappRuntimeBusy === 'reset_session' ? 'Setze zurueck...' : 'WhatsApp Session zuruecksetzen'}
                </button>
                <button
                  type="button"
                  className="primary"
                  onClick={() => void saveWhatsappRuntime()}
                  disabled={whatsappRuntimeBusy === 'save_settings'}
                >
                  {whatsappRuntimeBusy === 'save_settings' ? 'Speichert...' : 'Runtime speichern'}
                </button>
              </div>
            </article>

            <article className="publishing-feed-item publishing-target-card">
              <div className="publishing-item-head">
                <div>
                  <strong>QR Login</strong>
                  <p className="publishing-target-ref">Wird nur angezeigt, wenn der Provider einen QR liefert</p>
                </div>
                <span className={`status-chip ${getStatusTone(getWhatsappConnectionLabel(whatsappClientConfig.connectionStatus))}`}>
                  {getWhatsappConnectionLabel(whatsappClientConfig.connectionStatus)}
                </span>
              </div>
              {whatsappClientConfig.qrCodeDataUrl ? (
                <div className="publishing-qr-shell">
                  <img className="publishing-qr-image" src={whatsappClientConfig.qrCodeDataUrl} alt="WhatsApp QR Login" />
                </div>
              ) : (
                <p className="publishing-empty">
                  Kein QR gespeichert. Wenn die Session ein neues Login braucht, erscheint der QR hier.
                </p>
              )}
              <div className="publishing-target-audit">
                <p>
                  <strong>Status:</strong> {getWhatsappConnectionLabel(whatsappClientConfig.connectionStatus)}
                </p>
                <p>
                  <strong>Wartezeit:</strong> {Math.round((whatsappClientConfig.loginTimeoutMs || 120000) / 1000)} Sekunden
                </p>
                <p>
                  <strong>Hinweis:</strong> Browser nach dem Scan offen lassen, bis die Hauptoberflaeche sichtbar ist.
                </p>
                <p>
                  <strong>Manueller Debug:</strong> Test-Kanal im sichtbaren Browser oeffnen und dann den Manual-Debug starten.
                </p>
                <p>
                  <strong>Debug-Artefakte:</strong> {whatsappClientConfig.lastChannelDebugArtifacts?.length || 0}
                </p>
              </div>
            </article>
          </section>
        ) : null}

        {(whatsappTargetConfig.targets || []).length ? (
          <div className="publishing-feed">
            {(whatsappTargetConfig.targets || []).map((target, index) => {
              const targetStatus = getWhatsappTargetStatus(target);
              const targetKind = getWhatsappTargetKind(target);

              return (
                <article
                  key={target.id || index}
                  className={`publishing-feed-item publishing-target-card ${targetKind === 'live' ? 'publishing-target-live' : ''}`}
                >
                  <div className="publishing-item-head">
                    <div className="publishing-target-heading">
                      <strong>{target.name || `WhatsApp Ziel ${index + 1}`}</strong>
                      <p className="publishing-target-ref">{target.targetRef || 'Target Ref fehlt'}</p>
                    </div>
                    <div className="publishing-target-chip-row">
                      <span className={`status-chip ${targetKind === 'live' ? 'danger' : targetKind === 'test' ? 'success' : 'info'}`}>
                        {getWhatsappTargetTypeLabel(target)}
                      </span>
                      <span className={`status-chip ${targetStatus.tone}`}>{targetStatus.label}</span>
                    </div>
                  </div>

                  {targetKind === 'live' ? (
                    <div className="publishing-live-warning">
                      <strong>LIVE KANAL</strong>
                      <span>{target.isActive ? 'Aktiv' : 'Deaktiviert'}</span>
                      <p>{target.isActive ? 'Live-Sendung ist bewusst freigegeben.' : 'Erst manuell aktivieren.'}</p>
                    </div>
                  ) : null}

                  {targetKind === 'test' ? (
                    <div className="publishing-live-warning">
                      <strong>TEST KANAL</strong>
                      <span>{target.isActive ? 'Aktiv' : 'Deaktiviert'}</span>
                      <p>Dieser Kanal ist fuer Testposts und sichere Queue-Laeufe gedacht.</p>
                    </div>
                  ) : null}

                  <div className="publishing-target-audit">
                    <p>
                      <strong>Typ:</strong> {targetKind === 'test' ? 'TEST' : targetKind === 'live' ? 'LIVE' : 'STANDARD'}
                    </p>
                    <p>
                      <strong>Verbindung:</strong> {getWhatsappConnectionLabel(whatsappClientConfig.connectionStatus)}
                    </p>
                    <p>
                      <strong>Worker:</strong> {getWhatsappWorkerLabel(whatsappClientConfig.workerStatus)}
                    </p>
                    <p>
                      <strong>URL:</strong> {target.channelUrl || '-'}
                    </p>
                    <p>
                      <strong>Letzte Sendung:</strong> {formatDateTime(target.lastSentAt)}
                    </p>
                    <p>
                      <strong>Letzter Test:</strong> {formatDateTime(target.lastTestedAt)}
                    </p>
                    <p>
                      <strong>Letzter Status:</strong> {target.lastDeliveryStatus || '-'}
                    </p>
                    <p>
                      <strong>Letzter Fehler:</strong>{' '}
                      {target.lastError ? `${formatDateTime(target.lastErrorAt)} | ${target.lastError}` : '-'}
                    </p>
                    {whatsappClientConfig.lastChannelTargetRef &&
                    (target.targetRef === whatsappClientConfig.lastChannelTargetRef ||
                      target.channelUrl === whatsappClientConfig.lastChannelTargetRef) ? (
                      <p>
                        <strong>Navigation:</strong>{' '}
                        {getWhatsappChannelNavigationLabel(whatsappClientConfig.channelNavigationStatus)} |{' '}
                        {getWhatsappAdminStatusLabel(whatsappClientConfig.channelAdminStatus)}
                      </p>
                    ) : null}
                  </div>

                  <div className="form-row">
                    <input
                      value={target.name || ''}
                      disabled={!isAdmin}
                      placeholder="Name der Zielgruppe"
                      onChange={(event) => updateWhatsappTarget(index, { name: event.target.value })}
                    />
                    <input
                      value={target.targetLabel || ''}
                      disabled={!isAdmin}
                      placeholder="Sichtbares Label"
                      onChange={(event) => updateWhatsappTarget(index, { targetLabel: event.target.value })}
                    />
                    <input
                      value={target.targetRef || ''}
                      disabled={!isAdmin}
                      placeholder="Kanal URL / Ziel-Ref"
                      onChange={(event) => updateWhatsappTarget(index, { targetRef: event.target.value })}
                    />
                  </div>
                  <div className="form-row">
                    <input
                      value={target.channelUrl || ''}
                      disabled={!isAdmin}
                      placeholder="Kanal URL"
                      onChange={(event) => updateWhatsappTarget(index, { channelUrl: event.target.value })}
                    />
                    <select
                      value={target.targetType || 'WHATSAPP_CHANNEL'}
                      disabled={!isAdmin}
                      onChange={(event) => {
                        const nextType = event.target.value;
                        updateWhatsappTarget(index, {
                          targetType: nextType,
                          requiresManualActivation: nextType !== 'WHATSAPP_TEST_CHANNEL'
                        });
                      }}
                    >
                      <option value="WHATSAPP_TEST_CHANNEL">WHATSAPP_TEST_CHANNEL</option>
                      <option value="WHATSAPP_CHANNEL">WHATSAPP_CHANNEL</option>
                    </select>
                  </div>

                  <div className="publishing-split">
                    <label className="checkbox-card">
                      <span>Ziel aktiv</span>
                      <input
                        type="checkbox"
                        checked={Boolean(target.isActive)}
                        disabled={!isAdmin}
                        onChange={(event) => updateWhatsappTarget(index, { isActive: event.target.checked })}
                      />
                    </label>
                    <label className="checkbox-card">
                      <span>Fuer Publishing verwenden</span>
                      <input
                        type="checkbox"
                        checked={Boolean(target.useForPublishing)}
                        disabled={!isAdmin}
                        onChange={(event) => updateWhatsappTarget(index, { useForPublishing: event.target.checked })}
                      />
                    </label>
                  </div>

                  {isAdmin ? (
                    <div className="publishing-target-actions">
                      <button
                        type="button"
                        className="secondary"
                        onClick={() => void testWhatsappTarget(target)}
                        disabled={testingWhatsappTargetId === target.id || typeof target.id !== 'number'}
                      >
                        {testingWhatsappTargetId === target.id ? 'Pruefe...' : 'Verbindung pruefen'}
                      </button>
                      <button
                        type="button"
                        className="secondary"
                        onClick={() => void sendWhatsappTargetTestPost(target)}
                        disabled={sendingWhatsappTargetId === target.id || typeof target.id !== 'number'}
                      >
                        {sendingWhatsappTargetId === target.id ? 'Sende...' : 'Testpost senden'}
                      </button>
                      <button
                        type="button"
                        className="secondary"
                        onClick={() => removeWhatsappTarget(index)}
                        disabled={target.isSystem === true}
                      >
                        {target.isSystem === true ? 'Systemziel fix' : 'Zielgruppe entfernen'}
                      </button>
                    </div>
                  ) : null}
                </article>
              );
            })}
          </div>
        ) : (
          <p className="publishing-empty">
            Noch keine persistenten WhatsApp Ziele gespeichert. Die bestehende Gateway-Logik bleibt unveraendert.
          </p>
        )}

        {isAdmin ? (
          <button type="button" className="primary" onClick={() => void saveWhatsappTargets()}>
            Zielgruppen speichern
          </button>
        ) : null}

        <section className="publishing-panel publishing-panel-compact">
          <div className="publishing-panel-header">
            <div>
              <p className="section-title">Queue</p>
              <h3 className="page-title">Offene WhatsApp Beitraege</h3>
            </div>
            <span className="publishing-note">{recentQueueCount} Eintraege</span>
          </div>
          {recentQueueCount ? (
            <div className="publishing-feed">
              {whatsappRecentQueue.map((item) => (
                <article key={item.id} className="publishing-feed-item">
                  <div className="publishing-item-head">
                    <strong>{item.payload?.title || `Queue ${item.id}`}</strong>
                    <span className={`status-chip ${getStatusTone(item.status)}`}>{item.status}</span>
                  </div>
                  <p>
                    Retry {item.retry_count} | erstellt {formatDateTime(item.created_at)}
                  </p>
                  <p>
                    {(item.targets || [])
                      .filter((entry) => entry.channel_type === 'whatsapp')
                      .map((entry) => `${entry.target_label || entry.target_ref || 'WhatsApp'}:${entry.status}`)
                      .join(' | ') || 'Kein WhatsApp Target'}
                  </p>
                  {isAdmin ? (
                    <button type="button" className="secondary" onClick={() => void retryQueue(item.id)}>
                      Retry anstossen
                    </button>
                  ) : null}
                </article>
              ))}
            </div>
          ) : (
            <p className="publishing-empty">Keine offenen WhatsApp Queue-Eintraege.</p>
          )}
        </section>

        <section className="publishing-panel publishing-panel-compact">
          <div className="publishing-panel-header">
            <div>
              <p className="section-title">Logs</p>
              <h3 className="page-title">Letzte WhatsApp Worker-Events</h3>
            </div>
            <span className="publishing-note">{recentLogCount} Eintraege</span>
          </div>
          {recentLogCount ? (
            <div className="publishing-feed">
              {whatsappRecentLogs.map((item) => (
                <article key={item.id} className="publishing-feed-item">
                  <div className="publishing-item-head">
                    <strong>{item.event_type}</strong>
                    <span className={`status-chip ${item.level === 'error' ? 'danger' : item.level === 'warning' ? 'warning' : 'info'}`}>
                      {item.level}
                    </span>
                  </div>
                  <p>{item.message}</p>
                  <p>{formatDateTime(item.created_at)}</p>
                </article>
              ))}
            </div>
          ) : (
            <p className="publishing-empty">Noch keine WhatsApp Worker-Logs gespeichert.</p>
          )}
        </section>
      </section>
    );
  }

  function renderFacebookTab() {
    return (
      <section className="card publishing-panel">
        <div className="publishing-panel-header">
          <div>
            <p className="section-title">Facebook Worker</p>
            <h2 className="page-title">Persistente Session und Retry</h2>
          </div>
        </div>
        <label className="checkbox-card">
          <span>Facebook Worker aktiv</span>
          <input
            type="checkbox"
            checked={facebookSettings.facebookEnabled}
            onChange={(event) => setFacebookSettings((prev) => ({ ...prev, facebookEnabled: event.target.checked }))}
          />
        </label>
        <div className="form-row">
          <select
            value={facebookSettings.facebookSessionMode}
            onChange={(event) => setFacebookSettings((prev) => ({ ...prev, facebookSessionMode: event.target.value }))}
          >
            <option value="persistent">persistent</option>
            <option value="manual-refresh">manual-refresh</option>
          </select>
          <input
            type="number"
            value={facebookSettings.facebookDefaultRetryLimit}
            onChange={(event) =>
              setFacebookSettings((prev) => ({ ...prev, facebookDefaultRetryLimit: Number(event.target.value || 3) }))
            }
          />
          <input
            value={facebookSettings.facebookDefaultTarget}
            placeholder="Default Zielgruppe / Seite"
            onChange={(event) => setFacebookSettings((prev) => ({ ...prev, facebookDefaultTarget: event.target.value }))}
          />
        </div>
        {isAdmin ? (
          <button type="button" className="primary" onClick={() => void saveFacebookWorker()}>
            Speichern
          </button>
        ) : null}
      </section>
    );
  }

  function renderLogsTab() {
    return (
      <section className="card publishing-panel">
        <div className="publishing-panel-header">
          <div>
            <p className="section-title">Logs</p>
            <h2 className="page-title">Publishing und Worker-Events</h2>
          </div>
          <span className="publishing-note">{logs.length} Eintraege geladen</span>
        </div>
        {logs.length ? (
          <div className="publishing-feed">
            {logs.map((item) => (
              <article key={item.id} className="publishing-feed-item">
                <div className="publishing-item-head">
                  <strong>{item.event_type}</strong>
                  <span className={`status-chip ${item.level === 'warning' ? 'warning' : 'info'}`}>{item.level}</span>
                </div>
                <p>{item.message}</p>
                <p>{formatDateTime(item.created_at)}</p>
              </article>
            ))}
          </div>
        ) : (
          <p className="publishing-empty">Keine Daten vorhanden</p>
        )}
      </section>
    );
  }

  function renderCurrentTab() {
    if (currentTab === '/publishing/workers') {
      return renderWorkersTab();
    }
    if (currentTab === '/publishing/telegram') {
      return renderTelegramTab();
    }
    if (currentTab === '/publishing/whatsapp') {
      return renderWhatsappTab();
    }
    if (currentTab === '/publishing/facebook') {
      return renderFacebookTab();
    }
    if (currentTab === '/publishing/logs') {
      return renderLogsTab();
    }
    return renderQueueTab();
  }

  return (
    <Layout>
      <div className="publishing-page">
        <section className="card publishing-hero">
          <div className="publishing-panel-header">
            <div>
              <p className="section-title">Output</p>
              <h1 className="page-title">Publishing, Queue und Worker als klarer Versandbereich</h1>
              <p className="page-subtitle">
                Diese Seite laedt jetzt tab-spezifisch, zeigt Fehler sofort sichtbar an und blockiert nicht mehr durch
                unnoetige Queue- oder Log-Requests.
              </p>
            </div>
            <span className="badge">Entscheidung - Queue - Worker - Zielkanal</span>
          </div>
        </section>

        <section className="card publishing-panel publishing-panel-compact">
          <div className="publishing-tabs" role="tablist" aria-label="Publishing Bereiche">
            {tabs.map((item) => (
              <NavLink
                key={item.path}
                to={item.path}
                className={({ isActive }) => `publishing-tab ${isActive ? 'active' : ''}`}
              >
                {item.label}
              </NavLink>
            ))}
          </div>
        </section>

        {status ? (
          <section className="card publishing-panel publishing-status-strip">
            <p>{status}</p>
          </section>
        ) : null}

        {loading ? (
          <section className="card publishing-panel">
            <p style={{ margin: 0 }}>Publishing-Daten werden geladen...</p>
          </section>
        ) : (
          <>
            <section className="card publishing-panel publishing-panel-compact">
              <div className="publishing-panel-header">
                <div>
                  <p className="section-title">Sofortansicht</p>
                  <h2 className="page-title">Wichtigste Versanddaten oben</h2>
                </div>
                <span className="publishing-note">{tabs.find((item) => item.path === currentTab)?.label || 'Publishing'}</span>
              </div>
              <div className="publishing-grid">
                {summaryCards.map((card) => (
                  <article key={card.title} className={`publishing-stat-card publishing-tone-${card.tone}`}>
                    <div className="publishing-item-head">
                      <strong>{card.title}</strong>
                      <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                    </div>
                    <h3>{card.value}</h3>
                    <p>{card.detail}</p>
                  </article>
                ))}
              </div>
            </section>

            {renderCurrentTab()}
          </>
        )}
      </div>
    </Layout>
  );
}

export default PublishingPage;
