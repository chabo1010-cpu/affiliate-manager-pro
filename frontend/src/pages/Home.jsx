import { useEffect, useMemo, useState } from 'react'
import Layout from '../components/layout/Layout'
import { useAuth } from '../context/AuthContext'
import './Home.css'

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000'
const DASHBOARD_API_TIMEOUT_MS = 1500

function toNumber(value) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : 0
}

function formatDateTime(value) {
  if (!value) {
    return '-'
  }

  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) {
    return value
  }

  return new Intl.DateTimeFormat('de-DE', {
    dateStyle: 'short',
    timeStyle: 'short'
  }).format(parsed)
}

function shortenText(value, maxLength = 84) {
  if (!value) {
    return '-'
  }

  const normalized = String(value).replace(/\s+/g, ' ').trim()
  if (normalized.length <= maxLength) {
    return normalized
  }

  return `${normalized.slice(0, Math.max(0, maxLength - 3)).trimEnd()}...`
}

function sortByTimeDesc(items = []) {
  return [...items].sort((left, right) => {
    const leftTime = new Date(left.time || left.createdAt || left.updatedAt || 0).getTime()
    const rightTime = new Date(right.time || right.createdAt || right.updatedAt || 0).getTime()
    return rightTime - leftTime
  })
}

function isWithinLastMinutes(value, minutes) {
  if (!value) {
    return false
  }

  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) {
    return false
  }

  return Date.now() - parsed.getTime() <= minutes * 60 * 1000
}

function getStatusTone(status) {
  const normalized = String(status || '').trim().toLowerCase()

  if (
    normalized.includes('fehler') ||
    normalized.includes('kritisch') ||
    normalized.includes('failed') ||
    normalized.includes('error') ||
    normalized.includes('block')
  ) {
    return 'danger'
  }

  if (
    normalized.includes('warn') ||
    normalized.includes('deaktiviert') ||
    normalized.includes('leer') ||
    normalized.includes('pending') ||
    normalized.includes('retry') ||
    normalized.includes('konfiguration')
  ) {
    return 'warning'
  }

  if (
    normalized.includes('aktiv') ||
    normalized.includes('verbunden') ||
    normalized.includes('bereit') ||
    normalized.includes('geschuetzt') ||
    normalized.includes('sent') ||
    normalized.includes('ok')
  ) {
    return 'success'
  }

  return 'info'
}

function getOperationalTone(status) {
  const normalized = String(status || '').trim().toLowerCase()

  if (['active', 'aktiv', 'live', 'produktiv'].includes(normalized)) {
    return 'success'
  }

  if (['session_missing', 'session fehlt', 'not_configured', 'nicht konfiguriert', 'disabled', 'deaktiviert'].includes(normalized)) {
    return 'warning'
  }

  if (['prepared', 'vorbereitet', 'optional aktiv'].includes(normalized)) {
    return 'info'
  }

  return getStatusTone(status)
}

function HomePage() {
  const { user } = useAuth()
  const [dashboard, setDashboard] = useState({
    amazon: null,
    bot: null,
    dealEngine: null,
    copybot: null,
    advertising: null,
    keepaStatus: null,
    queue: null,
    logs: null,
    workers: null,
    repostSettings: null,
    history: null,
    sources: null,
    copybotLogs: null,
    debug: null
  })
  const [loading, setLoading] = useState(false)
  const [status, setStatus] = useState('')

  useEffect(() => {
    let cancelled = false

    async function apiFetch(path) {
      const controller = new AbortController()
      const timeoutId = window.setTimeout(() => controller.abort(), DASHBOARD_API_TIMEOUT_MS)

      try {
        const response = await fetch(`${API_BASE_URL}${path}`, {
          signal: controller.signal,
          headers: {
            'Content-Type': 'application/json',
            'X-User-Role': user?.role || ''
          }
        })
        const data = await response.json().catch(() => ({}))

        if (!response.ok) {
          throw new Error(data?.error || `Request fehlgeschlagen (${response.status}).`)
        }

        return data
      } catch (error) {
        if (error?.name === 'AbortError') {
          console.warn('[DASHBOARD_DATA_LOAD_TIMEOUT]', {
            path,
            timeoutMs: DASHBOARD_API_TIMEOUT_MS
          })
          throw new Error(`Timeout nach ${DASHBOARD_API_TIMEOUT_MS}ms: ${path}`)
        }
        throw error
      } finally {
        window.clearTimeout(timeoutId)
      }
    }

    async function loadDashboard() {
      try {
        console.info('[DASHBOARD_RENDER_START]', {
          mode: 'async_placeholders_first',
          timeoutMs: DASHBOARD_API_TIMEOUT_MS
        })
        console.info('[DASHBOARD_SAFE_FALLBACK_RENDERED]', {
          state: 'initial_placeholders',
          message: 'Keine Daten'
        })
        setLoading(false)
        setStatus('')

        const results = await Promise.allSettled([
          apiFetch('/api/bot'),
          apiFetch('/api/copybot/overview'),
          apiFetch('/api/keepa/status'),
          apiFetch('/api/amazon/status'),
          apiFetch('/api/publishing/queue'),
          Promise.resolve({ items: [] }),
          apiFetch('/api/publishing/workers/status'),
          apiFetch('/api/deals/settings'),
          apiFetch('/api/deals/history'),
          apiFetch('/api/copybot/sources'),
          Promise.resolve({ items: [] }),
          apiFetch('/api/advertising/dashboard'),
          apiFetch('/api/deal-engine/dashboard'),
          Promise.resolve({ runtimeFlags: {} })
        ])

        const [
          botResult,
          copybotResult,
          keepaResult,
          amazonResult,
          queueResult,
          logsResult,
          workersResult,
          repostSettingsResult,
          historyResult,
          sourcesResult,
          copybotLogsResult,
          advertisingResult,
          dealEngineResult,
          debugResult
        ] = results

        const partialErrors = results
          .filter((item) => item.status === 'rejected')
          .map((item) => (item.reason instanceof Error ? item.reason.message : 'Ein Teilbereich konnte nicht geladen werden.'))
        const loadedCount = results.filter((item) => item.status === 'fulfilled').length
        const failedCount = results.length - loadedCount

        if (!cancelled) {
          setDashboard({
            bot: botResult.status === 'fulfilled' ? botResult.value : null,
            dealEngine: dealEngineResult.status === 'fulfilled' ? dealEngineResult.value : null,
            copybot: copybotResult.status === 'fulfilled' ? copybotResult.value : null,
            keepaStatus: keepaResult.status === 'fulfilled' ? keepaResult.value : null,
            amazon: amazonResult.status === 'fulfilled' ? amazonResult.value : null,
            queue: queueResult.status === 'fulfilled' ? queueResult.value : null,
            logs: logsResult.status === 'fulfilled' ? logsResult.value : null,
            workers: workersResult.status === 'fulfilled' ? workersResult.value : null,
            repostSettings: repostSettingsResult.status === 'fulfilled' ? repostSettingsResult.value : null,
            history: historyResult.status === 'fulfilled' ? historyResult.value : null,
            sources: sourcesResult.status === 'fulfilled' ? sourcesResult.value : null,
            copybotLogs: copybotLogsResult.status === 'fulfilled' ? copybotLogsResult.value : null,
            advertising: advertisingResult.status === 'fulfilled' ? advertisingResult.value : null,
            debug: debugResult.status === 'fulfilled' ? debugResult.value : null
          })

          console.info('[DASHBOARD_DATA_LOADED]', {
            loadedCount,
            failedCount,
            skippedHeavyInitialLoads: ['publishing_logs', 'copybot_logs', 'debug_test']
          })

          if (partialErrors.length) {
            setStatus(failedCount === results.length ? 'Keine Daten' : `Monitoring teilweise unvollstaendig: ${partialErrors[0]}`)
            console.info('[DASHBOARD_SAFE_FALLBACK_RENDERED]', {
              state: failedCount === results.length ? 'all_failed' : 'partial_failed',
              failedCount
            })
          }
        }
      } catch (error) {
        if (!cancelled) {
          setStatus('Keine Daten')
          console.info('[DASHBOARD_SAFE_FALLBACK_RENDERED]', {
            state: 'load_error',
            error: error instanceof Error ? error.message : 'Dashboard konnte nicht geladen werden.'
          })
        }
      } finally {
        if (!cancelled) {
          setLoading(false)
        }
      }
    }

    void loadDashboard()

    return () => {
      cancelled = true
    }
  }, [user?.role])

  const amazonStatus = dashboard.amazon || {}
  const botOverview = dashboard.bot || {}
  const dealEngineOverview = dashboard.dealEngine || {}
  const botModules = botOverview?.modules || {}
  const copybotOverview = dashboard.copybot || {}
  const keepaStatus = dashboard.keepaStatus || {}
  const queueItems = Array.isArray(dashboard.queue?.items) ? dashboard.queue.items : []
  const publishingLogs = Array.isArray(dashboard.logs?.items) ? dashboard.logs.items : []
  const workerChannels = Array.isArray(dashboard.workers?.channels) ? dashboard.workers.channels : []
  const repostSettings = dashboard.repostSettings || {}
  const historyItems = Array.isArray(dashboard.history?.items) ? dashboard.history.items : []
  const sourceItems = Array.isArray(dashboard.sources?.items) ? dashboard.sources.items : []
  const copybotLogItems = Array.isArray(dashboard.copybotLogs?.items) ? dashboard.copybotLogs.items : []
  const advertisingOverview = dashboard.advertising || {}
  const debugOverview = dashboard.debug || {}
  const runtimeFlags = debugOverview.runtimeFlags || {}
  const operationalStatus = botOverview.operationalStatus || {}
  const productionReality = botOverview.productionReality || { live: [], prepared: [], blocked: [] }
  const latestQueueItems = queueItems.slice(0, 6)
  const latestPublishingItems = publishingLogs.slice(0, 6)
  const latestHistoryItems = historyItems.slice(0, 6)
  const latestDeals = Array.isArray(copybotOverview?.lastProcessedDeals) ? copybotOverview.lastProcessedDeals.slice(0, 6) : []
  const amazonApiStatus =
    amazonStatus?.overview?.apiStatus || (amazonStatus?.connection?.connected === true ? 'verbunden' : 'unbekannt')
  const repostCooldownEnabled = Boolean(repostSettings?.repostCooldownEnabled)
  const repostCooldownHours = toNumber(repostSettings?.repostCooldownHours)
  const keepaEnabled = Boolean(keepaStatus?.settings?.keepaEnabled)
  const keepaGapPct = toNumber(keepaStatus?.settings?.strongDealMinComparisonGapPct)
  const telegramUserApi = botModules?.telegramUserApi || {}
  const telegramBotApi = botModules?.telegramBotApi || {}
  const whatsappModule = botModules?.whatsapp || {}
  const persistenceModule = botModules?.persistence || {}
  const telegramBotConfigured = Boolean(telegramBotApi?.configured)
  const telegramLoginReady = Boolean(telegramUserApi?.enabled && telegramUserApi?.apiConfigured)
  const telegramReaderRuntime = operationalStatus.telegramReader || {}
  const telegramBotRuntime = operationalStatus.telegramBot || {}
  const whatsappRuntime = operationalStatus.whatsapp || {}
  const aiResolverRuntime = operationalStatus.aiResolver || {}
  const schedulerRuntime = operationalStatus.scheduler || {}

  const activeSources = useMemo(
    () => sourceItems.filter((item) => Number(item.is_active) === 1),
    [sourceItems]
  )
  const activeTelegramSourceItems = useMemo(
    () => activeSources.filter((item) => String(item.platform || '').toLowerCase() === 'telegram'),
    [activeSources]
  )
  const activeWhatsappSourceItems = useMemo(
    () => activeSources.filter((item) => String(item.platform || '').toLowerCase() === 'whatsapp'),
    [activeSources]
  )

  const queueSummary = useMemo(
    () =>
      workerChannels.reduce(
        (summary, item) => ({
          pending: summary.pending + toNumber(item.pending ?? item.waiting),
          sending: summary.sending + toNumber(item.sending ?? item.processing),
          sent: summary.sent + toNumber(item.sent ?? item.posted),
          retry: summary.retry + toNumber(item.retry),
          failed: summary.failed + toNumber(item.failed)
        }),
        { pending: 0, sending: 0, sent: 0, retry: 0, failed: 0 }
      ),
    [workerChannels]
  )

  const openQueueCount = queueSummary.pending + queueSummary.sending + queueSummary.retry
  const publishingTone =
    queueSummary.failed > 0 ? 'danger' : openQueueCount > 0 ? 'warning' : queueSummary.sent > 0 ? 'success' : 'info'

  const lockSummary = useMemo(
    () =>
      historyItems.reduce(
        (summary, item) => {
          const originType = String(item.originType || '').toLowerCase()
          const channel = String(item.channel || '').toLowerCase()

          if (originType === 'manual') {
            summary.manual += 1
          }

          if (originType === 'automatic') {
            summary.automatic += 1
          }

          if (channel.includes('telegram')) {
            summary.telegram += 1
          } else if (channel.includes('whatsapp')) {
            summary.whatsapp += 1
          } else if (channel.includes('facebook')) {
            summary.facebook += 1
          }

          return summary
        },
        { manual: 0, automatic: 0, telegram: 0, whatsapp: 0, facebook: 0 }
      ),
    [historyItems]
  )

  const errorEntries = useMemo(() => {
    const queueErrors = queueItems.flatMap((item) =>
      (item.targets || [])
        .filter((target) => ['failed', 'retry'].includes(String(target.status || '').toLowerCase()))
        .map((target, index) => ({
          id: `queue-${item.id}-${target.id || index}`,
          category: 'Queue',
          title: `${target.channel_type || 'target'} in ${target.status}`,
          detail:
            target.error_message ||
            `${shortenText(item.payload?.title || `Queue ${item.id}`, 72)} wartet weiter auf Versand.`,
          time: target.updated_at || item.updated_at || item.created_at,
          tone: target.status === 'failed' ? 'danger' : 'warning'
        }))
    )

    const publishingErrors = publishingLogs
      .filter((item) => ['warning', 'error'].includes(String(item.level || '').toLowerCase()))
      .map((item) => ({
        id: `publishing-${item.id}`,
        category: 'Publishing',
        title: item.event_type || 'Publishing Event',
        detail: item.message || 'Publishing-Fehler ohne Detailtext.',
        time: item.created_at,
        tone: String(item.level || '').toLowerCase() === 'error' ? 'danger' : 'warning'
      }))

    const copybotErrors = copybotLogItems
      .filter((item) => ['warning', 'error'].includes(String(item.level || '').toLowerCase()))
      .map((item) => ({
        id: `copybot-${item.id}`,
        category: 'Quelle',
        title: item.event_type || 'Copybot Event',
        detail: item.message || 'Quellen-Fehler ohne Detailtext.',
        time: item.created_at,
        tone: String(item.level || '').toLowerCase() === 'error' ? 'danger' : 'warning'
      }))

    return sortByTimeDesc([...queueErrors, ...publishingErrors, ...copybotErrors]).slice(0, 8)
  }, [copybotLogItems, publishingLogs, queueItems])

  const timelineItems = useMemo(() => {
    const dealEvents = latestDeals.map((item, index) => ({
      id: `deal-${item.id || index}`,
      category: 'Deal',
      title: item.title || item.source_name || `Deal ${index + 1}`,
      detail: `${item.platform || 'Quelle'} | ${item.seller_type || 'FBM'} | Status ${item.status || 'offen'}`,
      time: item.created_at,
      tone: getStatusTone(item.status || 'info')
    }))

    const publishingEvents = latestPublishingItems.map((item) => ({
      id: `publishing-timeline-${item.id}`,
      category: 'Publishing',
      title: item.event_type || 'Publishing Event',
      detail: item.message || 'Publishing-Event',
      time: item.created_at,
      tone: getStatusTone(item.level || 'info')
    }))

    const sourceEvents = copybotLogItems.slice(0, 6).map((item) => ({
      id: `source-timeline-${item.id}`,
      category: 'Quelle',
      title: item.event_type || 'Quellen-Event',
      detail: item.message || 'Quellen-Event',
      time: item.created_at,
      tone: getStatusTone(item.level || 'info')
    }))

    const lockEvents = latestHistoryItems.map((item) => ({
      id: `lock-${item.id}`,
      category: 'Sperre',
      title: item.title || item.asin || 'Sperre gespeichert',
      detail: `${item.channel || 'Kanal unbekannt'} | ${item.originType || 'unknown'} | ${item.sellerType || 'FBM'}`,
      time: item.postedAt,
      tone: 'info'
    }))

    return sortByTimeDesc([...dealEvents, ...publishingEvents, ...sourceEvents, ...lockEvents]).slice(0, 12)
  }, [copybotLogItems, latestDeals, latestHistoryItems, latestPublishingItems])

  const overallSystemStatus =
    errorEntries.length > 0
      ? 'Aufmerksamkeit noetig'
      : openQueueCount > 0
        ? 'Aktiv'
        : telegramLoginReady || telegramBotConfigured
          ? 'Bereit'
          : 'Konfiguration noetig'

  const systemStatusCards = [
    {
      title: 'Systemstatus',
      value: overallSystemStatus,
      detail: `${openQueueCount} offene Jobs | ${errorEntries.length} Fehlerhinweise | Letzter Check ${formatDateTime(
        botOverview?.lastCheck
      )}`,
      tone: getStatusTone(overallSystemStatus)
    },
    {
      title: 'Decision Engine',
      value: dealEngineOverview?.runtimeStatus?.internetDecision?.label || 'Internet primaer',
      detail:
        dealEngineOverview?.runtimeStatus?.aiResolver?.detail ||
        `Marktvergleich fuehrt. Keepa bleibt Backup ab ${keepaGapPct || 0}% Mindestabstand.`,
      tone: 'success'
    },
    {
      title: 'Output Integrationen',
      value: `${telegramBotRuntime?.label || 'vorbereitet'} / ${whatsappRuntime?.label || 'nicht konfiguriert'}`,
      detail: `${queueSummary.sent} erfolgreiche Sendungen | ${queueSummary.failed} fehlgeschlagen | ${queueSummary.retry} im Retry`,
      tone: publishingTone
    },
    {
      title: 'Persistenz',
      value: repostCooldownEnabled ? 'Geschuetzt' : 'Sperre deaktiviert',
      detail: `${toNumber(persistenceModule?.queueEntries || queueItems.length)} Queue-Eintraege | ${historyItems.length} Sperren | ${shortenText(
        persistenceModule?.dbPath || '-',
        44
      )}`,
      tone: repostCooldownEnabled ? 'success' : 'warning'
    }
  ]

  const operationalCards = [
    {
      title: 'Telegram Reader',
      value: telegramReaderRuntime?.label || 'vorbereitet',
      detail:
        telegramReaderRuntime?.detail ||
        'Telegram Reader ist vorbereitet und benoetigt fuer Live-Betrieb eine echte User Session.',
      tone: getOperationalTone(telegramReaderRuntime?.status)
    },
    {
      title: 'Telegram Bot',
      value: telegramBotRuntime?.label || 'vorbereitet',
      detail: telegramBotRuntime?.detail || 'Telegram Bot bleibt der primaere Live-Output fuer genehmigte Deals.',
      tone: getOperationalTone(telegramBotRuntime?.status)
    },
    {
      title: 'WhatsApp',
      value: whatsappRuntime?.label || 'nicht konfiguriert',
      detail: whatsappRuntime?.detail || 'WhatsApp bleibt optional und nur mit echter Produktiv-Anbindung live.',
      tone: getOperationalTone(whatsappRuntime?.status)
    },
    {
      title: 'KI Resolver',
      value: aiResolverRuntime?.label || 'deaktiviert',
      detail:
        aiResolverRuntime?.detail ||
        'KI ist optional und greift nur bei Unsicherheit nach Marktvergleich, Keepa und Fake-Pattern ein.',
      tone: getOperationalTone(aiResolverRuntime?.status)
    },
    {
      title: 'Scheduler',
      value: schedulerRuntime?.label || 'deaktiviert',
      detail: schedulerRuntime?.detail || 'Scheduler startet Deals, Queue und Werbemodule gemeinsam.',
      tone: getOperationalTone(schedulerRuntime?.status)
    }
  ]

  const realitySummaryCards = [
    {
      title: 'Produktionsreif',
      items: productionReality.live || [],
      tone: 'success'
    },
    {
      title: 'Vorbereitet',
      items: productionReality.prepared || [],
      tone: 'info'
    },
    {
      title: 'Noch blockiert',
      items: productionReality.blocked || [],
      tone: 'warning'
    }
  ]

  const telegramStatusCards = [
    {
      title: 'Login Status',
      value: telegramReaderRuntime?.label || (toNumber(telegramUserApi?.activeSessions) > 0 ? 'Verbunden' : telegramLoginReady ? 'Bereit' : 'Konfiguration fehlt'),
      detail:
        telegramReaderRuntime?.detail ||
        `API ${telegramUserApi?.apiConfigured ? 'konfiguriert' : 'nicht konfiguriert'} | Reader ${
          telegramUserApi?.enabled ? 'aktiv' : 'deaktiviert'
        }`,
      tone: getOperationalTone(telegramReaderRuntime?.status || (toNumber(telegramUserApi?.activeSessions) > 0 ? 'active' : 'prepared'))
    },
    {
      title: 'Login Modus',
      value: telegramUserApi?.loginMode || '-',
      detail: `Letzte Nachricht ${formatDateTime(telegramUserApi?.lastMessageAt)}`,
      tone: 'info'
    },
    {
      title: 'Sessions',
      value: `${toNumber(telegramUserApi?.savedSessions)} gespeichert`,
      detail: `${toNumber(telegramUserApi?.activeSessions)} aktiv und wiederverwendbar`,
      tone: toNumber(telegramUserApi?.savedSessions) > 0 ? 'success' : 'warning'
    },
    {
      title: 'Channels',
      value: `${toNumber(telegramUserApi?.watchedChannels)} Watches`,
      detail: `${toNumber(telegramUserApi?.sourceCount)} aktive Telegram Quellen | ${toNumber(
        telegramBotApi?.publishTargets
      )} Bot-Ziele`,
      tone: toNumber(telegramUserApi?.watchedChannels) > 0 ? 'success' : 'info'
    }
  ]

  const sourceSummaryCards = [
    {
      title: 'Aktive Quellen',
      value: activeSources.length,
      detail: `${activeTelegramSourceItems.length} Telegram | ${activeWhatsappSourceItems.length} WhatsApp`,
      tone: activeSources.length ? 'success' : 'warning'
    },
    {
      title: 'Review Queue',
      value: toNumber(copybotOverview?.reviewCount),
      detail: `${toNumber(copybotOverview?.approvedCount)} approved | ${toNumber(copybotOverview?.rejectedCount)} rejected`,
      tone: toNumber(copybotOverview?.reviewCount) > 0 ? 'warning' : 'info'
    },
    {
      title: 'Quellen-Testlauf',
      value: copybotOverview?.lastProcessedSource?.name || 'Noch kein Import',
      detail: `Letzter Eingang ${formatDateTime(copybotOverview?.lastProcessedSource?.last_import_at)}`,
      tone: copybotOverview?.lastProcessedSource ? 'info' : 'warning'
    }
  ]

  const advertisingSummaryCards = [
    {
      title: 'Aktive Module',
      value: toNumber(advertisingOverview?.overview?.activeModuleCount),
      detail: `${toNumber(advertisingOverview?.overview?.plannedTodayCount)} geplante Posts heute`,
      tone: toNumber(advertisingOverview?.overview?.activeModuleCount) > 0 ? 'success' : 'warning'
    },
    {
      title: 'Naechster Werbe-Post',
      value: advertisingOverview?.overview?.nextPlannedPost?.moduleName || 'Noch nicht geplant',
      detail: `Naechster Lauf ${formatDateTime(advertisingOverview?.overview?.nextPlannedPost?.scheduledFor)}`,
      tone: advertisingOverview?.overview?.nextPlannedPost ? 'info' : 'warning'
    },
    {
      title: 'Letzter Versand',
      value: advertisingOverview?.overview?.lastSuccess?.moduleName || 'Noch keine Ausfuehrung',
      detail: `Letzter Erfolg ${formatDateTime(
        advertisingOverview?.overview?.lastSuccess?.sentAt || advertisingOverview?.overview?.lastSuccess?.updatedAt
      )}`,
      tone: advertisingOverview?.overview?.lastSuccess ? 'success' : 'info'
    }
  ]

  const liveFlowSteps =
    Array.isArray(botOverview?.finalFlow) && botOverview.finalFlow.length
      ? botOverview.finalFlow.map((step) => ({
          id: step.id,
          label: step.label,
          title: step.status || 'vorbereitet',
          detail: step.detail,
          tone: getOperationalTone(step.status)
        }))
      : [
          {
            id: 'detect',
            label: 'Deal erkannt',
            title: `${latestDeals.length} letzte Deals`,
            detail: latestDeals.length
              ? `${shortenText(latestDeals[0]?.title || latestDeals[0]?.source_name || 'Deal', 56)}`
              : 'Noch kein aktueller Deal im Verlauf.',
            tone: latestDeals.length ? 'success' : 'info'
          },
          {
            id: 'internet',
            label: 'Internetvergleich',
            title: 'Hauptentscheidung',
            detail: `Amazon ${amazonApiStatus} | Mindestabstand ${keepaGapPct || 0}% fuer starke Marktdeals`,
            tone: 'success'
          },
          {
            id: 'queue',
            label: 'Queue und Output',
            title: `${openQueueCount} offen`,
            detail: `${queueSummary.pending} pending | ${queueSummary.sending} sending | ${queueSummary.retry} retry | ${queueSummary.sent} sent`,
            tone: publishingTone
          }
        ]

  const dealEngineTimelineItems = Array.isArray(dealEngineOverview?.timeline) ? dealEngineOverview.timeline : []
  const dealEngineRunItems = Array.isArray(dealEngineOverview?.runs) ? dealEngineOverview.runs : []
  const topDealCandidates = useMemo(
    () =>
      sortByTimeDesc([
        ...latestDeals.map((item, index) => ({
          id: `copybot-live-${item.id || index}`,
          title: item.title || item.source_name || 'Keine Daten',
          decision: item.status || 'Keine Daten',
          source: item.source_name || item.platform || 'Keine Daten',
          time: item.created_at || item.updated_at
        })),
        ...dealEngineTimelineItems.map((item, index) => ({
          id: `engine-live-timeline-${item.id || index}`,
          title: item.title || 'Keine Daten',
          decision: String(item.detail || '').split('|')[0]?.trim() || 'Keine Daten',
          source: 'Deal Engine',
          time: item.createdAt || item.created_at
        })),
        ...dealEngineRunItems.map((item, index) => ({
          id: `engine-live-run-${item.id || index}`,
          title: item.title || item.asin || 'Keine Daten',
          decision: item.decision || 'Keine Daten',
          source: item.source?.name || item.sourceName || item.sourcePlatform || 'Deal Engine',
          time: item.createdAt || item.created_at
        }))
      ]),
    [dealEngineRunItems, dealEngineTimelineItems, latestDeals]
  )
  const lastDeal = topDealCandidates[0] || null
  const newDealsLastFiveMinutes = topDealCandidates.filter((item) => isWithinLastMinutes(item.time, 5)).length
  const approvedCount = toNumber(copybotOverview?.approvedCount) + toNumber(dealEngineOverview?.metrics?.approvedRuns)
  const reviewCount = toNumber(copybotOverview?.reviewCount) + toNumber(dealEngineOverview?.metrics?.queuedRuns)
  const blockCount = toNumber(copybotOverview?.rejectedCount) + toNumber(dealEngineOverview?.metrics?.rejectedRuns)
  const backendLiveStatus =
    loading
      ? 'Keine Daten'
      : status && !botOverview?.lastCheck && !dealEngineOverview?.systemStatus
        ? 'Offline'
        : botOverview?.lastCheck || dealEngineOverview?.systemStatus
          ? 'Online'
          : 'Keine Daten'
  const telegramReaderActive =
    String(telegramReaderRuntime?.status || '').toLowerCase() === 'active' || toNumber(telegramUserApi?.activeSessions) > 0
  const queueActive =
    openQueueCount > 0 ||
    schedulerRuntime?.live === true ||
    workerChannels.some((item) => ['active', 'running'].includes(String(item.status || '').toLowerCase()))
  const testGroupActive = Boolean(
    telegramBotConfigured || telegramBotApi?.targetChatConfigured || toNumber(telegramBotApi?.publishTargets) > 0
  )
  const routeEvidence = useMemo(
    () =>
      queueItems.reduce(
        (summary, item) => {
          const sourceType = String(item.source_type || item.sourceType || '').toLowerCase()
          const routingChannel = String(item.payload?.meta?.telegramRoutingChannel || '').toLowerCase()

          if (sourceType.includes('approved_route') || routingChannel === 'approved') {
            summary.approved = true
          }

          if (sourceType.includes('rejected_route') || routingChannel === 'rejected') {
            summary.rejected = true
          }

          return summary
        },
        { approved: false, rejected: false }
      ),
    [queueItems]
  )
  const outputTestGroupStatus = testGroupActive ? 'aktiv' : 'inaktiv'
  const outputApprovedStatus = routeEvidence.approved ? 'aktiv' : 'Keine Daten'
  const outputRejectedStatus = routeEvidence.rejected ? 'aktiv' : 'Keine Daten'
  const readerTestModeActive = runtimeFlags.readerTestMode === true
  const topLiveCards = [
    {
      title: 'System Status',
      value: backendLiveStatus,
      detail: `Backend: ${backendLiveStatus} | Reader: ${telegramReaderActive ? 'Aktiv' : 'Inaktiv'} | Queue: ${
        queueActive ? 'Aktiv' : 'Inaktiv'
      }`,
      tone: backendLiveStatus === 'Online' && telegramReaderActive && queueActive ? 'success' : backendLiveStatus === 'Online' ? 'info' : 'warning'
    },
    {
      title: 'Deal Flow Live',
      value: `${newDealsLastFiveMinutes} neu`,
      detail: `Approved ${approvedCount} | Review ${reviewCount} | Block ${blockCount}`,
      tone: blockCount > 0 ? 'warning' : approvedCount > 0 ? 'success' : 'info'
    },
    {
      title: 'Output Status',
      value: `Testgruppe ${outputTestGroupStatus}`,
      detail: `Approved: ${outputApprovedStatus} | Rejected: ${outputRejectedStatus}`,
      tone: outputTestGroupStatus === 'aktiv' ? 'success' : 'warning'
    },
    {
      title: 'Letzter Deal',
      value: shortenText(lastDeal?.title, 46),
      detail: lastDeal ? `${lastDeal.decision || 'Keine Daten'} | ${lastDeal.source || 'Keine Daten'}` : 'Keine Daten',
      tone: getStatusTone(lastDeal?.decision || 'info')
    }
  ]

  useEffect(() => {
    if (loading) {
      return
    }

    console.info('[DASHBOARD_TOP_PANEL_RENDERED]', {
      cardCount: topLiveCards.length,
      lastDeal: lastDeal?.id || null
    })
    console.info('[DASHBOARD_LIVE_DATA_BOUND]', {
      backend: backendLiveStatus,
      telegramReader: telegramReaderActive ? 'Aktiv' : 'Inaktiv',
      queue: queueActive ? 'Aktiv' : 'Inaktiv',
      newDealsLastFiveMinutes,
      approvedCount,
      reviewCount,
      blockCount,
      output: {
        testGroup: outputTestGroupStatus,
        approved: outputApprovedStatus,
        rejected: outputRejectedStatus
      }
    })
  }, [
    approvedCount,
    backendLiveStatus,
    blockCount,
    lastDeal?.id,
    loading,
    newDealsLastFiveMinutes,
    outputApprovedStatus,
    outputRejectedStatus,
    outputTestGroupStatus,
    queueActive,
    reviewCount,
    telegramReaderActive,
    topLiveCards.length
  ])

  useEffect(() => {
    if (!loading && readerTestModeActive) {
      console.info('[FRONTEND_TEST_FILTERS_RENDERED]', {
        minDiscountPercent: 0,
        minScore: 0,
        fakeThreshold: 100,
        sellerBlockade: 'AUS',
        marketComparisonRequired: 'AUS',
        sellerRouting: {
          amazonDirect: 'Veröffentlicht',
          fba: 'Veröffentlicht',
          fbm: 'Geblockt',
          unknown: 'Geblockt/Prüfen'
        }
      })
    }
  }, [loading, readerTestModeActive])

  return (
    <Layout>
      <div className="ops-home">
        <section className="card ops-hero">
          <div className="ops-hero-grid">
            <div className="ops-hero-copy">
              <p className="section-title">Affiliate Manager Pro</p>
              <h1 className="page-title">System Dashboard</h1>
              <span className="ops-header-note">Letzter Check {formatDateTime(botOverview?.lastCheck)}</span>
            </div>
          </div>

          <div className="ops-topbar" aria-label="Live Dashboard">
            {topLiveCards.map((card) => (
              <article key={card.title} className={`ops-status-card ops-tone-${card.tone}`}>
                <div className="ops-card-head">
                  <p className="section-title">{card.title}</p>
                  <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                </div>
                <h2>{card.value || 'Keine Daten'}</h2>
                <p className="ops-card-copy">{card.detail || 'Keine Daten'}</p>
              </article>
            ))}
          </div>

          {readerTestModeActive && (
            <div className="ops-inline-alert">
              <span className="status-chip info">{'\u{1F9EA} Testmodus aktiv'}</span>
              <p>
                {'Seller Routing: '}
                {'\u2705 Amazon Direkt -> Veröffentlicht | '}
                {'\u2705 FBA -> Veröffentlicht | '}
                {'\u{1F6AB} FBM -> Geblockt | '}
                {'\u26A0\uFE0F UNKNOWN -> Geblockt/Prüfen. '}
                {'Filter: Mindest-Rabatt: 0% | Mindest-Score: 0 | Fake-Schwelle: 100%'}
              </p>
            </div>
          )}

          {status && (
            <div className="ops-inline-alert">
              <span className="status-chip warning">Hinweis</span>
              <p>{status}</p>
            </div>
          )}
        </section>

        {loading ? (
          <section className="card ops-panel">
            <p style={{ margin: 0 }}>Dashboard wird geladen...</p>
          </section>
        ) : (
          <>
            <section className="card ops-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">1. Systemstatus</p>
                  <h2 className="page-title">Gesamtzustand auf einen Blick</h2>
                </div>
                <span className="ops-header-note">Letzter Check {formatDateTime(botOverview?.lastCheck)}</span>
              </div>
              <div className="ops-source-grid">
                {systemStatusCards.map((card) => (
                  <article key={card.title} className={`ops-status-card ops-tone-${card.tone}`}>
                    <div className="ops-card-head">
                      <p className="section-title">{card.title}</p>
                      <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                    </div>
                    <h2>{card.value}</h2>
                    <p className="ops-card-copy">{card.detail}</p>
                  </article>
                ))}
              </div>
            </section>

            <section className="card ops-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">2. Betriebsrealitaet</p>
                  <h2 className="page-title">Live, vorbereitet und noch blockiert</h2>
                </div>
                <span className="ops-header-note">
                  Reader {telegramReaderRuntime?.label || 'vorbereitet'} | KI {aiResolverRuntime?.label || 'deaktiviert'} | Scheduler{' '}
                  {schedulerRuntime?.label || 'deaktiviert'}
                </span>
              </div>

              <div className="ops-source-grid">
                {operationalCards.map((card) => (
                  <article key={card.title} className={`ops-status-card ops-tone-${card.tone}`}>
                    <div className="ops-card-head">
                      <p className="section-title">{card.title}</p>
                      <span className={`status-chip ${card.tone}`}>{card.value}</span>
                    </div>
                    <h2>{card.value}</h2>
                    <p className="ops-card-copy">{card.detail}</p>
                  </article>
                ))}
              </div>

              <div className="ops-reality-grid">
                {realitySummaryCards.map((card) => (
                  <article key={card.title} className={`ops-source-card ops-tone-${card.tone}`}>
                    <div className="ops-card-top">
                      <div>
                        <span className="ops-card-label">{card.title}</span>
                        <h3>{card.items.length}</h3>
                      </div>
                      <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                    </div>
                    {card.items.length ? (
                      <div className="ops-reality-list">
                        {card.items.map((item) => (
                          <p key={`${card.title}-${item}`}>{item}</p>
                        ))}
                      </div>
                    ) : (
                      <p className="ops-card-copy">Keine Eintraege.</p>
                    )}
                  </article>
                ))}
              </div>
            </section>

            <section className="card ops-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">3. Telegram Login Status</p>
                  <h2 className="page-title">Reader Session, Login und Bot-Output</h2>
                </div>
                <span className="ops-header-note">
                  User API {telegramLoginReady ? 'konfiguriert' : 'pruefen'} | Bot {telegramBotConfigured ? 'bereit' : 'pruefen'}
                </span>
              </div>
              <div className="ops-source-grid">
                {telegramStatusCards.map((card) => (
                  <article key={card.title} className={`ops-status-card ops-tone-${card.tone}`}>
                    <div className="ops-card-head">
                      <p className="section-title">{card.title}</p>
                      <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                    </div>
                    <h2>{card.value}</h2>
                    <p className="ops-card-copy">{card.detail}</p>
                  </article>
                ))}
              </div>
            </section>

            <section className="card ops-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">4. Aktive Quellen</p>
                  <h2 className="page-title">Eingangsquellen ohne Leerlauf</h2>
                </div>
                <span className="ops-header-note">{activeSources.length} aktive Quellen geladen</span>
              </div>

              <div className="ops-source-grid">
                {sourceSummaryCards.map((card) => (
                  <article key={card.title} className={`ops-source-card ops-tone-${card.tone}`}>
                    <div className="ops-card-top">
                      <div>
                        <span className="ops-card-label">{card.title}</span>
                        <h3>{card.value}</h3>
                      </div>
                      <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                    </div>
                    <p className="ops-card-copy">{card.detail}</p>
                  </article>
                ))}
              </div>

              <div className="ops-feed">
                {activeSources.length ? (
                  activeSources.slice(0, 8).map((item) => (
                    <article key={item.id} className="ops-feed-item">
                      <div className="ops-feed-head">
                        <strong>{item.name || `Quelle ${item.id}`}</strong>
                        <span className={`status-chip ${getStatusTone(item.platform)}`}>{item.platform || 'unknown'}</span>
                      </div>
                      <p>
                        Prioritaet {toNumber(item.priority)} | Typ {item.source_type || 'manual'} | Preisregel{' '}
                        {item.pricing_rule_name || '-'}
                      </p>
                      <p>Letzter Import {formatDateTime(item.last_import_at)}</p>
                    </article>
                  ))
                ) : (
                  <p className="ops-empty-state">Keine aktiven Quellen vorhanden.</p>
                )}
              </div>
            </section>

            <section className="card ops-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">4b. Werbung</p>
                  <h2 className="page-title">Freie Werbemodule und kommende Sendungen</h2>
                </div>
                <span className="ops-header-note">{toNumber(advertisingOverview?.publishing?.queueCount)} Werbejobs in Queue</span>
              </div>

              <div className="ops-source-grid">
                {advertisingSummaryCards.map((card) => (
                  <article key={card.title} className={`ops-source-card ops-tone-${card.tone}`}>
                    <div className="ops-card-top">
                      <div>
                        <span className="ops-card-label">{card.title}</span>
                        <h3>{card.value}</h3>
                      </div>
                      <span className={`status-chip ${card.tone}`}>{card.tone}</span>
                    </div>
                    <p className="ops-card-copy">{card.detail}</p>
                  </article>
                ))}
              </div>

              <div className="ops-feed">
                {(advertisingOverview?.upcoming || []).length ? (
                  advertisingOverview.upcoming.slice(0, 6).map((item, index) => (
                    <article key={`${item.moduleId}-${item.scheduledFor}-${index}`} className="ops-feed-item">
                      <div className="ops-feed-head">
                        <strong>{item.moduleName}</strong>
                        <span className={`status-chip ${getStatusTone(item.priority)}`}>{item.priority}</span>
                      </div>
                      <p>{formatDateTime(item.scheduledFor)}</p>
                    </article>
                  ))
                ) : (
                  <p className="ops-empty-state">Keine kommenden Werbesendungen vorhanden.</p>
                )}
              </div>
            </section>

            <section className="card ops-panel ops-flow-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">5. Finaler Flow</p>
                  <h2 className="page-title">Vom Eingang bis zum Versand</h2>
                </div>
                <span className="ops-header-note">Reader -&gt; Internet -&gt; Keepa -&gt; Fake-Pattern -&gt; KI optional -&gt; Queue</span>
              </div>

              <div className="ops-flow-grid">
                {liveFlowSteps.map((step) => (
                  <article key={step.id} className={`ops-flow-column ops-tone-${step.tone}`}>
                    <div className="ops-column-head">
                      <div>
                        <p className="ops-column-kicker">{step.label}</p>
                        <h3>{step.title}</h3>
                      </div>
                      <span className={`status-chip ${step.tone}`}>{step.tone}</span>
                    </div>
                    <div className="ops-node">
                      <strong>{step.label}</strong>
                      <p>{step.detail}</p>
                    </div>
                  </article>
                ))}
              </div>
            </section>

            <section className="card ops-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">6. Queue</p>
                  <h2 className="page-title">Offene Jobs und letzte Queue-Eintraege</h2>
                </div>
                <span className="ops-header-note">{openQueueCount} Jobs aktuell offen</span>
              </div>

              <div className="ops-queue-grid">
                <article className="ops-mini-stat">
                  <span>Pending</span>
                  <strong>{queueSummary.pending}</strong>
                </article>
                <article className="ops-mini-stat">
                  <span>Sending</span>
                  <strong>{queueSummary.sending}</strong>
                </article>
                <article className="ops-mini-stat">
                  <span>Retry</span>
                  <strong>{queueSummary.retry}</strong>
                </article>
                <article className="ops-mini-stat">
                  <span>Sent</span>
                  <strong>{queueSummary.sent}</strong>
                </article>
                <article className="ops-mini-stat">
                  <span>Failed</span>
                  <strong>{queueSummary.failed}</strong>
                </article>
              </div>

              <div className="ops-feed">
                {latestQueueItems.length ? (
                  latestQueueItems.map((item) => (
                    <article key={item.id} className="ops-feed-item">
                      <div className="ops-feed-head">
                        <strong>{shortenText(item?.payload?.title || `Queue ${item.id}`, 72)}</strong>
                        <span className={`status-chip ${getStatusTone(item.status)}`}>{item.status}</span>
                      </div>
                      <p>
                        {item.source_type || 'unknown'} | Retry {toNumber(item.retry_count)} | erstellt{' '}
                        {formatDateTime(item.created_at)}
                      </p>
                      <p>
                        {(item.targets || [])
                          .map((target) => `${target.channel_type}:${target.status}`)
                          .join(' | ') || 'Keine Targets'}
                      </p>
                    </article>
                  ))
                ) : (
                  <p className="ops-empty-state">Keine offenen Queue-Eintraege vorhanden.</p>
                )}
              </div>
            </section>

            <section className="card ops-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">7. Sperren</p>
                  <h2 className="page-title">Sperrzeiten und letzte gespeicherte Locks</h2>
                </div>
                <span className="ops-header-note">
                  {repostCooldownEnabled ? `${repostCooldownHours} Stunden aktiv` : 'Sperrzeit deaktiviert'}
                </span>
              </div>

              <div className="ops-channel-stats">
                <span>Gesamt: {historyItems.length}</span>
                <span>Manuell: {lockSummary.manual}</span>
                <span>Automatisch: {lockSummary.automatic}</span>
                <span>Telegram: {lockSummary.telegram}</span>
                <span>WhatsApp: {lockSummary.whatsapp}</span>
                <span>Facebook: {lockSummary.facebook}</span>
              </div>

              <div className="ops-feed">
                {latestHistoryItems.length ? (
                  latestHistoryItems.map((item) => (
                    <article key={item.id} className="ops-feed-item">
                      <div className="ops-feed-head">
                        <strong>{shortenText(item.title || item.asin || item.normalizedUrl, 72)}</strong>
                        <span className="status-chip info">{formatDateTime(item.postedAt)}</span>
                      </div>
                      <p>
                        {item.channel || 'Kanal unbekannt'} | {item.originType || 'unknown'} | {item.sellerType || 'FBM'}
                      </p>
                      <p>{shortenText(item.normalizedUrl || item.url || item.originalUrl, 92)}</p>
                    </article>
                  ))
                ) : (
                  <p className="ops-empty-state">Noch keine Sperr-Eintraege vorhanden.</p>
                )}
              </div>
            </section>

            <section className="card ops-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">8. Letzte Deals</p>
                  <h2 className="page-title">Zuletzt verarbeitete Deals</h2>
                </div>
                <span className="ops-header-note">{latestDeals.length} Eintraege sichtbar</span>
              </div>

              <div className="ops-feed">
                {latestDeals.length ? (
                  latestDeals.map((item, index) => (
                    <article key={item.id || index} className="ops-feed-item">
                      <div className="ops-feed-head">
                        <strong>{shortenText(item.title || item.source_name || `Deal ${index + 1}`, 72)}</strong>
                        <span className={`status-chip ${getStatusTone(item.status)}`}>{item.status || 'unknown'}</span>
                      </div>
                      <p>
                        {item.platform || 'Quelle'} | {item.seller_type || 'FBM'} | Score {toNumber(item.score)} | Rabatt{' '}
                        {toNumber(item.detected_discount)}%
                      </p>
                      <p>
                        Quelle {item.source_name || '-'} | {formatDateTime(item.created_at)}
                      </p>
                    </article>
                  ))
                ) : (
                  <p className="ops-empty-state">Noch keine letzten Deals verfuegbar.</p>
                )}
              </div>
            </section>

            <section className="card ops-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">9. Fehler</p>
                  <h2 className="page-title">Aktuelle Blocker und Warnungen</h2>
                </div>
                <span className="ops-header-note">{errorEntries.length} relevante Hinweise</span>
              </div>

              <div className="ops-feed">
                {errorEntries.length ? (
                  errorEntries.map((item) => (
                    <article key={item.id} className="ops-feed-item">
                      <div className="ops-feed-head">
                        <strong>{item.category}: {item.title}</strong>
                        <span className={`status-chip ${item.tone}`}>{item.tone}</span>
                      </div>
                      <p>{shortenText(item.detail, 110)}</p>
                      <p>{formatDateTime(item.time)}</p>
                    </article>
                  ))
                ) : (
                  <p className="ops-empty-state">Aktuell keine relevanten Fehler oder Warnungen.</p>
                )}
              </div>
            </section>

            <section className="card ops-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">10. Timeline</p>
                  <h2 className="page-title">Chronologische Systemereignisse</h2>
                </div>
                <span className="ops-header-note">Deals, Quellen, Publishing und Sperren zusammengefuehrt</span>
              </div>

              <div className="ops-feed">
                {timelineItems.length ? (
                  timelineItems.map((item) => (
                    <article key={item.id} className="ops-feed-item">
                      <div className="ops-feed-head">
                        <strong>{item.category}: {shortenText(item.title, 72)}</strong>
                        <span className={`status-chip ${item.tone}`}>{formatDateTime(item.time)}</span>
                      </div>
                      <p>{shortenText(item.detail, 110)}</p>
                    </article>
                  ))
                ) : (
                  <p className="ops-empty-state">Noch keine Timeline-Eintraege vorhanden.</p>
                )}
              </div>
            </section>
          </>
        )}
      </div>
    </Layout>
  )
}

export default HomePage
