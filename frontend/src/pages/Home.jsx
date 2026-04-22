import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import Layout from '../components/layout/Layout'
import { useAuth } from '../context/AuthContext'
import './Home.css'

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000'

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

function HomePage() {
  const { user } = useAuth()
  const [dashboard, setDashboard] = useState({
    amazon: null,
    bot: null,
    copybot: null,
    keepaStatus: null,
    queue: null,
    logs: null,
    workers: null,
    repostSettings: null,
    history: null,
    sources: null,
    copybotLogs: null
  })
  const [loading, setLoading] = useState(true)
  const [status, setStatus] = useState('')

  useEffect(() => {
    let cancelled = false

    async function apiFetch(path) {
      const response = await fetch(`${API_BASE_URL}${path}`, {
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
    }

    async function loadDashboard() {
      try {
        setLoading(true)
        setStatus('')

        const results = await Promise.allSettled([
          apiFetch('/api/bot'),
          apiFetch('/api/copybot/overview'),
          apiFetch('/api/keepa/status'),
          apiFetch('/api/amazon/status'),
          apiFetch('/api/publishing/queue'),
          apiFetch('/api/publishing/logs'),
          apiFetch('/api/publishing/workers/status'),
          apiFetch('/api/deals/settings'),
          apiFetch('/api/deals/history'),
          apiFetch('/api/copybot/sources'),
          apiFetch('/api/copybot/logs')
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
          copybotLogsResult
        ] = results

        const partialErrors = results
          .filter((item) => item.status === 'rejected')
          .map((item) => (item.reason instanceof Error ? item.reason.message : 'Ein Teilbereich konnte nicht geladen werden.'))

        if (!cancelled) {
          setDashboard({
            bot: botResult.status === 'fulfilled' ? botResult.value : null,
            copybot: copybotResult.status === 'fulfilled' ? copybotResult.value : null,
            keepaStatus: keepaResult.status === 'fulfilled' ? keepaResult.value : null,
            amazon: amazonResult.status === 'fulfilled' ? amazonResult.value : null,
            queue: queueResult.status === 'fulfilled' ? queueResult.value : null,
            logs: logsResult.status === 'fulfilled' ? logsResult.value : null,
            workers: workersResult.status === 'fulfilled' ? workersResult.value : null,
            repostSettings: repostSettingsResult.status === 'fulfilled' ? repostSettingsResult.value : null,
            history: historyResult.status === 'fulfilled' ? historyResult.value : null,
            sources: sourcesResult.status === 'fulfilled' ? sourcesResult.value : null,
            copybotLogs: copybotLogsResult.status === 'fulfilled' ? copybotLogsResult.value : null
          })

          if (partialErrors.length) {
            setStatus(`Monitoring teilweise unvollstaendig: ${partialErrors[0]}`)
          }
        }
      } catch (error) {
        if (!cancelled) {
          setStatus(error instanceof Error ? error.message : 'Dashboard konnte nicht geladen werden.')
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
      value: 'Internet primaer',
      detail: `Marktvergleich fuehrt. Keepa bleibt Backup ab ${keepaGapPct || 0}% Mindestabstand.`,
      tone: 'success'
    },
    {
      title: 'Output Integrationen',
      value: `${telegramBotConfigured ? 'Telegram bereit' : 'Telegram pruefen'} / ${whatsappModule?.configured ? 'WhatsApp bereit' : 'WhatsApp pruefen'}`,
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

  const telegramStatusCards = [
    {
      title: 'Login Status',
      value: toNumber(telegramUserApi?.activeSessions) > 0 ? 'Verbunden' : telegramLoginReady ? 'Bereit' : 'Konfiguration fehlt',
      detail: `API ${telegramUserApi?.apiConfigured ? 'konfiguriert' : 'nicht konfiguriert'} | Reader ${
        telegramUserApi?.enabled ? 'aktiv' : 'deaktiviert'
      }`,
      tone: toNumber(telegramUserApi?.activeSessions) > 0 ? 'success' : telegramLoginReady ? 'info' : 'warning'
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

  const liveFlowSteps = [
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
      id: 'lock',
      label: 'Sperrcheck',
      title: repostCooldownEnabled ? `${repostCooldownHours}h aktiv` : 'deaktiviert',
      detail: `${historyItems.length} gespeicherte Sperren fuer manuelle und automatische Posts`,
      tone: repostCooldownEnabled ? 'success' : 'warning'
    },
    {
      id: 'internet',
      label: 'Internetvergleich',
      title: 'Hauptentscheidung',
      detail: `Amazon ${amazonApiStatus} | Mindestabstand ${keepaGapPct || 0}% fuer starke Marktdeals`,
      tone: 'success'
    },
    {
      id: 'fallback',
      label: 'Keepa Fallback',
      title: keepaEnabled ? 'bereit' : 'deaktiviert',
      detail: 'avg90, avg180, min90 und Lowest90 bleiben reine Backup-Signale.',
      tone: keepaEnabled ? 'info' : 'warning'
    },
    {
      id: 'queue',
      label: 'Queue und Output',
      title: `${openQueueCount} offen`,
      detail: `${queueSummary.pending} pending | ${queueSummary.sending} sending | ${queueSummary.retry} retry | ${queueSummary.sent} sent`,
      tone: publishingTone
    }
  ]

  const workspaceCards = [
    {
      title: 'Dashboard',
      path: '/',
      description: 'Status, Timeline und Systemsicht'
    },
    {
      title: 'Generator',
      path: '/generator',
      description: 'Manueller Deal-Workflow'
    },
    {
      title: 'Scrapper',
      path: '/scraper',
      description: 'Deal-Eingang und Pruefung'
    },
    {
      title: 'Copybot',
      path: '/copybot',
      description: 'Telegram- und WhatsApp-Quellen'
    },
    {
      title: 'Templates',
      path: '/templates',
      description: 'Vorlagen und Textbausteine'
    },
    {
      title: 'Autobot',
      path: '/autobot',
      description: 'Integrationen, Queue und Plattformstatus'
    },
    {
      title: 'Logik-Zentrale',
      path: '/learning',
      description: 'Keepa, Marktvergleich und Lernlogik',
      adminOnly: true
    },
    {
      title: 'Publishing',
      path: '/publishing',
      description: 'Queue, Worker und Versand'
    },
    {
      title: 'Sperrzeiten',
      path: '/sperrzeiten',
      description: 'Repost-Sperre und Verlauf'
    },
    {
      title: 'Logs',
      path: '/logs',
      description: 'Aktionen und Historie'
    },
    {
      title: 'Einstellungen',
      path: '/settings',
      description: 'System und Ausgabe konfigurieren'
    }
  ]

  return (
    <Layout>
      <div className="ops-home">
        <section className="card ops-hero">
          <div className="ops-hero-grid">
            <div className="ops-hero-copy">
              <p className="section-title">Affiliate Manager Pro</p>
              <h1 className="page-title">System Dashboard</h1>
              <p className="page-subtitle">
                Uebersichtlich nach Arbeitsfluss geordnet: Systemstatus, Telegram Login, aktive Quellen, Live Flow,
                Queue, Sperren, letzte Deals, Fehler und Timeline. Internet bleibt Hauptlogik, Keepa bleibt Fallback,
                Queue und Sperrmodul bleiben durchgaengig aktiv.
              </p>
            </div>

            <div className="ops-hero-aside">
              <div className="ops-hero-chip-row">
                <span className="badge">Internet zuerst - Keepa nur Fallback - Queue ohne Verlust</span>
                <span className={`status-chip ${user?.role === 'admin' ? 'info' : 'success'}`}>
                  {user?.role === 'admin' ? 'Admin Workspace' : 'Workspace View'}
                </span>
              </div>

              <div className="ops-quick-links">
                {workspaceCards.filter((item) => !item.adminOnly || user?.role === 'admin').map((item) => (
                  <Link key={item.title} to={item.path} className="ops-quick-link">
                    <span>{item.title}</span>
                    <small>{item.description}</small>
                  </Link>
                ))}
              </div>
            </div>
          </div>

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
                  <p className="section-title">2. Telegram Login Status</p>
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
                  <p className="section-title">3. Aktive Quellen</p>
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

            <section className="card ops-panel ops-flow-panel">
              <div className="ops-panel-header">
                <div>
                  <p className="section-title">4. Live Flow</p>
                  <h2 className="page-title">Vom Eingang bis zum Versand</h2>
                </div>
                <span className="ops-header-note">Deal -&gt; Sperre -&gt; Internet -&gt; Keepa -&gt; Queue</span>
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
                  <p className="section-title">5. Queue</p>
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
                  <p className="section-title">6. Sperren</p>
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
                  <p className="section-title">7. Letzte Deals</p>
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
                  <p className="section-title">8. Fehler</p>
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
                  <p className="section-title">9. Timeline</p>
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
