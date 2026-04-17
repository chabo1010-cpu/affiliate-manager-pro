function cleanText(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function toNumber(value) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value !== 'string') {
    return null;
  }

  const normalized = value.trim().replace(/[^\d,.-]/g, '').replace(',', '.');
  const parsed = Number.parseFloat(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

const COMPARISON_ADAPTERS = [
  {
    id: 'manual-source',
    name: 'Manuelle Vergleichsquelle',
    description: 'Legale, manuell gepflegte Preisquelle pro Produkt oder interner Referenzpreis.',
    supportsLiveLookup: false
  },
  {
    id: 'idealo',
    name: 'Idealo',
    description: 'Nur aktivieren, wenn spaeter eine offizielle oder zulaessige Idealo-Integration verfuegbar ist.',
    supportsLiveLookup: false
  },
  {
    id: 'custom-api',
    name: 'Weitere legale API',
    description: 'Platzhalter fuer spaetere legale Vergleichsquellen mit offizieller API.',
    supportsLiveLookup: false
  }
];

export function getComparisonAdapterCatalog(config = {}) {
  return COMPARISON_ADAPTERS.map((adapter) => {
    const adapterConfig = config?.[adapter.id] || {};

    return {
      ...adapter,
      enabled: Boolean(adapterConfig.enabled),
      status:
        adapter.id === 'manual-source'
          ? Boolean(adapterConfig.enabled)
            ? 'bereit_fuer_manuelle_pflege'
            : 'deaktiviert'
          : Boolean(adapterConfig.enabled)
            ? 'nicht_verbunden'
            : 'deaktiviert'
    };
  });
}

export async function resolveComparisonFromAdapters({ settings, existingResult }) {
  const adapterConfig = settings?.comparisonSourceConfig || {};
  const activeAdapters = getComparisonAdapterCatalog(adapterConfig).filter((adapter) => adapter.enabled);

  if (!activeAdapters.length) {
    return {
      source: null,
      status: 'not_connected',
      notes: 'Keine legale Vergleichsquelle aktiv.'
    };
  }

  for (const adapter of activeAdapters) {
    if (adapter.id === 'manual-source') {
      const manualPrice = toNumber(existingResult?.comparison_price);
      const manualSource = cleanText(existingResult?.comparison_source);

      if (manualPrice && manualSource) {
        return {
          source: manualSource,
          status: 'manual',
          price: manualPrice,
          connected: true,
          notes: 'Manuelle Vergleichsquelle wurde auf dem Treffer hinterlegt.'
        };
      }

      return {
        source: 'Manuelle Vergleichsquelle',
        status: 'not_connected',
        notes: 'Manuelle Vergleichsdaten sind fuer dieses Produkt noch nicht gepflegt.'
      };
    }

    if (adapter.id === 'idealo') {
      return {
        source: 'Idealo',
        status: 'not_connected',
        notes: 'Keine offizielle oder zulaessige Idealo-Integration konfiguriert.'
      };
    }

    if (adapter.id === 'custom-api') {
      return {
        source: 'Weitere legale API',
        status: 'not_connected',
        notes: 'Keine legale externe Vergleichs-API verbunden.'
      };
    }
  }

  return {
    source: null,
    status: 'not_connected',
    notes: 'Keine legale Vergleichsquelle verfuegbar.'
  };
}
