import { useEffect, useMemo, useState } from 'react';
import { useAuth } from '../../context/AuthContext';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';
const MIN_GROUP_SLOTS = 10;

function createEmptySlot(slotIndex) {
  return {
    id: null,
    slotIndex,
    name: '',
    username: '',
    enabled: false,
    status: 'leer'
  };
}

function normalizeGroupsPayload(data = {}) {
  const slotCount = Math.max(MIN_GROUP_SLOTS, Number(data.slotCount || MIN_GROUP_SLOTS));
  const items = Array.isArray(data.items) ? data.items : [];
  const slotMap = new Map(items.map((item) => [Number(item.slotIndex || 0), item]));

  return {
    sessionName: data.sessionName || 'default-user',
    maxSlots: Number(data.maxSlots || 100),
    slotCount,
    stats: {
      activeCount: Number(data.stats?.activeCount || 0),
      configuredCount: Number(data.stats?.configuredCount || 0),
      visibleSlots: Number(data.stats?.visibleSlots || slotCount),
      activeSessionName: data.stats?.activeSessionName || '',
      sessionStatus: data.stats?.sessionStatus || 'disconnected'
    },
    items: Array.from({ length: slotCount }, (_, index) => {
      const slotIndex = index + 1;
      const item = slotMap.get(slotIndex) || createEmptySlot(slotIndex);

      return {
        id: item.id || null,
        slotIndex,
        name: item.name || '',
        username: item.username || '',
        enabled: item.enabled === true,
        status: item.status || 'leer'
      };
    })
  };
}

function getSlotStatus(item) {
  if (!item?.name?.trim()) {
    return { label: 'leer', className: 'info' };
  }

  if (!item.enabled) {
    return { label: 'inaktiv', className: 'warning' };
  }

  if (!item?.username?.trim()) {
    return { label: 'unvollstaendig', className: 'warning' };
  }

  return { label: 'aktiv', className: 'success' };
}

function TelegramGroupManager({ onStatusChange, onSessionNameChange }) {
  const { user } = useAuth();
  const isAdmin = user?.role === 'admin';
  const [groupsData, setGroupsData] = useState(() => normalizeGroupsPayload());
  const [loading, setLoading] = useState(isAdmin);
  const [saving, setSaving] = useState(false);
  const [dirty, setDirty] = useState(false);

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

  async function loadGroups() {
    if (!isAdmin) {
      return;
    }

    setLoading(true);

    try {
      const data = await apiFetch('/api/telegram/user-client/groups');
      const normalizedData = normalizeGroupsPayload(data);
      setGroupsData(normalizedData);
      onSessionNameChange?.(normalizedData.sessionName);
      setDirty(false);
    } catch (error) {
      onStatusChange?.(error instanceof Error ? error.message : 'Telegram Gruppen konnten nicht geladen werden.');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadGroups();
  }, [isAdmin, user?.role]);

  const summary = useMemo(() => {
    const activeCount = groupsData.items.filter((item) => item.enabled && item.name.trim() && item.username.trim()).length;
    const configuredCount = groupsData.items.filter((item) => item.name.trim()).length;

    return {
      activeCount,
      configuredCount
    };
  }, [groupsData.items]);

  function updateSlot(slotIndex, patch) {
    setGroupsData((prev) => ({
      ...prev,
      items: prev.items.map((item) => (item.slotIndex === slotIndex ? { ...item, ...patch } : item))
    }));
    setDirty(true);
  }

  function handleAddSlot() {
    setGroupsData((prev) => {
      if (prev.slotCount >= prev.maxSlots) {
        return prev;
      }

      const nextSlotCount = prev.slotCount + 1;
      return {
        ...prev,
        slotCount: nextSlotCount,
        items: [...prev.items, createEmptySlot(nextSlotCount)]
      };
    });
    setDirty(true);
  }

  function handleToggleAll(enabled) {
    setGroupsData((prev) => ({
      ...prev,
      items: prev.items.map((item) => ({ ...item, enabled }))
    }));
    setDirty(true);
  }

  async function handleSave() {
    if (!isAdmin) {
      return;
    }

    setSaving(true);

    try {
      const data = await apiFetch('/api/telegram/user-client/groups', {
        method: 'PUT',
        body: JSON.stringify({
          slotCount: groupsData.slotCount,
          items: groupsData.items.map((item) => ({
            id: item.id,
            slotIndex: item.slotIndex,
            name: item.name,
            username: item.username,
            enabled: item.enabled
          }))
        })
      });
      const normalizedData = normalizeGroupsPayload(data);
      setGroupsData(normalizedData);
      onSessionNameChange?.(normalizedData.sessionName);
      setDirty(false);
      onStatusChange?.('Telegram Gruppen gespeichert.');
    } catch (error) {
      onStatusChange?.(error instanceof Error ? error.message : 'Telegram Gruppen konnten nicht gespeichert werden.');
    } finally {
      setSaving(false);
    }
  }

  if (!isAdmin) {
    return null;
  }

  if (loading) {
    return (
      <section className="card" style={{ padding: '1.25rem' }}>
        Telegram Gruppen werden geladen...
      </section>
    );
  }

  return (
    <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '1rem' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
        <div style={{ display: 'grid', gap: '0.35rem' }}>
          <div>
            <p className="section-title">Telegram Gruppen</p>
            <h2 style={{ margin: '0.25rem 0 0.35rem', fontSize: '1.35rem' }}>Einfache Gruppenverwaltung</h2>
            <p className="text-muted" style={{ margin: 0 }}>
              Nur aktivierte und befuellte Gruppen werden vom Reader gelesen. Ein Name ohne Username oder Link bleibt sichtbar, wird aber noch ignoriert.
            </p>
          </div>
          <div style={{ display: 'flex', gap: '0.65rem', flexWrap: 'wrap' }}>
            <span className="status-chip info">Aktive Gruppen: {summary.activeCount} / {groupsData.slotCount}</span>
            <span className="status-chip info">Gesamt konfiguriert: {summary.configuredCount}</span>
            <span
              className={`status-chip ${
                ['connected', 'watching', 'active'].includes(groupsData.stats.sessionStatus) ? 'success' : 'warning'
              }`}
            >
              Session: {groupsData.sessionName} ({groupsData.stats.sessionStatus})
            </span>
          </div>
        </div>

        <div style={{ display: 'flex', gap: '0.65rem', flexWrap: 'wrap', alignItems: 'start' }}>
          <button className="secondary" onClick={handleAddSlot} disabled={saving || groupsData.slotCount >= groupsData.maxSlots}>
            Neue Gruppe hinzufuegen
          </button>
          <button className="secondary" onClick={() => handleToggleAll(true)} disabled={saving}>
            Alle aktivieren
          </button>
          <button className="secondary" onClick={() => handleToggleAll(false)} disabled={saving}>
            Alle deaktivieren
          </button>
          <button className="primary" onClick={() => void handleSave()} disabled={saving || !dirty}>
            {saving ? 'Speichert...' : 'Speichern'}
          </button>
        </div>
      </div>

      <div
        style={{
          display: 'grid',
          gap: '0.75rem',
          maxHeight: '32rem',
          overflowY: 'auto',
          paddingRight: '0.2rem',
          overscrollBehavior: 'contain'
        }}
      >
        {groupsData.items.map((item) => {
          const slotStatus = getSlotStatus(item);

          return (
            <div
              key={item.slotIndex}
              className="radio-card"
              style={{
                display: 'grid',
                gap: '0.75rem',
                alignItems: 'center',
                gridTemplateColumns: 'minmax(0, 1fr)'
              }}
            >
              <div
                style={{
                  display: 'grid',
                  gap: '0.75rem',
                  gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
                  alignItems: 'center'
                }}
              >
                <label className="checkbox-card" style={{ minWidth: 0 }}>
                  <span>Gruppe {item.slotIndex}</span>
                  <input
                    type="checkbox"
                    checked={item.enabled}
                    onChange={(event) => updateSlot(item.slotIndex, { enabled: event.target.checked })}
                  />
                </label>

                <input
                  placeholder="Name eingeben"
                  value={item.name}
                  onChange={(event) => updateSlot(item.slotIndex, { name: event.target.value })}
                />

                <input
                  placeholder="@username oder Link optional"
                  value={item.username}
                  onChange={(event) => updateSlot(item.slotIndex, { username: event.target.value })}
                />

                <span className={`status-chip ${slotStatus.className}`}>{slotStatus.label}</span>
              </div>
            </div>
          );
        })}
      </div>

      <p className="text-muted" style={{ margin: 0 }}>
        Zuerst werden 10 Slots angezeigt. Weitere Gruppen koennen bis maximal {groupsData.maxSlots} Slots ergaenzt werden.
      </p>
    </section>
  );
}

export default TelegramGroupManager;
