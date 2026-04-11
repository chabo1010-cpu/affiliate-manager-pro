import Layout from '../components/layout/Layout';
import { botData } from '../data/mock';

function AutobotPage() {
  return (
    <Layout>
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Autobot</p>
          <h2 style={{ margin: '0.25rem 0 1rem', fontSize: '1.5rem' }}>Status & Queue</h2>
          <div style={{ display: 'grid', gap: '0.75rem', gridTemplateColumns: 'repeat(auto-fit, minmax(160px,1fr))' }}>
            <div className="card" style={{ padding: '1rem' }}>
              <p className="text-muted">Bot Status</p>
              <h3 style={{ marginTop: '0.5rem' }}>{botData.status}</h3>
            </div>
            <div className="card" style={{ padding: '1rem' }}>
              <p className="text-muted">Queue</p>
              <h3 style={{ marginTop: '0.5rem' }}>{botData.queue}</h3>
            </div>
            <div className="card" style={{ padding: '1rem' }}>
              <p className="text-muted">Geprüfte Einträge</p>
              <h3 style={{ marginTop: '0.5rem' }}>{botData.reviewed}</h3>
            </div>
          </div>
        </section>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Vorschlagsliste</p>
          <div style={{ display: 'grid', gap: '0.75rem' }}>
            {botData.entries.map((entry) => (
              <div key={entry.id} className="radio-card" style={{ justifyContent: 'space-between' }}>
                <span>{entry.title}</span>
                <span className="status-chip info">{entry.status}</span>
              </div>
            ))}
          </div>
        </section>
      </div>
    </Layout>
  );
}

export default AutobotPage;
