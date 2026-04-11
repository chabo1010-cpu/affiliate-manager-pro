import Layout from '../components/layout/Layout';

function SettingsPage() {
  return (
    <Layout>
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Einstellungen</p>
          <h2 style={{ margin: '0.25rem 0 1rem', fontSize: '1.5rem' }}>Profil & technische Platzhalter</h2>
        </section>
        <section className="card" style={{ padding: '1.25rem', display: 'grid', gap: '1rem' }}>
          <div>
            <label className="section-title">Profil</label>
            <input placeholder="Name" />
          </div>
          <div>
            <label className="section-title">API Platzhalter</label>
            <input placeholder="API Token" />
          </div>
          <div>
            <label className="section-title">Partner Tag</label>
            <input placeholder="Partner Tag" />
          </div>
          <div>
            <label className="section-title">Kanal Platzhalter</label>
            <input placeholder="Telegram / WhatsApp Kanal" />
          </div>
          <button className="primary small">Speichern</button>
        </section>
      </div>
    </Layout>
  );
}

export default SettingsPage;
