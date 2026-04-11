import Layout from '../components/layout/Layout';
import { logs } from '../data/mock';

function LogsPage() {
  return (
    <Layout>
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Logs</p>
          <h2 style={{ margin: '0.25rem 0 1rem', fontSize: '1.5rem' }}>Aktionen & Historie</h2>
        </section>
        <div style={{ display: 'grid', gap: '0.85rem' }}>
          {logs.map((item) => (
            <section key={item.id} className="card" style={{ padding: '1rem', display: 'grid', gap: '0.5rem' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: '1rem', alignItems: 'center' }}>
                <h3 style={{ margin: 0, fontSize: '1rem' }}>{item.action}</h3>
                <span className={`status-chip ${item.status === 'erfolgreich' ? 'success' : item.status === 'pending' ? 'warning' : 'danger'}`}>{item.status}</span>
              </div>
              <p className="text-muted" style={{ margin: 0 }}>{item.user} · {item.time}</p>
            </section>
          ))}
        </div>
      </div>
    </Layout>
  );
}

export default LogsPage;
