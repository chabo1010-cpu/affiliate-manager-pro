import Layout from '../components/layout/Layout';
import { templates } from '../data/mock';

function TemplatesPage() {
  return (
    <Layout>
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Templates</p>
          <h2 style={{ margin: '0.25rem 0 1rem', fontSize: '1.5rem' }}>Textbausteine & CTA-Management</h2>
          <button className="primary small">Neu erstellen</button>
        </section>
        <div className="responsive-grid">
          {templates.map((item) => (
            <section key={item.id} className="card" style={{ padding: '1.25rem' }}>
              <p className="text-muted">{item.type}</p>
              <h3 style={{ margin: '0.5rem 0 0.75rem' }}>{item.label}</h3>
              <p style={{ color: '#cbd5e1' }}>{item.content}</p>
              <button className="secondary small" style={{ marginTop: '1rem' }}>Bearbeiten</button>
            </section>
          ))}
        </div>
      </div>
    </Layout>
  );
}

export default TemplatesPage;
