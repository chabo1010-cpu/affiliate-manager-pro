import { Link } from 'react-router-dom';
import Layout from '../components/layout/Layout';
import { quickLinks, homeCards } from '../data/mock';

function HomePage() {
  return (
    <Layout>
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', gap: '1rem' }}>
            <div>
              <p className="section-title">Dashboard</p>
              <h2 style={{ margin: '0.25rem 0 0', fontSize: '1.6rem' }}>Schnelle Übersicht</h2>
            </div>
            <span className="badge">Mobile-First & Telegram/WhatsApp</span>
          </div>
        </section>
        <div className="responsive-grid">
          {homeCards.map((card) => (
            <section key={card.title} className="card" style={{ padding: '1.25rem' }}>
              <p className="section-title">{card.title}</p>
              <h3 style={{ margin: '0.5rem 0 0', fontSize: '2rem' }}>{card.value}</h3>
              <p className="text-muted" style={{ marginTop: '0.8rem' }}>{card.subtitle}</p>
            </section>
          ))}
        </div>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Schnellzugriffe</p>
          <div style={{ display: 'grid', gap: '0.75rem' }}>
            {quickLinks.map((item) => (
              <Link key={item.title} to={item.path} className="radio-card" style={{ justifyContent: 'space-between' }}>
                <span>{item.title}</span>
                <span>{item.icon}</span>
              </Link>
            ))}
          </div>
        </section>
      </div>
    </Layout>
  );
}

export default HomePage;
