import Layout from '../components/layout/Layout';
import { team } from '../data/mock';

function TeamPage() {
  return (
    <Layout>
      <div style={{ display: 'grid', gap: '1rem' }}>
        <section className="card" style={{ padding: '1.25rem' }}>
          <p className="section-title">Team</p>
          <h2 style={{ margin: '0.25rem 0 1rem', fontSize: '1.5rem' }}>Mitarbeiterliste</h2>
        </section>
        <div style={{ display: 'grid', gap: '0.85rem' }}>
          {team.map((member) => (
            <section key={member.id} className="card" style={{ padding: '1rem', display: 'grid', gap: '0.5rem' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '1rem' }}>
                <div>
                  <h3 style={{ margin: 0 }}>{member.name}</h3>
                  <p className="text-muted" style={{ margin: '0.35rem 0 0' }}>{member.role}</p>
                </div>
                <span className={`status-chip ${member.status === 'aktiv' ? 'success' : 'warning'}`}>{member.status}</span>
              </div>
              <button className="secondary small">Bearbeiten</button>
            </section>
          ))}
        </div>
      </div>
    </Layout>
  );
}

export default TeamPage;
