import { NavLink } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';

const mobileNav = [
  { label: 'Home', path: '/', icon: '🏠' },
  { label: 'Generator', path: '/generator', icon: '📝' },
  { label: 'Copybot', path: '/copybot', icon: '🗂' },
  { label: 'Publishing', path: '/publishing', icon: '📦' }
];

function MobileHeader() {
  const { user } = useAuth();

  return (
    <header className="mobile-header">
      <div className="top-line">
        <div className="title-block">
          <h1>Affiliate Manager</h1>
          <p>Willkommen, {user?.username}</p>
        </div>
        <span className="user-chip">{user?.role}</span>
      </div>
      <nav className="mobile-nav">
        {mobileNav.map((item) => (
          <NavLink
            key={item.path}
            to={item.path}
            className={({ isActive }) => (isActive ? 'mobile-nav-link active' : 'mobile-nav-link')}
          >
            <span>{item.icon}</span>
            <span>{item.label}</span>
          </NavLink>
        ))}
      </nav>
    </header>
  );
}

export default MobileHeader;
