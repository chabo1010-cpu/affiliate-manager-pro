import { PropsWithChildren } from 'react';
import Sidebar from './Sidebar';
import MobileHeader from './MobileHeader';

interface LayoutProps extends PropsWithChildren {
  showSidebar?: boolean;
}

function Layout({ children, showSidebar = true }: LayoutProps) {
  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column', gap: '1rem' }}>
      <MobileHeader />
      <div className="page-container" style={{ padding: '1rem', maxWidth: '1440px', margin: '0 auto' }}>
        <div className="desktop-shell">
          {showSidebar && <Sidebar />}
          <main style={{ width: '100%' }}>{children}</main>
        </div>
        <div className="mobile-shell">
          <main>{children}</main>
        </div>
      </div>
    </div>
  );
}

export default Layout;
