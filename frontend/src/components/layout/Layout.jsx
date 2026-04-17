import Sidebar from './Sidebar';
import MobileHeader from './MobileHeader';

function Layout({ children, showSidebar = true }) {
  return (
    <div className="app-layout">
      <MobileHeader />
      <div className="page-container">
        <div className="desktop-shell">
          {showSidebar && <Sidebar />}
          <main className="layout-main">{children}</main>
        </div>
        <div className="mobile-shell">
          <main>{children}</main>
        </div>
      </div>
    </div>
  );
}

export default Layout;
