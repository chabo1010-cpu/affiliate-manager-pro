import { useEffect, useRef } from 'react';
import { useLocation } from 'react-router-dom';
import Sidebar from './Sidebar';
import MobileHeader from './MobileHeader';

function getScrollableContainers(root) {
  if (!root || typeof window === 'undefined') {
    return [];
  }

  const documentScroller = document.scrollingElement;
  const candidates = [
    ...(documentScroller
      ? [
          {
            className: documentScroller.tagName.toLowerCase(),
            overflowY: window.getComputedStyle(documentScroller).overflowY,
            position: window.getComputedStyle(documentScroller).position,
            scrollHeight: documentScroller.scrollHeight,
            clientHeight: documentScroller.clientHeight
          }
        ]
      : []),
    ...Array.from(root.querySelectorAll('.app-layout, .page-container, .desktop-shell, .layout-main, .layout-sidebar-shell, .sidebar'))
      .map((node) => {
        const styles = window.getComputedStyle(node);
        return {
          className: node.className,
          overflowY: styles.overflowY,
          position: styles.position,
          scrollHeight: node.scrollHeight,
          clientHeight: node.clientHeight
        };
      })
  ];

  return candidates
    .map((node) => {
      if ('className' in node) {
        return node;
      }
      return null;
    })
    .filter(Boolean)
    .filter((entry) => ['auto', 'scroll'].includes(entry.overflowY));
}

function isExpectedDesktopScrollPair(scrollContainers = []) {
  if (scrollContainers.length !== 2) {
    return false;
  }

  const signatures = scrollContainers.map((entry) => String(entry.className || '')).sort();
  return (
    signatures.some((value) => value.includes('layout-main')) &&
    signatures.some((value) => value.includes('sidebar'))
  );
}

function Layout({ children, showSidebar = true }) {
  const layoutRef = useRef(null);
  const location = useLocation();

  useEffect(() => {
    const route = location.pathname || '/';
    const startedAt = performance.now();
    console.info('[UI_PAGE_CHECK_START]', { route });
    console.info('[UI_ROUTE_START]', { route });

    let firstFrame = 0;
    let secondFrame = 0;
    firstFrame = window.requestAnimationFrame(() => {
      secondFrame = window.requestAnimationFrame(() => {
        const durationMs = Math.round(performance.now() - startedAt);
        console.info('[UI_ROUTE_DONE]', { route, durationMs });
        console.info('[UI_PAGE_CHECK_OK]', { route, durationMs });
        if (durationMs >= 600) {
          console.warn('[UI_ROUTE_SLOW]', { route, durationMs });
        }
      });
    });

    return () => {
      window.cancelAnimationFrame(firstFrame);
      window.cancelAnimationFrame(secondFrame);
    };
  }, [location.pathname]);

  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }

    try {
      const scrollContainers = getScrollableContainers(layoutRef.current);
      console.info('[SCROLL_CONTAINER_FOUND]', {
        route: location.pathname,
        count: scrollContainers.length,
        containers: scrollContainers
      });

      if (scrollContainers.length > 1 && !isExpectedDesktopScrollPair(scrollContainers)) {
        console.warn('[SCROLL_CONTAINER_CONFLICT]', {
          route: location.pathname,
          containers: scrollContainers
        });
      }

      const desktopActive = window.innerWidth >= 1024;
      const sidebarShell = layoutRef.current?.querySelector('.layout-sidebar-shell');
      if (desktopActive && showSidebar && !sidebarShell) {
        console.error('[SIDEBAR_RENDER_ERROR]', {
          route: location.pathname,
          reason: 'sidebar_missing_on_desktop'
        });
      }

      const mainElement = layoutRef.current?.querySelector('.layout-main');
      const rootStyles = layoutRef.current ? window.getComputedStyle(layoutRef.current) : null;
      const mainStyles = mainElement ? window.getComputedStyle(mainElement) : null;
      const scaleConflict =
        Number.parseFloat(window.getComputedStyle(document.documentElement).fontSize) > 18 ||
        rootStyles?.transform !== 'none' ||
        mainStyles?.transform !== 'none';

      if (scaleConflict) {
        console.warn('[LAYOUT_SCALE_CONFLICT]', {
          route: location.pathname,
          rootTransform: rootStyles?.transform || 'none',
          mainTransform: mainStyles?.transform || 'none',
          rootFontSize: window.getComputedStyle(document.documentElement).fontSize
        });
      }
    } catch (error) {
      console.error('[UI_ROUTE_ERROR]', {
        route: location.pathname,
        errorMessage: error instanceof Error ? error.message : 'Layout-Diagnose fehlgeschlagen.'
      });
      console.error('[UI_PAGE_CHECK_ERROR]', {
        route: location.pathname,
        errorMessage: error instanceof Error ? error.message : 'Layout-Diagnose fehlgeschlagen.'
      });
    }
  }, [location.pathname, showSidebar]);

  return (
    <div className="app-layout" ref={layoutRef}>
      <MobileHeader />
      <div className="page-container">
        <div className="desktop-shell">
          {showSidebar ? (
            <aside className="layout-sidebar-shell">
              <Sidebar />
            </aside>
          ) : null}
          <main className="layout-main">{children}</main>
        </div>
      </div>
    </div>
  );
}

export default Layout;
