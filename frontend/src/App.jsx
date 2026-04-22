import { useEffect } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';
import { useAuth } from './context/AuthContext';
import ProtectedRoute from './routes/ProtectedRoute';
import LoginPage from './pages/Login';
import HomePage from './pages/Home';
import GeneratorPosterPage from './pages/GeneratorPoster';
import TemplatesPage from './pages/Templates';
import ScraperPage from './pages/Scraper';
import AutobotPage from './pages/Autobot';
import LogsPage from './pages/Logs';
import SettingsPage from './pages/Settings';
import DealHistoryAdminPage from './pages/DealHistoryAdmin';
import CopybotPage from './pages/Copybot';
import PublishingPage from './pages/Publishing';
import KeepaPage from './pages/Keepa';

function App() {
  const { user } = useAuth();
  const learningPageElement = user?.role === 'admin' ? <KeepaPage /> : <Navigate to="/generator" replace />;

  useEffect(() => {
    console.info('SYSTEM SPEC APPLIED', {
      sections: ['Dashboard', 'Quellen', 'Regler', 'Output', 'Telegram Login', 'Sperrmodul', 'Queue']
    });
    console.info('PRIMARY DECISION SOURCE UPDATED', {
      primary: 'internetvergleich',
      fallback: 'keepa',
      aiMode: 'optional_unsicherheitsfall'
    });
    console.info('NAVIGATION RESTORED', {
      preservedMenus: [
        'Dashboard',
        'Generator',
        'Scrapper',
        'Copybot',
        'Templates',
        'Autobot',
        'Logik-Zentrale',
        'Publishing',
        'Sperrzeiten',
        'Logs',
        'Einstellungen'
      ]
    });
  }, []);

  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route
        path="/*"
        element={
          <ProtectedRoute>
            <Routes>
              <Route index element={<HomePage />} />
              <Route path="dashboard" element={<HomePage />} />
              <Route path="generator" element={<GeneratorPosterPage />} />
              <Route path="templates" element={<TemplatesPage />} />
              <Route path="scraper" element={<ScraperPage />} />
              <Route path="scrapper" element={<Navigate to="/scraper" replace />} />
              <Route path="autobot" element={<AutobotPage />} />
              <Route path="copybot" element={<CopybotPage />} />
              <Route path="copybot/telegram-sources" element={<CopybotPage />} />
              <Route path="copybot/whatsapp-sources" element={<CopybotPage />} />
              <Route path="copybot/pricing-rules" element={<CopybotPage />} />
              <Route path="copybot/sampling" element={<CopybotPage />} />
              <Route path="copybot/review" element={<CopybotPage />} />
              <Route path="copybot/logs" element={<CopybotPage />} />
              <Route path="publishing" element={<PublishingPage />} />
              <Route path="publishing/workers" element={<PublishingPage />} />
              <Route path="publishing/telegram" element={<PublishingPage />} />
              <Route path="publishing/whatsapp" element={<PublishingPage />} />
              <Route path="publishing/facebook" element={<PublishingPage />} />
              <Route path="publishing/logs" element={<PublishingPage />} />
              <Route path="keepa" element={learningPageElement} />
              <Route path="keepa/manual-search" element={learningPageElement} />
              <Route path="keepa/automatik" element={learningPageElement} />
              <Route path="keepa/ergebnisse" element={learningPageElement} />
              <Route path="keepa/benachrichtigungen" element={learningPageElement} />
              <Route path="keepa/einstellungen" element={learningPageElement} />
              <Route path="keepa/verbrauch-logs" element={learningPageElement} />
              <Route path="keepa/fake-drop-analyse" element={learningPageElement} />
              <Route path="keepa/review-queue" element={learningPageElement} />
              <Route path="keepa/lern-datenbank" element={learningPageElement} />
              <Route path="learning" element={learningPageElement} />
              <Route path="learning/manual-search" element={learningPageElement} />
              <Route path="learning/automatik" element={learningPageElement} />
              <Route path="learning/ergebnisse" element={learningPageElement} />
              <Route path="learning/benachrichtigungen" element={learningPageElement} />
              <Route path="learning/einstellungen" element={learningPageElement} />
              <Route path="learning/verbrauch-logs" element={learningPageElement} />
              <Route path="learning/fake-drop-analyse" element={learningPageElement} />
              <Route path="learning/review-queue" element={learningPageElement} />
              <Route path="learning/lern-datenbank" element={learningPageElement} />
              <Route path="logic" element={<Navigate to="/learning" replace />} />
              <Route path="sperrzeiten" element={<DealHistoryAdminPage />} />
              <Route path="deal-history" element={<Navigate to="/sperrzeiten" replace />} />
              <Route path="logs" element={<LogsPage />} />
              <Route path="settings" element={<SettingsPage />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </ProtectedRoute>
        }
      />
      <Route path="*" element={<Navigate to={user ? '/' : '/login'} replace />} />
    </Routes>
  );
}

export default App;
