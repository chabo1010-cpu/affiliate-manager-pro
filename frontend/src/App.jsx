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

  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route
        path="/*"
        element={
          <ProtectedRoute>
            <Routes>
              <Route index element={<HomePage />} />
              <Route path="generator" element={<GeneratorPosterPage />} />
              <Route path="templates" element={<TemplatesPage />} />
              <Route path="scraper" element={<ScraperPage />} />
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
              <Route path="publishing/facebook" element={<PublishingPage />} />
              <Route path="publishing/logs" element={<PublishingPage />} />
              <Route path="keepa" element={<KeepaPage />} />
              <Route path="keepa/manual-search" element={<KeepaPage />} />
              <Route path="keepa/automatik" element={<KeepaPage />} />
              <Route path="keepa/ergebnisse" element={<KeepaPage />} />
              <Route path="keepa/benachrichtigungen" element={<KeepaPage />} />
              <Route path="keepa/einstellungen" element={<KeepaPage />} />
              <Route path="keepa/verbrauch-logs" element={<KeepaPage />} />
              <Route path="keepa/fake-drop-analyse" element={<KeepaPage />} />
              <Route path="keepa/review-queue" element={<KeepaPage />} />
              <Route path="keepa/lern-datenbank" element={<KeepaPage />} />
              <Route path="logs" element={<LogsPage />} />
              <Route path="deal-history" element={<DealHistoryAdminPage />} />
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
