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
import TeamPage from './pages/Team';
import SettingsPage from './pages/Settings';

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
              <Route path="logs" element={<LogsPage />} />
              <Route path="team" element={<TeamPage />} />
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
