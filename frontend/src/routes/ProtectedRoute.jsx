import { Navigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';

function ProtectedRoute({ children }) {
  const { user, authReady } = useAuth();

  if (!authReady) {
    return <div style={{ padding: '1.25rem' }}>Sitzung wird geladen...</div>;
  }

  if (!user) {
    return <Navigate to="/login" replace />;
  }
  return <>{children}</>;
}

export default ProtectedRoute;
