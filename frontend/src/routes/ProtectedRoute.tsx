import { Navigate } from 'react-router-dom';
import { PropsWithChildren } from 'react';
import { useAuth } from '../context/AuthContext';

function ProtectedRoute({ children }: PropsWithChildren) {
  const { user } = useAuth();
  if (!user) {
    return <Navigate to="/login" replace />;
  }
  return <>{children}</>;
}

export default ProtectedRoute;
