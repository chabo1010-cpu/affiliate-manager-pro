import { createContext, useContext, useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';

const AuthContext = createContext(null);

const mockUsers = [
  { username: 'admin', role: 'admin' },
  { username: 'editor', role: 'editor' },
  { username: 'poster', role: 'poster' },
  { username: 'viewer', role: 'viewer' }
];

export function AuthProvider({ children }) {
  const navigate = useNavigate();
  const [user, setUser] = useState(() => {
    const raw = localStorage.getItem('affiliatemanager-user');
    return raw ? JSON.parse(raw) : null;
  });

  useEffect(() => {
    if (user) {
      localStorage.setItem('affiliatemanager-user', JSON.stringify(user));
    } else {
      localStorage.removeItem('affiliatemanager-user');
    }
  }, [user]);

  const login = (username) => {
    const found = mockUsers.find((item) => item.username === username);
    if (found) {
      setUser(found);
      navigate('/');
    }
  };

  const logout = () => {
    setUser(null);
    navigate('/login');
  };

  const value = useMemo(() => ({ user, login, logout }), [user, navigate]);

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider');
  }
  return context;
}
