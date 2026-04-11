import { createContext, PropsWithChildren, useContext, useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';

interface User {
  username: string;
  role: string;
}

interface AuthContextType {
  user: User | null;
  login: (username: string) => void;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType | null>(null);

const mockUsers: User[] = [
  { username: 'admin', role: 'admin' },
  { username: 'editor', role: 'editor' },
  { username: 'poster', role: 'poster' },
  { username: 'viewer', role: 'viewer' }
];

export function AuthProvider({ children }: PropsWithChildren) {
  const navigate = useNavigate();
  const [user, setUser] = useState<User | null>(() => {
    const raw = localStorage.getItem('affiliatemanager-user');
    return raw ? (JSON.parse(raw) as User) : null;
  });

  useEffect(() => {
    if (user) {
      localStorage.setItem('affiliatemanager-user', JSON.stringify(user));
    } else {
      localStorage.removeItem('affiliatemanager-user');
    }
  }, [user]);

  const login = (username: string) => {
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

  const value = useMemo(() => ({ user, login, logout }), [user]);

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider');
  }
  return context;
}
