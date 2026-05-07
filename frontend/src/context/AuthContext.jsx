import { createContext, useContext, useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';

const AuthContext = createContext(null);
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';

function normalizeLoginInput(credentialsOrIdentifier, maybePassword) {
  if (typeof credentialsOrIdentifier === 'object' && credentialsOrIdentifier !== null) {
    return {
      identifier: String(
        credentialsOrIdentifier.identifier ||
          credentialsOrIdentifier.username ||
          credentialsOrIdentifier.email ||
          ''
      ).trim(),
      password: String(credentialsOrIdentifier.password || '')
    };
  }

  return {
    identifier: String(credentialsOrIdentifier || '').trim(),
    password: String(maybePassword || '')
  };
}

function getInitialState() {
  return {
    user: null,
    token: '',
    authReady: false,
    authError: ''
  };
}

async function readJsonSafely(response) {
  try {
    return await response.json();
  } catch {
    return {};
  }
}

export function AuthProvider({ children }) {
  const navigate = useNavigate();
  const [authState, setAuthState] = useState(getInitialState);

  useEffect(() => {
    let cancelled = false;

    async function restoreSession() {
      try {
        const response = await fetch(`${API_BASE_URL}/api/auth/session`, {
          credentials: 'include'
        });
        const data = await readJsonSafely(response);

        if (cancelled) {
          return;
        }

        if (response.status === 401) {
          setAuthState({
            user: null,
            token: '',
            authReady: true,
            authError: ''
          });
          return;
        }

        if (!response.ok) {
          throw new Error(data?.message || 'Sitzung konnte nicht geladen werden.');
        }

        setAuthState({
          user: data?.user || null,
          token: data?.token || '',
          authReady: true,
          authError: ''
        });
      } catch (error) {
        if (cancelled) {
          return;
        }

        setAuthState({
          user: null,
          token: '',
          authReady: true,
          authError: error instanceof Error ? error.message : 'Sitzung konnte nicht geladen werden.'
        });
      }
    }

    void restoreSession();

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    function handleUnauthorized() {
      setAuthState({
        user: null,
        token: '',
        authReady: true,
        authError: ''
      });
      navigate('/login', { replace: true });
    }

    window.addEventListener('affiliate-manager:unauthorized', handleUnauthorized);
    return () => {
      window.removeEventListener('affiliate-manager:unauthorized', handleUnauthorized);
    };
  }, [navigate]);

  async function refreshSession() {
    const response = await fetch(`${API_BASE_URL}/api/auth/session`, {
      credentials: 'include'
    });
    const data = await readJsonSafely(response);

    if (!response.ok) {
      setAuthState({
        user: null,
        token: '',
        authReady: true,
        authError: data?.message || 'Sitzung konnte nicht geladen werden.'
      });
      throw new Error(data?.message || 'Sitzung konnte nicht geladen werden.');
    }

    setAuthState({
      user: data?.user || null,
      token: '',
      authReady: true,
      authError: ''
    });

    return data?.user || null;
  }

  async function login(credentialsOrIdentifier, maybePassword) {
    const credentials = normalizeLoginInput(credentialsOrIdentifier, maybePassword);
    const response = await fetch(`${API_BASE_URL}/api/auth/login`, {
      method: 'POST',
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        identifier: credentials.identifier,
        username: credentials.identifier,
        password: credentials.password
      })
    });

    const data = await readJsonSafely(response);
    if (!response.ok) {
      const message = data?.message || 'Login fehlgeschlagen.';
      setAuthState((current) => ({
        ...current,
        authReady: true,
        authError: message
      }));
      throw new Error(message);
    }

    setAuthState({
      user: data?.user || null,
      token: data?.token || '',
      authReady: true,
      authError: ''
    });
    navigate('/', { replace: true });

    return data?.user || null;
  }

  async function logout() {
    try {
      await fetch(`${API_BASE_URL}/api/auth/logout`, {
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json'
        }
      });
    } catch {
      // Lokales Logout darf auch dann funktionieren, wenn das Backend kurz nicht erreichbar ist.
    } finally {
      setAuthState({
        user: null,
        token: '',
        authReady: true,
        authError: ''
      });
      navigate('/login', { replace: true });
    }
  }

  const value = useMemo(
    () => ({
      user: authState.user,
      token: authState.token,
      authReady: authState.authReady,
      authError: authState.authError,
      login,
      logout,
      refreshSession,
      isAdmin: authState.user?.role === 'admin'
    }),
    [authState]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider');
  }
  return context;
}
