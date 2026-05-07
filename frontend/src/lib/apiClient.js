export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000';

function resolveBackendOrigin() {
  if (typeof window === 'undefined') {
    return API_BASE_URL;
  }

  try {
    return new URL(API_BASE_URL, window.location.origin).origin;
  } catch {
    return API_BASE_URL;
  }
}

function resolveRequestUrl(input) {
  if (typeof window === 'undefined') {
    return null;
  }

  try {
    if (typeof input === 'string') {
      return new URL(input, window.location.origin);
    }

    if (input instanceof URL) {
      return input;
    }

    if (input && typeof input.url === 'string') {
      return new URL(input.url, window.location.origin);
    }
  } catch {}

  return null;
}

export function isBackendRequest(input) {
  const requestUrl = resolveRequestUrl(input);
  if (!requestUrl) {
    return false;
  }

  return requestUrl.origin === resolveBackendOrigin();
}

export function installBackendFetchBridge() {
  if (typeof window === 'undefined' || window.__affiliateManagerFetchBridgeInstalled === true) {
    return;
  }

  const originalFetch = window.fetch.bind(window);

  window.fetch = async (input, init = {}) => {
    const requestUrl = resolveRequestUrl(input);
    const shouldAttachCredentials = isBackendRequest(input);
    const nextInit = shouldAttachCredentials
      ? {
          ...init,
          credentials: 'include'
        }
      : init;
    const response = await originalFetch(input, nextInit);

    if (
      shouldAttachCredentials &&
      response.status === 401 &&
      requestUrl &&
      !requestUrl.pathname.startsWith('/api/auth/')
    ) {
      window.dispatchEvent(new CustomEvent('affiliate-manager:unauthorized'));
    }

    return response;
  };

  window.__affiliateManagerFetchBridgeInstalled = true;
}
