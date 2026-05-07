import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'affiliate-auth-service-'));
process.env.APP_DATA_DIR = tempRoot;
process.env.APP_DB_PATH = path.join(tempRoot, 'deals.db');
process.env.AUTH_BOOTSTRAP_ADMIN_USERNAME = 'admin';
process.env.AUTH_BOOTSTRAP_ADMIN_PASSWORD = 'Admin12345!';

const {
  authenticateUser,
  createSessionForUser,
  getBootstrapAdminHint,
  getSessionUserByToken,
  revokeSessionByToken
} = await import('../services/authService.js');

function test(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

test('Admin kann sich mit korrekten Zugangsdaten anmelden', () => {
  const result = authenticateUser({
    identifier: 'admin',
    password: 'Admin12345!'
  });

  assert.equal(result?.role, 'admin');
  assert.equal(result?.username, 'admin');
});

test('Falsche Zugangsdaten werden abgelehnt', () => {
  const result = authenticateUser({
    identifier: 'admin',
    password: 'falsch'
  });

  assert.equal(result, null);
});

test('Bootstrap-Hinweis liefert den erwarteten Admin', () => {
  const hint = getBootstrapAdminHint();

  assert.equal(hint.username, 'admin');
  assert.equal(hint.password, 'Admin12345!');
});

test('Session kann erstellt, gelesen und widerrufen werden', () => {
  const user = authenticateUser({
    identifier: 'admin',
    password: 'Admin12345!'
  });

  assert.ok(user, 'Bootstrap-Admin sollte authentifizierbar sein.');

  const session = createSessionForUser(user, {
    userAgent: 'auth-service-test',
    ipAddress: '127.0.0.1'
  });

  assert.match(session.token, /^am_/);
  assert.equal(getSessionUserByToken(session.token)?.user.username, 'admin');
  assert.equal(revokeSessionByToken(session.token), true);
  assert.equal(getSessionUserByToken(session.token), null);
});

console.log('OK Auth Service Session-Flow getestet');
