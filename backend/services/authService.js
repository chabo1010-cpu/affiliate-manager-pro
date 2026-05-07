import crypto from 'crypto';
import { getDb } from '../db.js';
import { getAuthConfig } from '../env.js';

const db = getDb();
const PASSWORD_HASH_PREFIX = 'scrypt';
const AUTH_ROLES = new Set(['admin', 'editor', 'poster', 'viewer']);

function nowIso() {
  return new Date().toISOString();
}

function cleanText(value = '') {
  return typeof value === 'string' ? value.trim() : '';
}

function normalizeRole(value = '') {
  const normalized = cleanText(value).toLowerCase();
  return AUTH_ROLES.has(normalized) ? normalized : 'admin';
}

function addDays(date, days) {
  return new Date(date.getTime() + days * 24 * 60 * 60 * 1000);
}

function hashToken(token = '') {
  return crypto.createHash('sha256').update(cleanText(token)).digest('hex');
}

function createPasswordHash(password = '') {
  const salt = crypto.randomBytes(16).toString('hex');
  const digest = crypto.scryptSync(password, salt, 64).toString('hex');
  return `${PASSWORD_HASH_PREFIX}:${salt}:${digest}`;
}

function verifyPassword(password = '', storedHash = '') {
  const normalizedPassword = cleanText(password);
  const normalizedHash = cleanText(storedHash);

  if (!normalizedPassword || !normalizedHash) {
    return false;
  }

  if (!normalizedHash.startsWith(`${PASSWORD_HASH_PREFIX}:`)) {
    return normalizedPassword === normalizedHash;
  }

  const [, salt, expectedDigest] = normalizedHash.split(':');
  if (!salt || !expectedDigest) {
    return false;
  }

  const actualDigest = crypto.scryptSync(normalizedPassword, salt, 64).toString('hex');
  return crypto.timingSafeEqual(Buffer.from(actualDigest, 'hex'), Buffer.from(expectedDigest, 'hex'));
}

function mapUserRow(row = {}) {
  return {
    id: Number(row.id),
    email: cleanText(row.email),
    username: cleanText(row.username),
    displayName: cleanText(row.display_name || row.displayName) || cleanText(row.username) || 'Administrator',
    role: normalizeRole(row.role)
  };
}

function purgeExpiredSessions(referenceTime = nowIso()) {
  db.prepare(`DELETE FROM auth_sessions WHERE expires_at <= ?`).run(referenceTime);
}

function readUserByIdentifier(identifier = '') {
  const normalizedIdentifier = cleanText(identifier).toLowerCase();
  if (!normalizedIdentifier) {
    return null;
  }

  return (
    db
      .prepare(
        `
          SELECT *
          FROM auth_users
          WHERE LOWER(username) = ?
             OR LOWER(email) = ?
          ORDER BY
            CASE WHEN LOWER(username) = ? THEN 0 ELSE 1 END,
            id ASC
          LIMIT 1
        `
      )
      .get(normalizedIdentifier, normalizedIdentifier, normalizedIdentifier) || null
  );
}

function ensureBootstrapAdminUser() {
  const authConfig = getAuthConfig();
  const bootstrapAdmin = authConfig.bootstrapAdmin;
  const timestamp = nowIso();
  const passwordHash = createPasswordHash(bootstrapAdmin.password);
  const existingUser =
    db
      .prepare(
        `
          SELECT *
          FROM auth_users
          WHERE LOWER(username) = LOWER(?)
             OR LOWER(email) = LOWER(?)
          ORDER BY
            CASE WHEN LOWER(username) = LOWER(?) THEN 0 ELSE 1 END,
            id ASC
          LIMIT 1
        `
      )
      .get(bootstrapAdmin.username, bootstrapAdmin.email, bootstrapAdmin.username) || null;

  if (existingUser) {
    db.prepare(
      `
        UPDATE auth_users
        SET email = ?,
            username = ?,
            password_hash = ?,
            role = 'admin',
            display_name = ?,
            is_active = 1,
            updated_at = ?
        WHERE id = ?
      `
    ).run(
      bootstrapAdmin.email,
      bootstrapAdmin.username,
      passwordHash,
      bootstrapAdmin.displayName,
      timestamp,
      existingUser.id
    );

    return (
      db.prepare(`SELECT * FROM auth_users WHERE id = ? LIMIT 1`).get(existingUser.id) || {
        ...existingUser,
        email: bootstrapAdmin.email,
        username: bootstrapAdmin.username,
        password_hash: passwordHash,
        display_name: bootstrapAdmin.displayName,
        role: 'admin',
        is_active: 1
      }
    );
  }

  const insertResult = db
    .prepare(
      `
        INSERT INTO auth_users (
          email,
          username,
          password_hash,
          role,
          display_name,
          is_active,
          created_at,
          updated_at
        ) VALUES (?, ?, ?, 'admin', ?, 1, ?, ?)
      `
    )
    .run(bootstrapAdmin.email, bootstrapAdmin.username, passwordHash, bootstrapAdmin.displayName, timestamp, timestamp);

  return db.prepare(`SELECT * FROM auth_users WHERE id = ? LIMIT 1`).get(Number(insertResult.lastInsertRowid));
}

function touchUserLogin(userId) {
  const numericUserId = Number.parseInt(String(userId ?? ''), 10);
  if (!Number.isFinite(numericUserId) || numericUserId <= 0) {
    return;
  }

  const timestamp = nowIso();
  db.prepare(
    `
      UPDATE auth_users
      SET last_login_at = ?,
          updated_at = ?
      WHERE id = ?
    `
  ).run(timestamp, timestamp, numericUserId);
}

ensureBootstrapAdminUser();

export function getAuthCookieName() {
  return getAuthConfig().cookieName;
}

export function getBootstrapAdminHint() {
  const bootstrapAdmin = getAuthConfig().bootstrapAdmin;

  return {
    email: bootstrapAdmin.email,
    username: bootstrapAdmin.username,
    displayName: bootstrapAdmin.displayName,
    password: bootstrapAdmin.password
  };
}

export function authenticateUser({ identifier = '', username = '', email = '', password = '' } = {}) {
  ensureBootstrapAdminUser();

  const resolvedIdentifier = cleanText(identifier) || cleanText(username) || cleanText(email);
  const userRow = readUserByIdentifier(resolvedIdentifier);

  if (!userRow || Number(userRow.is_active) !== 1) {
    return null;
  }

  if (!verifyPassword(password, userRow.password_hash)) {
    return null;
  }

  touchUserLogin(userRow.id);
  return mapUserRow(userRow);
}

export function createSessionForUser(user = {}, meta = {}) {
  ensureBootstrapAdminUser();
  purgeExpiredSessions();

  const numericUserId = Number.parseInt(String(user.id ?? ''), 10);
  if (!Number.isFinite(numericUserId) || numericUserId <= 0) {
    throw new Error('createSessionForUser requires a persisted user.');
  }

  const authConfig = getAuthConfig();
  const createdAt = new Date();
  const timestamp = createdAt.toISOString();
  const expiresAt = addDays(createdAt, authConfig.sessionTtlDays).toISOString();
  const token = `am_${crypto.randomBytes(24).toString('hex')}`;
  const tokenHash = hashToken(token);
  const userAgent = cleanText(meta.userAgent);
  const ipAddress = cleanText(meta.ipAddress);

  const insertResult = db
    .prepare(
      `
        INSERT INTO auth_sessions (
          user_id,
          token_hash,
          expires_at,
          last_seen_at,
          user_agent,
          ip_address,
          created_at,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `
    )
    .run(numericUserId, tokenHash, expiresAt, timestamp, userAgent, ipAddress, timestamp, timestamp);

  return {
    sessionId: Number(insertResult.lastInsertRowid),
    token,
    expiresAt,
    user: mapUserRow(user)
  };
}

export function getSessionUserByToken(token = '') {
  const normalizedToken = cleanText(token);
  if (!normalizedToken) {
    return null;
  }

  const timestamp = nowIso();
  purgeExpiredSessions(timestamp);

  const sessionRow =
    db
      .prepare(
        `
          SELECT
            s.id AS session_id,
            s.user_id,
            s.expires_at,
            u.id,
            u.email,
            u.username,
            u.role,
            u.display_name,
            u.is_active
          FROM auth_sessions s
          INNER JOIN auth_users u ON u.id = s.user_id
          WHERE s.token_hash = ?
            AND s.expires_at > ?
          LIMIT 1
        `
      )
      .get(hashToken(normalizedToken), timestamp) || null;

  if (!sessionRow || Number(sessionRow.is_active) !== 1) {
    return null;
  }

  db.prepare(
    `
      UPDATE auth_sessions
      SET last_seen_at = ?,
          updated_at = ?
      WHERE id = ?
    `
  ).run(timestamp, timestamp, sessionRow.session_id);

  return {
    sessionId: Number(sessionRow.session_id),
    expiresAt: sessionRow.expires_at,
    user: mapUserRow(sessionRow)
  };
}

export function revokeSessionByToken(token = '') {
  const normalizedToken = cleanText(token);
  if (!normalizedToken) {
    return false;
  }

  const result = db.prepare(`DELETE FROM auth_sessions WHERE token_hash = ?`).run(hashToken(normalizedToken));
  return Number(result.changes || 0) > 0;
}
