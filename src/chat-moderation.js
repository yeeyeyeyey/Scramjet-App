import { randomBytes, timingSafeEqual } from 'node:crypto';
import { dirname } from 'node:path';
import { mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';

const ID_PATTERN = /^[A-Za-z0-9_-]{8,64}$/;
const TOKEN_LIFETIME = 8 * 60 * 60 * 1000;
const LOGIN_WINDOW = 15 * 60 * 1000;

function matchesCode(candidate, expected) {
  if (!expected || !/^\d{4,64}$/.test(candidate)) return false;
  const a = Buffer.from(candidate);
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}

export class ChatModeration {
  constructor({ adminCode = '', ownerCode = '', file = '', clock = Date.now } = {}) {
    this.adminCode = String(adminCode);
    this.ownerCode = String(ownerCode);
    this.file = file;
    this.clock = clock;
    this.states = new Map();
    this.revokedAdmins = new Set();
    this.tokens = new Map();
    this.attempts = new Map();
    if (file) {
      try {
        const saved = JSON.parse(readFileSync(file, 'utf8'));
        for (const [id, state] of Object.entries(saved.users || {})) {
          if (ID_PATTERN.test(id) && state && typeof state === 'object') {
            this.states.set(id, {
              banned: state.banned === true,
              warned: state.warned === true,
              timeoutUntil: Math.max(0, Number(state.timeoutUntil) || 0),
            });
          }
        }
        for (const id of saved.revokedAdmins || []) if (ID_PATTERN.test(id)) this.revokedAdmins.add(id);
      } catch (error) {
        if (error.code !== 'ENOENT') console.error('Could not load chat moderation state:', error.message);
      }
    }
  }

  get enabled() { return !!(this.adminCode && this.ownerCode); }

  save() {
    if (!this.file) return;
    const users = Object.fromEntries(this.states);
    try {
      mkdirSync(dirname(this.file), { recursive: true });
      const temp = `${this.file}.${process.pid}.tmp`;
      writeFileSync(temp, JSON.stringify({ users, revokedAdmins: [...this.revokedAdmins] }), { mode: 0o600 });
      renameSync(temp, this.file);
    } catch (error) {
      console.error('Could not save chat moderation state:', error.message);
    }
  }

  stateFor(id) {
    const original = this.states.get(id) || {};
    const state = {
      banned: original.banned === true,
      warned: original.warned === true,
      timeoutUntil: Number(original.timeoutUntil) > this.clock() ? Number(original.timeoutUntil) : 0,
    };
    if (original.timeoutUntil && !state.timeoutUntil) {
      if (state.banned || state.warned) this.states.set(id, state);
      else this.states.delete(id);
      this.save();
    }
    return state;
  }

  allStates() {
    const result = {};
    for (const id of this.states.keys()) result[id] = this.stateFor(id);
    return result;
  }

  isRestricted(id) {
    const state = this.stateFor(id);
    return state.banned || state.timeoutUntil > this.clock();
  }

  login(id, code, remote = '') {
    if (!this.enabled) return { error: 'Admin access needs Railway environment variables.', status: 503 };
    if (!ID_PATTERN.test(id)) return { error: 'Invalid chat ID.', status: 400 };
    const now = this.clock();
    const ipKey = `ip:${String(remote).slice(0, 90)}`;
    const userKey = `user:${id}`;
    for (const key of [ipKey, userKey]) {
      const prior = (this.attempts.get(key) || []).filter(time => now - time < LOGIN_WINDOW);
      this.attempts.set(key, prior);
      if (prior.length >= (key === ipKey ? 40 : 5)) return { error: 'Too many tries. Wait 15 minutes.', status: 429 };
    }
    const role = matchesCode(String(code || ''), this.ownerCode) ? 'owner'
      : matchesCode(String(code || ''), this.adminCode) ? 'admin' : '';
    if (!role) {
      for (const key of [ipKey, userKey]) this.attempts.get(key).push(now);
      return { error: 'Incorrect code.', status: 401 };
    }
    if (role === 'admin' && this.revokedAdmins.has(id)) {
      return { error: 'The owner removed admin access for this profile.', status: 403 };
    }
    this.attempts.delete(userKey);
    const token = randomBytes(32).toString('base64url');
    this.tokens.set(token, { id, role, expires: now + TOKEN_LIFETIME });
    return { token, role };
  }

  session(token, id) {
    const entry = this.tokens.get(String(token || ''));
    if (!entry) return null;
    if (entry.expires <= this.clock()) { this.tokens.delete(token); return null; }
    if (entry.role === 'admin' && this.revokedAdmins.has(entry.id)) { this.tokens.delete(token); return null; }
    return entry.id === id ? entry : null;
  }

  roleFor(id) {
    let role = '';
    for (const [token, entry] of this.tokens) {
      if (!this.session(token, entry.id)) continue;
      if (entry.id === id) role = entry.role === 'owner' ? 'owner' : role || 'admin';
    }
    return role;
  }

  isModerator(id) { return !!this.roleFor(id); }

  adminAccess(session, action, targetId) {
    if (!session || session.role !== 'owner') return { error: 'Only the owner can manage admins.', status: 403 };
    if (!ID_PATTERN.test(targetId)) return { error: 'Select an admin.', status: 400 };
    if (targetId === session.id || this.roleFor(targetId) === 'owner') return { error: 'You cannot remove the owner.', status: 403 };
    if (action === 'revoke_admin') {
      if (this.roleFor(targetId) !== 'admin') return { error: 'That person is not an admin.', status: 400 };
      this.revokedAdmins.add(targetId);
      for (const [token, entry] of this.tokens) if (entry.id === targetId && entry.role === 'admin') this.tokens.delete(token);
    } else if (action === 'restore_admin') {
      if (!this.revokedAdmins.has(targetId)) return { error: 'Admin access was not removed.', status: 400 };
      this.revokedAdmins.delete(targetId);
    } else return { error: 'Unknown action.', status: 400 };
    this.save();
    return { revoked: this.revokedAdmins.has(targetId) };
  }

  change(session, action, targetId, minutes = 10) {
    if (!session || !['admin', 'owner'].includes(session.role)) return { error: 'Admin login required.', status: 401 };
    if (!ID_PATTERN.test(targetId)) return { error: 'Select a person.', status: 400 };
    if (targetId === session.id) return { error: 'You cannot punish yourself.', status: 403 };
    if (this.isModerator(targetId)) return { error: 'You cannot punish another admin or owner.', status: 403 };
    const state = this.stateFor(targetId);
    switch (action) {
      case 'ban': state.banned = true; break;
      case 'unban': state.banned = false; break;
      case 'warn': state.warned = true; break;
      case 'unwarn': state.warned = false; break;
      case 'timeout':
        if (!Number.isInteger(Number(minutes)) || Number(minutes) < 1 || Number(minutes) > 1440) {
          return { error: 'Pick a timeout between 1 minute and 24 hours.', status: 400 };
        }
        state.timeoutUntil = this.clock() + Number(minutes) * 60000;
        break;
      case 'untimeout': state.timeoutUntil = 0; break;
      default: return { error: 'Unknown action.', status: 400 };
    }
    if (state.banned || state.warned || state.timeoutUntil) this.states.set(targetId, state);
    else this.states.delete(targetId);
    this.save();
    return { state };
  }
}
