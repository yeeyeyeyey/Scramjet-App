import { createHash, randomBytes, randomInt, timingSafeEqual } from 'node:crypto';
import { dirname } from 'node:path';
import { mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';

const ID_PATTERN = /^[A-Za-z0-9_-]{8,64}$/;
const TOKEN_LIFETIME = 8 * 60 * 60 * 1000;
const LOGIN_WINDOW = 15 * 60 * 1000;
const OWNER_REQUEST_LIFETIME = 10 * 60 * 1000;
const DEVICE_KEY_PATTERN = /^[a-f0-9]{64}$/;

function digest(value) { return createHash('sha256').update(String(value)).digest('hex'); }

function matchesCode(candidate, expected) {
  if (!expected || !/^\d{4,64}$/.test(candidate)) return false;
  const a = Buffer.from(candidate);
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}

function matchesCodeHash(candidate, expected) {
  if (!/^[a-f0-9]{64}$/.test(expected)) return false;
  return timingSafeEqual(Buffer.from(candidate, 'hex'), Buffer.from(expected, 'hex'));
}

export class ChatModeration {
  constructor({ adminCode = '', ownerCode = '', file = '', clock = Date.now } = {}) {
    this.adminCode = String(adminCode);
    this.ownerCode = String(ownerCode);
    this.initialAdminCode = this.adminCode;
    this.initialOwnerCode = this.ownerCode;
    this.file = file;
    this.clock = clock;
    this.states = new Map();
    this.revokedAdmins = new Set();
    this.ownerDevices = new Map();
    this.primaryOwnerId = '';
    this.ownerRequests = new Map();
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
        if (saved.codeBase?.admin === digest(this.initialAdminCode) && /^\d{8,12}$/.test(saved.codes?.admin || '')) this.adminCode = saved.codes.admin;
        const ownerBaseMatches = !saved.codeBase?.owner || saved.codeBase.owner === digest(this.initialOwnerCode);
        if (ownerBaseMatches && /^\d{8,12}$/.test(saved.codes?.owner || '')) this.ownerCode = saved.codes.owner;
        // Changing the Railway owner code is the recovery path if the only owner loses their device key.
        if (ownerBaseMatches) for (const [id, hash] of Object.entries(saved.ownerDevices || {})) {
          if (ID_PATTERN.test(id) && /^[a-f0-9]{64}$/.test(hash)) this.ownerDevices.set(id, hash);
        }
        if (this.ownerDevices.size) this.primaryOwnerId = this.ownerDevices.has(saved.primaryOwnerId)
          ? saved.primaryOwnerId : this.ownerDevices.keys().next().value;
      } catch (error) {
        if (error.code !== 'ENOENT') console.error('Could not load chat moderation state:', error.message);
      }
    }
  }

  get enabled() { return !!(this.adminCode && this.ownerCode); }

  save() {
    if (!this.file) return true;
    const users = Object.fromEntries(this.states);
    try {
      mkdirSync(dirname(this.file), { recursive: true });
      const temp = `${this.file}.${process.pid}.tmp`;
      writeFileSync(temp, JSON.stringify({
        users, revokedAdmins: [...this.revokedAdmins], ownerDevices: Object.fromEntries(this.ownerDevices),
        primaryOwnerId: this.primaryOwnerId,
        codes: {
          admin: this.adminCode === this.initialAdminCode ? '' : this.adminCode,
          owner: this.ownerCode === this.initialOwnerCode ? '' : this.ownerCode,
        },
        codeBase: { admin: digest(this.initialAdminCode), owner: digest(this.initialOwnerCode) },
      }), { mode: 0o600 });
      renameSync(temp, this.file);
      return true;
    } catch (error) {
      console.error('Could not save chat moderation state:', error.message);
      return false;
    }
  }

  nextCode(otherCode) {
    let code;
    do { code = String(randomInt(1_000_000_000, 10_000_000_000)); }
    while (code === this.adminCode || code === this.ownerCode || code === otherCode);
    return code;
  }

  grantSession(id, role) {
    const token = randomBytes(32).toString('base64url');
    this.tokens.set(token, { id, role, expires: this.clock() + TOKEN_LIFETIME });
    return { token, role, primaryOwner: role === 'owner' && id === this.primaryOwnerId };
  }

  pendingOwners() {
    const now = this.clock();
    for (const [id, entry] of this.ownerRequests) if (entry.expires <= now) this.ownerRequests.delete(id);
    return [...this.ownerRequests.values()].filter(entry => entry.status === 'pending')
      .map(({ id, name, requestId }) => ({ id, name, requestId }));
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

  login(id, code, remote = '', deviceKey = '', name = '') {
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
    if (role === 'owner') {
      if (!DEVICE_KEY_PATTERN.test(deviceKey)) return { error: 'This browser needs a private owner key. Reload and try again.', status: 400 };
      const ownerHash = this.ownerDevices.get(id);
      if (ownerHash && matchesCodeHash(digest(deviceKey), ownerHash)) return this.grantSession(id, role);
      if (!this.ownerDevices.size) {
        this.ownerDevices.set(id, digest(deviceKey));
        this.primaryOwnerId = id;
        if (!this.save()) { this.ownerDevices.delete(id); this.primaryOwnerId = ''; return { error: 'Could not save owner access.', status: 500 }; }
        return this.grantSession(id, role);
      }
      if (this.pendingOwners().length >= 20 && !this.ownerRequests.has(id)) return { error: 'Too many owner requests right now.', status: 429 };
      const requestId = randomBytes(16).toString('hex');
      const requestToken = randomBytes(32).toString('hex');
      this.ownerRequests.set(id, {
        id, name: String(name || 'Guest').replace(/[<>\u0000-\u001f]/g, '').trim().slice(0, 24) || 'Guest',
        requestId, tokenHash: digest(requestToken), deviceHash: digest(deviceKey),
        expires: now + OWNER_REQUEST_LIFETIME, status: 'pending',
      });
      return { pending: true, requestId, requestToken };
    }
    return this.grantSession(id, role);
  }

  ownerRequestStatus(id, requestId, requestToken) {
    const entry = this.ownerRequests.get(id);
    if (!entry || entry.expires <= this.clock() || entry.requestId !== requestId ||
        !matchesCodeHash(digest(requestToken), entry.tokenHash)) {
      return { error: 'Owner request expired. Enter the code again.', status: 404 };
    }
    if (entry.status === 'denied') { this.ownerRequests.delete(id); return { error: 'The owner declined your request.', status: 403 }; }
    if (entry.status === 'pending') return { pending: true };
    this.ownerRequests.delete(id);
    if (this.ownerDevices.get(id) !== entry.deviceHash) return { error: 'Owner access changed. Enter the code again.', status: 403 };
    return this.grantSession(id, 'owner');
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

  adminAccess(session, action, targetId, requestId = '') {
    if (!session || session.role !== 'owner') return { error: 'Only the owner can manage admins.', status: 403 };
    if (['approve_owner', 'deny_owner', 'remove_owner'].includes(action) && session.id !== this.primaryOwnerId) {
      return { error: 'Only the first owner can manage owner access.', status: 403 };
    }
    if (!ID_PATTERN.test(targetId)) return { error: 'Select a person.', status: 400 };
    if (targetId === session.id) return { error: 'You cannot change your own access.', status: 403 };
    let newCode = '', codeKind = '';
    const previousAdmins = new Set(this.revokedAdmins);
    const previousOwners = new Map(this.ownerDevices);
    const previousAdminCode = this.adminCode;
    const previousOwnerCode = this.ownerCode;
    const previousRequests = new Map(this.ownerRequests);
    if (action === 'revoke_admin') {
      if (this.roleFor(targetId) !== 'admin') return { error: 'That person is not an admin.', status: 400 };
      this.revokedAdmins.add(targetId);
      this.adminCode = newCode = this.nextCode();codeKind = 'admin';
    } else if (action === 'restore_admin') {
      if (!this.revokedAdmins.has(targetId)) return { error: 'Admin access was not removed.', status: 400 };
      this.revokedAdmins.delete(targetId);
    } else if (action === 'approve_owner' || action === 'deny_owner') {
      const entry = this.ownerRequests.get(targetId);
      if (!entry || entry.requestId !== requestId || entry.expires <= this.clock() || entry.status !== 'pending') {
        return { error: 'This owner request expired.', status: 404 };
      }
      if (action === 'approve_owner') this.ownerDevices.set(targetId, entry.deviceHash);
      this.ownerRequests.set(targetId, { ...entry, status: action === 'approve_owner' ? 'approved' : 'denied' });
      // Keep the request until its browser has collected the result.
    } else if (action === 'remove_owner') {
      if (!this.ownerDevices.has(targetId)) return { error: 'That person is not an owner.', status: 400 };
      this.ownerDevices.delete(targetId);
      this.ownerCode = newCode = this.nextCode();codeKind = 'owner';
      this.ownerRequests.clear();
    } else return { error: 'Unknown action.', status: 400 };
    if (!this.save()) {
      this.revokedAdmins = previousAdmins;
      this.ownerDevices = previousOwners;
      this.adminCode = previousAdminCode;
      this.ownerCode = previousOwnerCode;
      this.ownerRequests = previousRequests;
      return { error: 'Could not save access changes. Check the moderation storage.', status: 500 };
    }
    if (newCode) for (const [token, entry] of this.tokens) {
      if (entry.role === codeKind && !(codeKind === 'owner' && entry.id === session.id)) this.tokens.delete(token);
    }
    if (action === 'approve_owner') for (const [token, entry] of this.tokens) {
      if (entry.role === 'owner' && entry.id === targetId) this.tokens.delete(token);
    }
    return { revoked: this.revokedAdmins.has(targetId), removedOwner: action === 'remove_owner',
      newCode, codeKind, approved: action === 'approve_owner' };
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
