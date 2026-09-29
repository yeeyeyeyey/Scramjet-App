import { createHash, randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';
import { dirname } from 'node:path';
import { mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';

const ID = /^[A-Za-z0-9_-]{8,64}$/;
const MAX_TICKETS = 200;
const MAX_MESSAGES = 60;
const hash = value => createHash('sha256').update(String(value)).digest('hex');
function publicTicket(ticket) {
  const { keyHash, ...visible } = ticket;
  return structuredClone(visible);
}

export class ChatFeedback {
  constructor({ file = '', clock = Date.now } = {}) {
    this.file = file;
    this.clock = clock;
    this.tickets = [];
    if (!file) return;
    try {
      const saved = JSON.parse(readFileSync(file, 'utf8'));
      if (Array.isArray(saved.tickets)) {
        this.tickets = saved.tickets.filter(ticket =>
          ticket && typeof ticket.id === 'string' && ID.test(ticket.ownerId) &&
          /^[a-f0-9]{64}$/.test(ticket.keyHash) &&
          Array.isArray(ticket.messages) && ticket.messages.length <= MAX_MESSAGES
        ).slice(-MAX_TICKETS);
      }
    } catch (error) {
      if (error.code !== 'ENOENT') console.error('Could not load feedback tickets:', error.message);
    }
  }

  save() {
    if (!this.file) return true;
    try {
      mkdirSync(dirname(this.file), { recursive: true });
      const temp = `${this.file}.${process.pid}.tmp`;
      writeFileSync(temp, JSON.stringify({ tickets: this.tickets }), { mode: 0o600 });
      renameSync(temp, this.file);
      return true;
    } catch (error) {
      console.error('Could not save feedback tickets:', error.message);
      return false;
    }
  }

  visible(ticket, viewerId, staff, key = '') {
    if (!ticket) return false;
    if (staff === true) return true;
    if (ticket.ownerId !== viewerId || !/^[A-Za-z0-9_-]{43}$/.test(key)) return false;
    return timingSafeEqual(Buffer.from(hash(key), 'hex'), Buffer.from(ticket.keyHash, 'hex'));
  }

  list(viewerId, staff = false, keys = new Map()) {
    return this.tickets.filter(ticket => this.visible(ticket, viewerId, staff, keys.get(ticket.id)))
      .sort((a, b) => b.updatedAt - a.updatedAt)
      .map(({ id, ownerId, ownerName, subject, status, createdAt, updatedAt }) =>
        ({ id, ownerId, ownerName, subject, status, createdAt, updatedAt }));
  }

  get(viewerId, staff, ticketId, key = '') {
    const ticket = this.tickets.find(row => row.id === ticketId);
    return this.visible(ticket, viewerId, staff, key) ? publicTicket(ticket) : null;
  }

  create(ownerId, ownerName, subject, text) {
    if (!ID.test(ownerId) || !subject || !text) return { error: 'Add a subject and description.', status: 400 };
    if (this.tickets.filter(ticket => ticket.ownerId === ownerId && ticket.status === 'open').length >= 5) {
      return { error: 'Close an open ticket before creating another.', status: 429 };
    }
    let removed = null;
    if (this.tickets.length >= MAX_TICKETS) {
      const index = this.tickets.findIndex(ticket => ticket.status === 'closed');
      if (index < 0) return { error: 'Feedback is full right now. Please try later.', status: 503 };
      removed = { index, ticket: this.tickets.splice(index, 1)[0] };
    }
    const now = this.clock();
    const accessKey = randomBytes(32).toString('base64url');
    const ticket = {
      id: randomUUID(), ownerId, ownerName: ownerName.slice(0, 24),
      keyHash: hash(accessKey),
      subject: subject.slice(0, 80), status: 'open', createdAt: now, updatedAt: now,
      messages: [{ id: randomUUID(), by: ownerId, name: ownerName.slice(0, 24), text: text.slice(0, 500), time: now }],
    };
    this.tickets.push(ticket);
    if (!this.save()) {
      this.tickets.pop();if (removed) this.tickets.splice(removed.index, 0, removed.ticket);
      return { error: 'Could not save the ticket. Try again.', status: 500 };
    }
    return { ticket: publicTicket(ticket), accessKey };
  }

  reply(viewerId, viewerName, staff, ticketId, text, key = '') {
    const ticket = this.tickets.find(row => row.id === ticketId);
    if (!this.visible(ticket, viewerId, staff, key)) return { error: 'Ticket not found.', status: 404 };
    if (ticket.status !== 'open') return { error: 'Reopen this ticket to reply.', status: 409 };
    if (!text) return { error: 'Write a reply first.', status: 400 };
    if (ticket.messages.length >= MAX_MESSAGES) return { error: 'This ticket is full. Start a new one.', status: 409 };
    const previousTime = ticket.updatedAt;
    ticket.updatedAt = this.clock();
    ticket.messages.push({ id: randomUUID(), by: viewerId, name: viewerName.slice(0, 24), staff: !!staff, text: text.slice(0, 500), time: ticket.updatedAt });
    if (!this.save()) { ticket.messages.pop();ticket.updatedAt = previousTime;return { error: 'Could not save the reply.', status: 500 }; }
    return { ticket: publicTicket(ticket) };
  }

  setStatus(viewerId, staff, ticketId, status, key = '') {
    const ticket = this.tickets.find(row => row.id === ticketId);
    if (!this.visible(ticket, viewerId, staff, key)) return { error: 'Ticket not found.', status: 404 };
    if (status !== 'open' && status !== 'closed') return { error: 'Invalid ticket status.', status: 400 };
    const previous = ticket.status, previousTime = ticket.updatedAt;
    ticket.status = status;ticket.updatedAt = this.clock();
    if (!this.save()) { ticket.status = previous;ticket.updatedAt = previousTime;return { error: 'Could not save the ticket.', status: 500 }; }
    return { ticket: publicTicket(ticket) };
  }
}
