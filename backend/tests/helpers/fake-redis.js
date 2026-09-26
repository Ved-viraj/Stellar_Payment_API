/**
 * Minimal in-memory Redis stand-in for concurrency tests.
 *
 * Supports the subset used by the idempotency middleware and the payment
 * session lock: GET / SET (with EX/PX) / DEL, plus sendCommand for
 * `SET key value NX PX ttl` and the compare-and-delete EVAL script.
 *
 * Each command awaits a FIFO latency tick and then executes its
 * check-and-mutate step synchronously, which matches Redis' single-threaded
 * atomicity while still letting concurrent callers interleave between
 * commands — exactly the window real race conditions live in.
 */
export function createFakeRedis({ latencyMs = 0 } = {}) {
  const store = new Map();
  const commandLog = [];

  const now = () => Date.now();
  const live = (key) => {
    const entry = store.get(key);
    if (!entry) return null;
    if (entry.expiresAt !== null && entry.expiresAt <= now()) {
 * In-memory stand-in for the subset of Redis used by the exchange-rate
 * coordinator (issues #1445, #1446): SET [PX ms] [NX], GET, DEL and the
 * compare-and-delete EVAL script. Several coordinators can share one
 * instance to simulate multiple API processes against the same Redis.
 *
 * Commands resolve asynchronously (optionally after `latencyMs`) so they
 * interleave the way network round-trips do. Expiry follows Date.now(), so
 * it works with vi.useFakeTimers().
 */
export function createFakeRedis({ latencyMs = 0 } = {}) {
  const store = new Map(); // key -> { value, expiresAt }
  const calls = [];
  let failWith = null;

  const live = (key) => {
    const entry = store.get(key);
    if (!entry) return null;
    if (entry.expiresAt !== null && Date.now() >= entry.expiresAt) {
      store.delete(key);
      return null;
    }
    return entry;
  };
  // Commands complete in the order they were issued (FIFO), like a single
  // Redis connection: the idempotency middleware and the session lock share
  // one client, and the lock's correctness relies on that ordering.
  let queue = Promise.resolve();
  // With no latency, stay on the microtask queue so tests using
  // vi.useFakeTimers() are not blocked on a timer that never fires.
  const tick = () => {
    if (latencyMs <= 0) return Promise.resolve();
    queue = queue.then(
      () => new Promise((resolve) => setTimeout(resolve, Math.random() * latencyMs)),
    );
    return queue;
  };

  function setEntry(key, value, { ex, px } = {}) {
    let expiresAt = null;
    if (ex) expiresAt = now() + Number(ex) * 1000;
    if (px) expiresAt = now() + Number(px);
    store.set(key, { value: String(value), expiresAt });
  }

  const client = {
    isOpen: true,
    store,
    commandLog,
    async get(key) {
      await tick();
      commandLog.push(["GET", key]);
      return live(key)?.value ?? null;
    },
    async set(key, value, opts = {}) {
      await tick();
      commandLog.push(["SET", key]);
      if (opts.NX && live(key)) return null;
      setEntry(key, value, { ex: opts.EX, px: opts.PX });
      return "OK";
    },
    async del(key) {
      await tick();
      commandLog.push(["DEL", key]);
      return store.delete(key) ? 1 : 0;
    },
    async sendCommand(args) {
      await tick();
      const [cmd, ...rest] = args;
      commandLog.push([String(cmd).toUpperCase(), rest[0]]);
      switch (String(cmd).toUpperCase()) {
        case "SET": {
          const [key, value, ...flags] = rest;
          const upper = flags.map((f) => String(f).toUpperCase());
          if (upper.includes("NX") && live(key)) return null;
          const pxIdx = upper.indexOf("PX");
          const exIdx = upper.indexOf("EX");
          setEntry(key, value, {
            px: pxIdx >= 0 ? flags[pxIdx + 1] : undefined,
            ex: exIdx >= 0 ? flags[exIdx + 1] : undefined,
          });
          return "OK";
        }
        case "EVAL": {
          const [script, numKeys, key, token] = rest;
          if (!/redis\.call\("get", KEYS\[1\]\) == ARGV\[1\]/.test(script) || numKeys !== "1") {
            throw new Error("fake-redis: unsupported EVAL script");
          }
          const entry = live(key);
          if (entry && entry.value === token) {
            store.delete(key);
            return 1;
          }
          return 0;
        }
        case "GET":
          return live(rest[0])?.value ?? null;
        default:
          throw new Error(`fake-redis: unsupported command ${cmd}`);
      }
    },
    keys(prefix = "") {
      return [...store.keys()].filter((k) => k.startsWith(prefix) && live(k));
    },
  };

  return client;

  const execute = (args) => {
    const [cmd, ...rest] = args;
    switch (String(cmd).toUpperCase()) {
      case 'GET':
        return live(rest[0])?.value ?? null;
      case 'SET': {
        const [key, value, ...opts] = rest;
        const upper = opts.map((o) => String(o).toUpperCase());
        const pxIndex = upper.indexOf('PX');
        const ttl = pxIndex >= 0 ? Number(opts[pxIndex + 1]) : null;
        if (upper.includes('NX') && live(key)) return null;
        store.set(key, { value: String(value), expiresAt: ttl ? Date.now() + ttl : null });
        return 'OK';
      }
      case 'DEL':
        return rest.reduce((n, key) => n + (live(key) && store.delete(key) ? 1 : 0), 0);
      case 'EVAL': {
        // Only the compare-and-delete release script is supported.
        const [, , key, token] = rest;
        if (live(key)?.value === token) {
          store.delete(key);
          return 1;
        }
        return 0;
      }
      default:
        throw new Error(`fake-redis: unsupported command ${cmd}`);
    }
  };

  return {
    isOpen: true,
    calls,
    store,
    /** Make every subsequent command reject with `error` (null to heal). */
    setFailure(error) {
      failWith = error;
    },
    async sendCommand(args) {
      calls.push(args);
      if (latencyMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, latencyMs));
      } else {
        await Promise.resolve();
      }
      if (failWith) throw failWith;
      return execute(args);
    },
    /** Test helper: count calls by command name. */
    count(cmd) {
      return calls.filter(([c]) => String(c).toUpperCase() === cmd).length;
    },
  };
}
