import 'dotenv/config';
import crypto from 'node:crypto';
import http from 'node:http';
import type { Socket } from 'node:net';
import Database from 'better-sqlite3';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import { eq } from 'drizzle-orm';
import { integer, sqliteTable } from 'drizzle-orm/sqlite-core';
import {
  applySqlitePragmas,
  ensureSqliteDirExistsSync,
  getSqliteMode,
  type SqliteMode,
} from '../connectors/sqlite_common.ts';
import { RpcRequest, RpcResponse } from '../connectors/rpc/rpc_common.ts';

type WsRpcRequest = RpcRequest & { id?: number };
type WsRpcResponse = RpcResponse & { id?: number };
type TransferBatchItem = {
  args: Record<string, unknown>;
  resolve: () => void;
  reject: (reason?: unknown) => void;
};

const SQLITE_FILE = process.env.SQLITE_FILE ?? './.data/accounts.sqlite';
const mode: SqliteMode = getSqliteMode();

function envFlag(name: string, defaultValue: boolean) {
  const value = process.env[name];
  if (value == null) return defaultValue;
  return value !== '0' && value.toLowerCase() !== 'false';
}

function envNumber(name: string, defaultValue: number) {
  const value = process.env[name];
  if (value == null) return defaultValue;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : defaultValue;
}

const TRANSFER_BATCHING_ENABLED = envFlag('SQLITE_WS_RPC_BATCHING', true);
const TRANSFER_GROUP_COMMIT_ENABLED = envFlag(
  'SQLITE_WS_RPC_GROUP_COMMIT',
  true,
);
const TRANSFER_BATCH_TRIGGER_SIZE = Math.max(
  0,
  Math.floor(envNumber('SQLITE_WS_RPC_BATCH_SIZE', 64)),
);
const TRANSFER_BATCH_WINDOW_MS = Math.max(
  0,
  envNumber('SQLITE_WS_RPC_BATCH_MS', 1),
);

ensureSqliteDirExistsSync(SQLITE_FILE, mode);

const dbFile = new Database(mode === 'fastest' ? ':memory:' : SQLITE_FILE);
applySqlitePragmas(dbFile, mode);

const accounts = sqliteTable('accounts', {
  id: integer('id').primaryKey(),
  balance: integer('balance').notNull(),
});

const db = drizzle(dbFile, { schema: { accounts } });

function ensureSchema() {
  dbFile
    .prepare(
      `CREATE TABLE IF NOT EXISTS accounts (
                                             id INTEGER PRIMARY KEY,
                                             balance INTEGER NOT NULL
       )`,
    )
    .run();
}

ensureSchema();

const selectTransferAccounts = dbFile.prepare(
  'SELECT id, balance FROM accounts WHERE id IN (?, ?)',
);
const updateAccountBalance = dbFile.prepare(
  'UPDATE accounts SET balance = ? WHERE id = ?',
);

function applyTransfer(args: Record<string, unknown>) {
  const fromId = Number(args.from_id ?? args.from);
  const toId = Number(args.to_id ?? args.to);
  const amount = Number(args.amount);

  if (
    !Number.isInteger(fromId) ||
    !Number.isInteger(toId) ||
    !Number.isFinite(amount)
  ) {
    throw new Error('invalid transfer args');
  }
  if (fromId === toId || amount <= 0) return;

  const delta = BigInt(amount);
  const rows = selectTransferAccounts.all(fromId, toId) as Array<{
    id: number;
    balance: number;
  }>;

  if (rows.length !== 2) {
    throw new Error('account_missing');
  }

  const [first, second] = rows;
  const fromRow = first.id === fromId ? first : second;
  const toRow = first.id === fromId ? second : first;

  const fromBal = BigInt(fromRow.balance);
  const toBal = BigInt(toRow.balance);

  if (fromBal < delta) return;

  const newFrom = fromBal - delta;
  const newTo = toBal + delta;

  updateAccountBalance.run(Number(newFrom), fromId);
  updateAccountBalance.run(Number(newTo), toId);
}

const runTransferTxn = dbFile.transaction((args: Record<string, unknown>) => {
  applyTransfer(args);
});

const runTransferGroupCommitTxn = dbFile.transaction(
  (batch: TransferBatchItem[]) => {
    const outcomes: Array<Error | null> = new Array(batch.length);

    for (let i = 0; i < batch.length; i++) {
      try {
        applyTransfer(batch[i]!.args);
        outcomes[i] = null;
      } catch (err) {
        outcomes[i] = err instanceof Error ? err : new Error(String(err));
      }
    }

    return outcomes;
  },
);

let transferQueue: TransferBatchItem[] = [];
let transferFlushPromise: Promise<void> | null = null;
let transferFlushTimer: NodeJS.Timeout | null = null;

function clearTransferFlushTimer() {
  if (!transferFlushTimer) return;
  clearTimeout(transferFlushTimer);
  transferFlushTimer = null;
}

function resolveTransferBatch(
  batch: TransferBatchItem[],
  outcomes: Array<Error | null>,
) {
  for (let i = 0; i < batch.length; i++) {
    const outcome = outcomes[i];
    if (outcome) {
      batch[i]!.reject(outcome);
    } else {
      batch[i]!.resolve();
    }
  }
}

async function flushTransferQueue(): Promise<void> {
  if (transferFlushPromise) {
    await transferFlushPromise;
    if (transferQueue.length === 0) {
      return;
    }
  }

  if (transferQueue.length === 0) {
    return;
  }

  clearTransferFlushTimer();
  const batch = transferQueue;
  transferQueue = [];

  transferFlushPromise = Promise.resolve().then(() => {
    if (TRANSFER_GROUP_COMMIT_ENABLED) {
      const outcomes = runTransferGroupCommitTxn(batch);
      resolveTransferBatch(batch, outcomes);
      return;
    }

    for (const item of batch) {
      try {
        runTransferTxn(item.args);
        item.resolve();
      } catch (err) {
        item.reject(err instanceof Error ? err : new Error(String(err)));
      }
    }
  });

  try {
    await transferFlushPromise;
  } finally {
    transferFlushPromise = null;
    if (transferQueue.length > 0) {
      await flushTransferQueue();
    }
  }
}

function scheduleTransferFlush() {
  if (transferFlushPromise || transferQueue.length === 0) {
    return;
  }

  const sizeTriggered =
    TRANSFER_BATCH_TRIGGER_SIZE > 0 &&
    transferQueue.length >= TRANSFER_BATCH_TRIGGER_SIZE;
  if (sizeTriggered || TRANSFER_BATCH_WINDOW_MS === 0) {
    void flushTransferQueue();
    return;
  }

  if (TRANSFER_BATCH_WINDOW_MS > 0 && !transferFlushTimer) {
    transferFlushTimer = setTimeout(() => {
      transferFlushTimer = null;
      void flushTransferQueue();
    }, TRANSFER_BATCH_WINDOW_MS);
  }
}

async function rpcTransfer(args: Record<string, unknown>) {
  if (!TRANSFER_BATCHING_ENABLED) {
    runTransferTxn(args);
    return;
  }

  await new Promise<void>((resolve, reject) => {
    transferQueue.push({ args, resolve, reject });
    scheduleTransferFlush();
  });
}

async function rpcGetAccount(args: Record<string, unknown>) {
  const id = Number(args.id);
  if (!Number.isInteger(id)) throw new Error('invalid id');

  const row = db
    .select()
    .from(accounts)
    .where(eq(accounts.id, id))
    .limit(1)
    .get();

  if (!row) return null;

  return {
    id: row.id,
    balance: BigInt(row.balance).toString(),
  };
}

async function rpcVerify() {
  const rawInitial = process.env.SEED_INITIAL_BALANCE;
  if (!rawInitial) {
    console.warn(
      '[sqlite-ws-rpc] SEED_INITIAL_BALANCE not set; skipping verify',
    );
    return { skipped: true };
  }

  let initial: bigint;
  try {
    initial = BigInt(rawInitial);
  } catch {
    throw new Error(`invalid SEED_INITIAL_BALANCE=${rawInitial}`);
  }

  const row = dbFile
    .prepare(
      `
        SELECT
          COUNT(*) AS count,
          COALESCE(SUM(balance), 0) AS total,
          SUM(CASE WHEN balance != ? THEN 1 ELSE 0 END) AS changed
        FROM accounts
      `,
    )
    .get(initial.toString()) as
    | { count: number; total: number; changed: number }
    | undefined;

  const count = BigInt(row?.count ?? 0);
  const total = BigInt(row?.total ?? 0);
  const changed = BigInt(row?.changed ?? 0);
  const expected = initial * count;

  if (count === 0n) {
    throw new Error('verify failed: accounts=0');
  }
  if (total !== expected) {
    throw new Error(
      `verify failed: accounts=${count} total=${total} expected=${expected}`,
    );
  }
  if (changed === 0n) {
    throw new Error('verify failed: total preserved but no balances changed');
  }

  return {
    accounts: count.toString(),
    total: total.toString(),
    changed: changed.toString(),
  };
}

async function rpcSeed(args: Record<string, unknown>) {
  const count = Number(args.accounts ?? process.env.SEED_ACCOUNTS ?? '0');
  const rawInitial =
    (args.initialBalance as string | number | undefined) ??
    process.env.SEED_INITIAL_BALANCE;

  if (!Number.isInteger(count) || count <= 0) {
    throw new Error('[sqlite-ws-rpc] invalid accounts for seed');
  }
  if (rawInitial === undefined || rawInitial === null) {
    throw new Error('[sqlite-ws-rpc] missing initialBalance for seed');
  }

  let initial: bigint;
  try {
    initial = BigInt(rawInitial);
  } catch {
    throw new Error(`[sqlite-ws-rpc] invalid initialBalance=${rawInitial}`);
  }

  const seedTx = dbFile.transaction(() => {
    dbFile.prepare('DELETE FROM accounts').run();

    const insert = dbFile.prepare(
      'INSERT INTO accounts (id, balance) VALUES (?, ?)',
    );

    const batchSize = 10_000;
    for (let start = 0; start < count; start += batchSize) {
      const end = Math.min(start + batchSize, count);
      for (let id = start; id < end; id++) {
        insert.run(id, Number(initial));
      }
    }
  });

  seedTx();

  console.log(
    `[sqlite-ws-rpc] seeded accounts: count=${count} initial=${initial.toString()}`,
  );
}

async function handleRpc(body: RpcRequest): Promise<RpcResponse> {
  const name = body?.name;
  const args = body?.args ?? {};

  if (!name) return { ok: false, error: 'missing name' };

  try {
    if (name !== 'transfer' && transferQueue.length > 0) {
      await flushTransferQueue();
    }

    switch (name) {
      case 'health':
        return { ok: true, result: { status: 'ok' } };
      case 'transfer':
        await rpcTransfer(args);
        return { ok: true };
      case 'getAccount':
        return { ok: true, result: await rpcGetAccount(args) };
      case 'verify':
        return { ok: true, result: await rpcVerify() };
      case 'seed':
        return { ok: true, result: await rpcSeed(args) };
      default:
        return { ok: false, error: `unknown method: ${name}` };
    }
  } catch (err: any) {
    console.error('Unhandled error in handleRpc:', err);
    return { ok: false, error: 'internal error' };
  }
}

function sendFrame(socket: Socket, opcode: number, payload?: Buffer) {
  if (socket.destroyed || !socket.writable) {
    return;
  }

  const body = payload ?? Buffer.allocUnsafe(0);
  let header: Buffer;

  if (body.length < 126) {
    header = Buffer.allocUnsafe(2);
    header[0] = 0x80 | opcode;
    header[1] = body.length;
  } else if (body.length < 65536) {
    header = Buffer.allocUnsafe(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(body.length, 2);
  } else {
    header = Buffer.allocUnsafe(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(body.length), 2);
  }

  try {
    socket.cork();
    socket.write(header);
    if (body.length > 0) {
      socket.write(body);
    }
    socket.uncork();
  } catch (err: any) {
    if (err?.code !== 'EPIPE' && err?.code !== 'ECONNRESET') {
      throw err;
    }
  }
}

function sendJson(socket: Socket, body: WsRpcResponse) {
  sendFrame(socket, 0x1, Buffer.from(JSON.stringify(body)));
}

function bindSocket(socket: Socket) {
  let buffer: Buffer<ArrayBufferLike> = Buffer.alloc(0);
  let closed = false;

  socket.setNoDelay(true);
  socket.setKeepAlive(true);

  socket.on('data', (chunk: Buffer) => {
    buffer = buffer.length === 0 ? chunk : Buffer.concat([buffer, chunk]);

    while (buffer.length >= 2) {
      const first = buffer[0];
      const second = buffer[1];
      const fin = (first & 0x80) !== 0;
      const opcode = first & 0x0f;
      const masked = (second & 0x80) !== 0;

      if (!fin || opcode === 0x0) {
        sendFrame(socket, 0x8, Buffer.from([0x03, 0xea]));
        socket.end();
        return;
      }

      const frame = buffer;
      let offset = 2;
      let payloadLength = second & 0x7f;

      if (payloadLength === 126) {
        if (buffer.length < offset + 2) return;
        payloadLength = buffer.readUInt16BE(offset);
        offset += 2;
      } else if (payloadLength === 127) {
        if (buffer.length < offset + 8) return;
        const bigLength = buffer.readBigUInt64BE(offset);
        if (bigLength > BigInt(Number.MAX_SAFE_INTEGER)) {
          sendFrame(socket, 0x8, Buffer.from([0x03, 0xf1]));
          socket.end();
          return;
        }
        payloadLength = Number(bigLength);
        offset += 8;
      }

      const maskOffset = offset;
      if (masked) {
        if (frame.length < offset + 4) return;
        offset += 4;
      }

      if (frame.length < offset + payloadLength) {
        return;
      }

      const payload = frame.subarray(offset, offset + payloadLength);
      buffer = frame.subarray(offset + payloadLength);

      if (masked) {
        const mask = frame.subarray(maskOffset, maskOffset + 4);
        for (let i = 0; i < payload.length; i++) {
          payload[i] ^= mask[i & 3];
        }
      }

      if (opcode === 0x8) {
        closed = true;
        sendFrame(socket, 0x8, payload);
        socket.end();
        return;
      }

      if (opcode === 0x9) {
        sendFrame(socket, 0xa, payload);
        continue;
      }

      if (opcode !== 0x1) {
        sendFrame(socket, 0x8, Buffer.from([0x03, 0xeb]));
        socket.end();
        return;
      }

      let body: WsRpcRequest;
      try {
        body = JSON.parse(payload.toString('utf8')) as WsRpcRequest;
      } catch {
        sendJson(socket, { ok: false, error: 'invalid json' });
        continue;
      }

      const requestId = Number.isInteger(body.id) ? body.id : undefined;

      void handleRpc(body).then((rsp) => {
        if (closed || socket.destroyed || !socket.writable) {
          return;
        }

        sendJson(
          socket,
          requestId === undefined ? rsp : { ...rsp, id: requestId },
        );
      });
    }
  });

  socket.on('close', () => {
    closed = true;
  });

  socket.on('error', (err) => {
    closed = true;

    const code = (err as NodeJS.ErrnoException).code;
    if (code === 'EPIPE' || code === 'ECONNRESET') {
      return;
    }

    console.error('[sqlite-ws-rpc] socket error:', err);
  });
}

const port = Number(process.env.SQLITE_WS_RPC_PORT ?? 4107);

const server = http.createServer((req, res) => {
  const url = new URL(req.url ?? '/', `http://${req.headers.host}`);

  if (req.method === 'GET' && url.pathname === '/') {
    res.statusCode = 200;
    res.end('sqlite drizzle ws rpc server');
    return;
  }

  res.statusCode = 404;
  res.end('not found');
});

server.on('upgrade', (req, socket) => {
  const url = new URL(req.url ?? '/', `http://${req.headers.host}`);

  if (url.pathname !== '/rpc') {
    socket.end('HTTP/1.1 404 Not Found\r\n\r\n');
    return;
  }

  const key = req.headers['sec-websocket-key'];
  const upgrade = req.headers.upgrade;

  if (
    typeof key !== 'string' ||
    typeof upgrade !== 'string' ||
    upgrade.toLowerCase() !== 'websocket'
  ) {
    socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
    return;
  }

  const accept = crypto
    .createHash('sha1')
    .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
    .digest('base64');

  socket.write(
    [
      'HTTP/1.1 101 Switching Protocols',
      'Upgrade: websocket',
      'Connection: Upgrade',
      `Sec-WebSocket-Accept: ${accept}`,
      '\r\n',
    ].join('\r\n'),
  );

  bindSocket(socket as Socket);
});

server.listen(port, () => {
  console.log(
    `sqlite drizzle ws rpc server listening on ws://localhost:${port}`,
  );
  console.log(
    `[sqlite-ws-rpc] batching=${TRANSFER_BATCHING_ENABLED ? 'on' : 'off'} group_commit=${TRANSFER_GROUP_COMMIT_ENABLED ? 'on' : 'off'} batch_size=${TRANSFER_BATCH_TRIGGER_SIZE} batch_ms=${TRANSFER_BATCH_WINDOW_MS}`,
  );
});
