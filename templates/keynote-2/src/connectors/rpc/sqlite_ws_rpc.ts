import type { RpcConnector } from '../../core/connectors.ts';
import { RpcRequest, RpcResponse } from './rpc_common.ts';

type WsRpcResponse = RpcResponse & { id?: number };

export default function sqlite_ws_rpc(
  url = process.env.SQLITE_WS_RPC_URL || 'ws://127.0.0.1:4107',
): RpcConnector {
  let rpcUrl: string | null = null;
  let socket: WebSocket | null = null;
  let connectPromise: Promise<WebSocket> | null = null;
  let nextId = 1;
  const pending = new Map<
    number,
    {
      resolve: (value: unknown) => void;
      reject: (reason?: unknown) => void;
    }
  >();

  function ensureUrl() {
    if (!url) {
      throw new Error('SQLITE_WS_RPC_URL not set');
    }

    if (!rpcUrl) {
      const base = new URL(url);
      if (base.protocol === 'http:') base.protocol = 'ws:';
      if (base.protocol === 'https:') base.protocol = 'wss:';
      base.pathname = '/rpc';
      base.search = '';
      base.hash = '';
      rpcUrl = base.toString();
    }
  }

  function rejectPending(err: Error) {
    for (const { reject } of pending.values()) {
      reject(err);
    }
    pending.clear();
  }

  function handleMessage(data: string | ArrayBufferLike | Blob) {
    const text =
      typeof data === 'string'
        ? data
        : data instanceof Blob
          ? null
          : new TextDecoder().decode(data);

    if (text == null) {
      rejectPending(new Error('[sqlite_ws_rpc] unexpected blob message'));
      socket?.close();
      return;
    }

    let json: WsRpcResponse;
    try {
      json = JSON.parse(text) as WsRpcResponse;
    } catch {
      rejectPending(
        new Error(
          `[sqlite_ws_rpc] invalid JSON response: ${text.slice(0, 200)}`,
        ),
      );
      socket?.close();
      return;
    }

    const responseId = json.id;
    if (typeof responseId !== 'number' || !Number.isInteger(responseId)) {
      rejectPending(
        new Error('[sqlite_ws_rpc] response missing numeric request id'),
      );
      socket?.close();
      return;
    }

    const entry = pending.get(responseId);
    if (!entry) {
      return;
    }

    pending.delete(responseId);
    if (!json.ok) {
      entry.reject(
        new Error(
          `[sqlite_ws_rpc] RPC failed: ${json.error || 'unknown error'}`,
        ),
      );
      return;
    }

    entry.resolve(json.result);
  }

  async function ensureSocket() {
    ensureUrl();

    if (socket && socket.readyState === WebSocket.OPEN) {
      return socket;
    }

    if (connectPromise) {
      return connectPromise;
    }

    connectPromise = new Promise<WebSocket>((resolve, reject) => {
      const ws = new WebSocket(rpcUrl!);
      let settled = false;

      ws.onmessage = (event) => {
        handleMessage(event.data);
      };

      ws.onopen = () => {
        socket = ws;
        settled = true;
        resolve(ws);
      };

      ws.onerror = () => {
        if (!settled) {
          settled = true;
          reject(new Error('[sqlite_ws_rpc] websocket connection failed'));
        }
      };

      ws.onclose = (event) => {
        if (socket === ws) {
          socket = null;
        }

        const err = new Error(
          `[sqlite_ws_rpc] websocket closed${event.reason ? `: ${event.reason}` : ''}`,
        );
        rejectPending(err);

        if (!settled) {
          settled = true;
          reject(err);
        }
      };
    }).finally(() => {
      connectPromise = null;
    });

    return connectPromise;
  }

  async function wsCall(name: string, args?: Record<string, unknown>) {
    const ws = await ensureSocket();
    const id = nextId++;
    const body: RpcRequest & { id: number } = { id, name, args };

    return new Promise<unknown>((resolve, reject) => {
      pending.set(id, { resolve, reject });

      try {
        ws.send(JSON.stringify(body));
      } catch (err) {
        pending.delete(id);
        reject(
          new Error(`[sqlite_ws_rpc] RPC ${name} send failed: ${String(err)}`),
        );
      }
    });
  }

  const connector: RpcConnector = {
    name: 'sqlite_ws_rpc',
    maxInflightPerWorker: 16,

    async open() {
      await ensureSocket();
      await wsCall('health');
    },

    async close() {
      if (!socket) {
        return;
      }

      const ws = socket;
      socket = null;

      if (
        ws.readyState === WebSocket.OPEN ||
        ws.readyState === WebSocket.CONNECTING
      ) {
        ws.close();
      }
    },

    async getAccount(id: number) {
      const result = (await wsCall('getAccount', { id })) as {
        id: number;
        balance: bigint;
      } | null;

      if (!result) return null;
      return {
        id: result.id,
        balance: result.balance,
      };
    },

    async verify() {
      await wsCall('verify');
    },

    async call(name: string, args?: Record<string, unknown>) {
      return wsCall(name, args);
    },

    async createWorker() {
      return sqlite_ws_rpc(url);
    },
  };

  return connector;
}
