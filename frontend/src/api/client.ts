import type {
  APIErrorShape,
  DemoLeaseRequest,
  EffectiveConfig,
  LeaseDetail,
  LeaseFilter,
  LeasePage,
  LeaseSummary,
  MetricsSnapshot,
  TrafficRequest,
} from '../types';

interface Envelope<T> {
  data: T;
}

export class APIError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(message: string, status: number, code = 'request_failed') {
    super(message);
    this.name = 'APIError';
    this.status = status;
    this.code = code;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(path, {
      ...init,
      headers: {
        Accept: 'application/json',
        ...(init?.body ? { 'Content-Type': 'application/json' } : {}),
        ...init?.headers,
      },
    });
  } catch {
    throw new APIError('无法连接到 MiniLogback 管理端', 0, 'network_error');
  }

  if (!response.ok) {
    let errorBody: APIErrorShape = {};
    try {
      errorBody = (await response.json()) as APIErrorShape;
    } catch {
      // A non-JSON proxy error is deliberately reduced to a safe generic message.
    }
    throw new APIError(
      errorBody.error?.message ?? `请求失败 (${response.status})`,
      response.status,
      errorBody.error?.code,
    );
  }

  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
}

export const api = {
  async getMetrics(): Promise<MetricsSnapshot> {
    const envelope = await request<Envelope<MetricsSnapshot>>('/api/v1/metrics/current');
    return envelope.data;
  },

  async getLeases(state: LeaseFilter, signal?: AbortSignal): Promise<LeasePage> {
    const query = new URLSearchParams({ limit: '100' });
    if (state !== 'all') query.set('state', state);
    return request<LeasePage>(`/api/v1/leases?${query}`, { signal });
  },

  async getLease(id: number, signal?: AbortSignal): Promise<LeaseDetail> {
    const envelope = await request<Envelope<LeaseDetail>>(`/api/v1/leases/${id}`, { signal });
    return envelope.data;
  },

  async getConfig(): Promise<EffectiveConfig> {
    const envelope = await request<Envelope<EffectiveConfig>>('/api/v1/config/effective');
    return envelope.data;
  },

  async startTraffic(payload: TrafficRequest): Promise<void> {
    await request('/api/v1/demo/traffic', { method: 'POST', body: JSON.stringify(payload) });
  },

  async createDemoLease(payload: DemoLeaseRequest): Promise<LeaseSummary> {
    const envelope = await request<Envelope<LeaseSummary>>('/api/v1/demo/leases', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    return envelope.data;
  },

  async releaseDemoLease(id: number): Promise<void> {
    await request(`/api/v1/demo/leases/${id}`, { method: 'DELETE' });
  },
};
