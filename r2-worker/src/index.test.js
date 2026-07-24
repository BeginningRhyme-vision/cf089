import { describe, it, expect, vi, beforeEach } from 'vitest';
import worker from './index.js';

// Global mock storage
let mockFetchCalls = [];
let mockClients = [];

// Mock aws4fetch module
vi.mock('aws4fetch', () => {
  return {
    AwsClient: class MockAwsClient {
      constructor(config) {
        this.config = config;
        mockClients.push(this);
      }
      async sign(_url, options) {
        return { headers: options?.headers || {} };
      }
      async fetch(url, options) {
        mockFetchCalls.push({ client: this, url, options });
        
        // Simulating Source Client behavior (GET)
        if (this.config.accessKeyId === 'source-ak') {
             if (options.method === 'GET') {
                 return {
                     ok: true,
                     body: 'mock-file-content',
                     headers: new Map(),
                 }
             }
        }

        // Simulating Dest Client behavior (PUT)
        if (this.config.accessKeyId === 'dest-ak') {
            if (options.method === 'PUT') {
                 return {
                     ok: true,
                     text: async () => "OK",
                     headers: {
                         get: (key) => key === 'ETag' ? '"mock-etag"' : null
                     }
                 }
            }
        }
        
        return { ok: false, status: 404, text: async () => "Not Found" };
      }
    },
  };
});

describe('Worker HTTP Handler', () => {
  let env;
  let ctx;
  let request;

  beforeEach(() => {
    // Reset mocks
    vi.clearAllMocks();
    mockFetchCalls = [];
    mockClients = [];

    globalThis.fetch = vi.fn(async (url, options) => {
      if (options?.method === 'PUT') {
        return {
          ok: true,
          status: 200,
          headers: {
            get: (key) => (key === 'etag' || key === 'ETag') ? '"mock-etag"' : null,
          }
        };
      }
      return { ok: false, status: 404, text: async () => "Not Found" };
    });

    // Mock Environment
    env = {
      SOURCE_ACCESS_KEY_ID: 'source-ak',
      SOURCE_SECRET_ACCESS_KEY: 'source-sk',
      AWS_ACCESS_KEY_ID: 'dest-ak',
      AWS_SECRET_ACCESS_KEY: 'dest-sk',
      AWS_REGION: 'us-east-1',
      UPLOAD_RETRY_ATTEMPTS: '2',
    };

    // Mock ExecutionContext
    ctx = {
      waitUntil: vi.fn(),
      passThroughOnException: vi.fn(),
    };
  });

  it('should successfully copy from R2 to S3', async () => {
    const payload = {
      r2Key: 'https://account.r2.cloudflarestorage.com/bucket/video.mp4',
      s3Url: 'https://target-bucket.oss.com/video.mp4',
      size: 100,
      offset: 0,
      uploadId: 'upload-123',
      partNumber: 5,
    };

    request = new Request('http://worker/initiate-copy', {
      method: 'POST',
      body: JSON.stringify(payload),
    });

    const response = await worker.fetch(request, env, ctx);
    
    expect(response.status).toBe(200);
    const body = await response.json();
    expect(body.etag).toBe('"mock-etag"');

    // Check Clients
    expect(mockClients.length).toBe(2);
    const sourceClient = mockClients.find(c => c.config.accessKeyId === 'source-ak');
    const destClient = mockClients.find(c => c.config.accessKeyId === 'dest-ak');
    
    expect(sourceClient).toBeDefined();
    expect(destClient).toBeDefined();

    // Check Source Fetch
    const sourceCall = mockFetchCalls.find(call => call.client === sourceClient);
    expect(sourceCall).toBeDefined();
    const sourcePathname = typeof sourceCall.url === 'string'
      ? new URL(sourceCall.url).pathname
      : sourceCall.url.pathname;
    expect(sourcePathname).toBe('/bucket/video.mp4');
    expect(sourceCall.options.headers['Range']).toBe('bytes=0-99');

    expect(destClient).toBeDefined();

    expect(globalThis.fetch).toHaveBeenCalled();
    const putCall = globalThis.fetch.mock.calls.find(([, opts]) => opts?.method === 'PUT');
    expect(putCall).toBeDefined();
    const [putUrl] = putCall;
    const destUrl = new URL(putUrl);
    expect(destUrl.searchParams.get('partNumber')).toBe('5');
    expect(destUrl.searchParams.get('uploadId')).toBe('upload-123');
  });

  it('should handle s3:// scheme in r2Key', async () => {
    const payload = {
      r2Key: 's3://source-bucket/video.mp4',
      s3Url: 'https://target-bucket.oss.com/video.mp4',
      size: 100,
      offset: 0,
    };

    request = new Request('http://worker/initiate-copy', {
      method: 'POST',
      body: JSON.stringify(payload),
    });

    const response = await worker.fetch(request, env, ctx);
    expect(response.status).toBe(200);

    const sourceClient = mockClients.find(c => c.config.accessKeyId === 'source-ak');
    // Expect converted endpoint for s3 scheme
    expect(sourceClient.config.endpoint).toBe('https://source-bucket.s3.amazonaws.com');
  });

  it('should shortcut zero-byte copy without source credentials or source range fetch', async () => {
    delete env.SOURCE_ACCESS_KEY_ID;
    delete env.SOURCE_SECRET_ACCESS_KEY;

    request = new Request('http://worker/initiate-copy', {
      method: 'POST',
      body: JSON.stringify({
        r2Key: 'https://account.r2.cloudflarestorage.com/bucket/empty.txt',
        s3Url: 'https://target-bucket.oss.com/empty.txt',
        size: 0,
        offset: 0,
      }),
    });

    const response = await worker.fetch(request, env, ctx);
    expect(response.status).toBe(200);

    const sourceGetCall = mockFetchCalls.find((call) => call.options?.method === 'GET');
    expect(sourceGetCall).toBeUndefined();
    const sourceClient = mockClients.find(c => c.config.accessKeyId === 'source-ak');
    expect(sourceClient).toBeUndefined();

    const putCall = globalThis.fetch.mock.calls.find(([, opts]) => opts?.method === 'PUT');
    expect(putCall).toBeDefined();
    expect(putCall[1].headers['Content-Length']).toBe('0');
    expect(putCall[1].body).toBeInstanceOf(Uint8Array);
    expect(putCall[1].body).toHaveLength(0);
  });

  it('should reject negative transfer sizes', async () => {
    request = new Request('http://worker/initiate-copy', {
      method: 'POST',
      body: JSON.stringify({
        r2Key: 'https://account.r2.cloudflarestorage.com/bucket/video.mp4',
        s3Url: 'https://target-bucket.oss.com/video.mp4',
        size: -1,
        offset: 0,
      }),
    });

    const response = await worker.fetch(request, env, ctx);
    expect(response.status).toBe(400);

    const body = await response.json();
    expect(body.error.code).toBe('InvalidSize');
    expect(body.error.retryable).toBe(false);
  });

  it('should log stage timing for successful copy requests', async () => {
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

    request = new Request('http://worker/initiate-copy', {
      method: 'POST',
      body: JSON.stringify({
        r2Key: 'https://account.r2.cloudflarestorage.com/bucket/video.mp4',
        s3Url: 'https://target-bucket.oss.com/video.mp4',
        size: 100,
        offset: 0,
        uploadId: 'upload-123',
        partNumber: 5,
      }),
    });

    const response = await worker.fetch(request, env, ctx);
    expect(response.status).toBe(200);

    const timingLog = logSpy.mock.calls
      .map((call) => call.join(' '))
      .find((msg) => msg.includes('Copy timing success for '));

    expect(timingLog).toBeDefined();
    expect(timingLog).toContain('source_fetch_ms=');
    expect(timingLog).toContain('dest_upload_ms=');
    expect(timingLog).toContain('total_copy_ms=');

    logSpy.mockRestore();
  });

  it('should pass timeout signals to source fetch and destination upload', async () => {
    env.SOURCE_FETCH_TIMEOUT_SECONDS = '5';
    env.DEST_UPLOAD_TIMEOUT_SECONDS = '180';

    request = new Request('http://worker/initiate-copy', {
      method: 'POST',
      body: JSON.stringify({
        r2Key: 'https://account.r2.cloudflarestorage.com/bucket/video.mp4',
        s3Url: 'https://target-bucket.oss.com/video.mp4',
        size: 100,
        offset: 0,
        uploadId: 'upload-123',
        partNumber: 5,
      }),
    });

    const response = await worker.fetch(request, env, ctx);
    expect(response.status).toBe(200);

    const sourceCall = mockFetchCalls.find((call) => call.options?.method === 'GET');
    expect(sourceCall).toBeDefined();
    expect(sourceCall.options.signal).toBeDefined();
    expect(typeof sourceCall.options.signal.aborted).toBe('boolean');

    const putCall = globalThis.fetch.mock.calls.find(([, opts]) => opts?.method === 'PUT');
    expect(putCall).toBeDefined();
    expect(putCall[1].signal).toBeDefined();
    expect(typeof putCall[1].signal.aborted).toBe('boolean');
  });

  it('should return 400 for missing parameters', async () => {
    const payload = {
      // Missing r2Key
      s3Url: 'https://target.com/file',
    };

    request = new Request('http://worker/initiate-copy', {
      method: 'POST',
      body: JSON.stringify(payload),
    });

    const response = await worker.fetch(request, env, ctx);
    expect(response.status).toBe(400);
  });

  it('should return structured fatal error for missing destination credentials', async () => {
    delete env.AWS_ACCESS_KEY_ID;
    delete env.AWS_SECRET_ACCESS_KEY;

    request = new Request('http://worker/initiate-copy', {
      method: 'POST',
      body: JSON.stringify({
        r2Key: 'https://account.r2.cloudflarestorage.com/bucket/video.mp4',
        s3Url: 'https://target-bucket.oss.com/video.mp4',
        size: 100,
        offset: 0,
      }),
    });

    const response = await worker.fetch(request, env, ctx);
    expect(response.status).toBe(500);
    expect(response.headers.get('X-Transfer-Retryable')).toBe('false');

    const body = await response.json();
    expect(body.error.code).toBe('MissingEnv');
    expect(body.error.retryable).toBe(false);
  });

  it('should classify destination NoSuchUpload as fatal', async () => {
    globalThis.fetch = vi.fn(async (_url, options) => {
      if (options?.method === 'PUT') {
        return {
          ok: false,
          status: 404,
          text: async () => '<Error><Code>NoSuchUpload</Code><Message>The specified upload does not exist.</Message></Error>',
          headers: {
            get: () => null,
          }
        };
      }
      return { ok: false, status: 404, text: async () => "Not Found" };
    });

    request = new Request('http://worker/initiate-copy', {
      method: 'POST',
      body: JSON.stringify({
        r2Key: 'https://account.r2.cloudflarestorage.com/bucket/video.mp4',
        s3Url: 'https://target-bucket.oss.com/video.mp4',
        size: 100,
        offset: 0,
        uploadId: 'upload-123',
        partNumber: 1,
      }),
    });

    const response = await worker.fetch(request, env, ctx);
    expect(response.status).toBe(404);
    expect(response.headers.get('X-Transfer-Retryable')).toBe('false');

    const body = await response.json();
    expect(body.error.code).toBe('DestNoSuchUpload');
    expect(body.error.stage).toBe('dest_put');
    expect(body.error.retryable).toBe(false);
  });

  it('should classify network connection lost as retryable', async () => {
    globalThis.fetch = vi.fn(async (_url, options) => {
      if (options?.method === 'PUT') {
        throw new Error('Network connection lost.');
      }
      return { ok: false, status: 404, text: async () => "Not Found" };
    });

    request = new Request('http://worker/initiate-copy', {
      method: 'POST',
      body: JSON.stringify({
        r2Key: 'https://account.r2.cloudflarestorage.com/bucket/video.mp4',
        s3Url: 'https://target-bucket.oss.com/video.mp4',
        size: 100,
        offset: 0,
        uploadId: 'upload-123',
        partNumber: 1,
      }),
    });

    const response = await worker.fetch(request, env, ctx);
    expect(response.status).toBe(502);
    expect(response.headers.get('X-Transfer-Retryable')).toBe('true');

    const body = await response.json();
    expect(body.error.code).toBe('NetworkConnectionLost');
    expect(body.error.retryable).toBe(true);
  });

  it('should honor Retry-After header for retryable destination responses', async () => {
    const originalSetTimeout = globalThis.setTimeout;
    const timeoutSpy = vi.spyOn(globalThis, 'setTimeout').mockImplementation((fn, ms, ...args) => {
      fn(...args);
      return 0;
    });

    let putAttempts = 0;
    globalThis.fetch = vi.fn(async (_url, options) => {
      if (options?.method === 'PUT') {
        putAttempts += 1;
        if (putAttempts === 1) {
          return {
            ok: false,
            status: 429,
            text: async () => '<Error><Code>SlowDown</Code><Message>retry later</Message></Error>',
            headers: {
              get: (key) => (key === 'Retry-After' || key === 'retry-after') ? '6' : null,
            }
          };
        }
        return {
          ok: true,
          status: 200,
          headers: {
            get: (key) => (key === 'etag' || key === 'ETag') ? '"mock-etag"' : null,
          }
        };
      }
      return { ok: false, status: 404, text: async () => "Not Found" };
    });

    request = new Request('http://worker/initiate-copy', {
      method: 'POST',
      body: JSON.stringify({
        r2Key: 'https://account.r2.cloudflarestorage.com/bucket/video.mp4',
        s3Url: 'https://target-bucket.oss.com/video.mp4',
        size: 100,
        offset: 0,
        uploadId: 'upload-123',
        partNumber: 1,
      }),
    });

    const response = await worker.fetch(request, env, ctx);
    expect(response.status).toBe(200);
    expect(timeoutSpy).toHaveBeenCalledWith(expect.any(Function), 6000);

    timeoutSpy.mockRestore();
    globalThis.setTimeout = originalSetTimeout;
  });

  it('should retry when reading a retryable destination error body loses the connection', async () => {
    let putAttempts = 0;
    globalThis.fetch = vi.fn(async (_url, options) => {
      if (options?.method === 'PUT') {
        putAttempts += 1;
        if (putAttempts === 1) {
          return {
            ok: false,
            status: 503,
            text: async () => {
              throw new Error('Network connection lost.');
            },
            headers: {
              get: () => null,
            }
          };
        }
        return {
          ok: true,
          status: 200,
          headers: {
            get: (key) => (key === 'etag' || key === 'ETag') ? '"mock-etag"' : null,
          }
        };
      }
      return { ok: false, status: 404, text: async () => "Not Found" };
    });

    request = new Request('http://worker/initiate-copy', {
      method: 'POST',
      body: JSON.stringify({
        r2Key: 'https://account.r2.cloudflarestorage.com/bucket/video.mp4',
        s3Url: 'https://target-bucket.oss.com/video.mp4',
        size: 100,
        offset: 0,
        uploadId: 'upload-123',
        partNumber: 1,
      }),
    });

    const response = await worker.fetch(request, env, ctx);
    expect(response.status).toBe(200);
    expect(putAttempts).toBe(2);
  });

  it('should not retry when reading a non-retryable destination error body loses the connection', async () => {
    let putAttempts = 0;
    globalThis.fetch = vi.fn(async (_url, options) => {
      if (options?.method === 'PUT') {
        putAttempts += 1;
        return {
          ok: false,
          status: 404,
          text: async () => {
            throw new Error('Network connection lost.');
          },
          headers: {
            get: () => null,
          }
        };
      }
      return { ok: false, status: 404, text: async () => "Not Found" };
    });

    request = new Request('http://worker/initiate-copy', {
      method: 'POST',
      body: JSON.stringify({
        r2Key: 'https://account.r2.cloudflarestorage.com/bucket/video.mp4',
        s3Url: 'https://target-bucket.oss.com/video.mp4',
        size: 100,
        offset: 0,
        uploadId: 'upload-123',
        partNumber: 1,
      }),
    });

    const response = await worker.fetch(request, env, ctx);
    expect(response.status).toBe(404);
    expect(putAttempts).toBe(1);

    const body = await response.json();
    expect(body.error.code).toBe('DestNotFound');
    expect(body.error.retryable).toBe(false);
  });

  it('should honor Retry-After when reading a retryable destination error body loses the connection', async () => {
    const originalSetTimeout = globalThis.setTimeout;
    const timeoutSpy = vi.spyOn(globalThis, 'setTimeout').mockImplementation((fn, ms, ...args) => {
      fn(...args);
      return 0;
    });

    let putAttempts = 0;
    globalThis.fetch = vi.fn(async (_url, options) => {
      if (options?.method === 'PUT') {
        putAttempts += 1;
        if (putAttempts === 1) {
          return {
            ok: false,
            status: 429,
            text: async () => {
              throw new Error('Network connection lost.');
            },
            headers: {
              get: (key) => (key === 'Retry-After' || key === 'retry-after') ? '6' : null,
            }
          };
        }
        return {
          ok: true,
          status: 200,
          headers: {
            get: (key) => (key === 'etag' || key === 'ETag') ? '"mock-etag"' : null,
          }
        };
      }
      return { ok: false, status: 404, text: async () => "Not Found" };
    });

    request = new Request('http://worker/initiate-copy', {
      method: 'POST',
      body: JSON.stringify({
        r2Key: 'https://account.r2.cloudflarestorage.com/bucket/video.mp4',
        s3Url: 'https://target-bucket.oss.com/video.mp4',
        size: 100,
        offset: 0,
        uploadId: 'upload-123',
        partNumber: 1,
      }),
    });

    const response = await worker.fetch(request, env, ctx);
    expect(response.status).toBe(200);
    expect(putAttempts).toBe(2);
    expect(timeoutSpy).toHaveBeenCalledWith(expect.any(Function), 6000);

    timeoutSpy.mockRestore();
    globalThis.setTimeout = originalSetTimeout;
  });

  it('should fall back to exponential backoff when Retry-After is invalid', async () => {
    const originalSetTimeout = globalThis.setTimeout;
    const originalRandom = Math.random;
    Math.random = vi.fn(() => 0);
    const timeoutSpy = vi.spyOn(globalThis, 'setTimeout').mockImplementation((fn, ms, ...args) => {
      fn(...args);
      return 0;
    });

    let putAttempts = 0;
    globalThis.fetch = vi.fn(async (_url, options) => {
      if (options?.method === 'PUT') {
        putAttempts += 1;
        if (putAttempts === 1) {
          return {
            ok: false,
            status: 429,
            text: async () => '<Error><Code>SlowDown</Code><Message>retry later</Message></Error>',
            headers: {
              get: (key) => (key === 'Retry-After' || key === 'retry-after') ? 'not-a-number' : null,
            }
          };
        }
        return {
          ok: true,
          status: 200,
          headers: {
            get: (key) => (key === 'etag' || key === 'ETag') ? '"mock-etag"' : null,
          }
        };
      }
      return { ok: false, status: 404, text: async () => "Not Found" };
    });

    request = new Request('http://worker/initiate-copy', {
      method: 'POST',
      body: JSON.stringify({
        r2Key: 'https://account.r2.cloudflarestorage.com/bucket/video.mp4',
        s3Url: 'https://target-bucket.oss.com/video.mp4',
        size: 100,
        offset: 0,
        uploadId: 'upload-123',
        partNumber: 1,
      }),
    });

    const response = await worker.fetch(request, env, ctx);
    expect(response.status).toBe(200);
    expect(timeoutSpy).toHaveBeenCalledWith(expect.any(Function), 3000);

    timeoutSpy.mockRestore();
    Math.random = originalRandom;
    globalThis.setTimeout = originalSetTimeout;
  });
});
