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
});
