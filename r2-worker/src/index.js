import { AwsClient } from "aws4fetch";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function backoffMs(attempt) {
  const base = Math.min(1000 * 2 ** (attempt - 1), 8000);
  const jitter = Math.floor(Math.random() * 250);
  return base + jitter;
}

const retryAfterMinMs = 1000;
const retryAfterMaxMs = 10000;
const retryFallbackExtraMs = 2000;

function clampRetryDelayMs(ms) {
  if (!Number.isFinite(ms) || ms <= 0) {
    return null;
  }
  return Math.min(Math.max(Math.ceil(ms), retryAfterMinMs), retryAfterMaxMs);
}

function parseRetryAfterMs(headers) {
  const raw = headers?.get?.("Retry-After") || headers?.get?.("retry-after");
  if (!raw) {
    return null;
  }

  const trimmed = String(raw).trim();
  if (!trimmed) {
    return null;
  }

  const seconds = Number.parseFloat(trimmed);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return clampRetryDelayMs(seconds * 1000);
  }

  const at = Date.parse(trimmed);
  if (Number.isNaN(at)) {
    return null;
  }

  return clampRetryDelayMs(at - Date.now());
}

function getRetryDelayMs(attempt, headers) {
  const retryAfterMs = parseRetryAfterMs(headers);
  if (retryAfterMs !== null) {
    return retryAfterMs;
  }
  return backoffMs(attempt) + retryFallbackExtraMs;
}

function elapsedMs(startedAt) {
  return Math.max(0, Date.now() - startedAt);
}

function formatTimingMs(ms) {
  if (!Number.isFinite(ms)) {
    return "na";
  }
  return String(Math.max(0, Math.round(ms)));
}

function getEnvTimeoutMs(env, key, defaultSeconds) {
  const raw = env?.[key];
  const parsed = Number.parseInt(raw || "", 10);
  const seconds = Number.isFinite(parsed) && parsed > 0 ? parsed : defaultSeconds;
  return seconds * 1000;
}

function isAbortError(error) {
  if (!error) {
    return false;
  }
  if (error.name === "AbortError") {
    return true;
  }
  const message = String(error?.message || "");
  const lower = message.toLowerCase();
  return lower.includes("aborted") || lower.includes("aborterror") || lower.includes("operation was aborted");
}

async function fetchWithTimeout(fetcher, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => {
    controller.abort();
  }, timeoutMs);

  try {
    return await fetcher(controller.signal);
  } finally {
    clearTimeout(timer);
  }
}

function isRetryableStatus(status) {
  return status === 408 || status === 429 || (status >= 500 && status <= 599);
}

class TransferError extends Error {
  constructor({ statusCode, code, stage, message, retryable, details }) {
    super(message);
    this.name = "TransferError";
    this.statusCode = statusCode;
    this.code = code;
    this.stage = stage;
    this.retryable = retryable;
    this.details = details || null;
  }
}

function createTransferError(statusCode, code, stage, message, retryable, details = null) {
  return new TransferError({ statusCode, code, stage, message, retryable, details });
}

function classifyLooseNetworkError(stage, message) {
  const lower = (message || "").toLowerCase();

  if (lower.includes("timed out") || lower.includes("timeout")) {
    return createTransferError(504, "NetworkTimeout", stage, message, true);
  }
  if (
    lower.includes("network connection lost") ||
    lower.includes("connection lost") ||
    lower.includes("connection closed") ||
    lower.includes("stream closed") ||
    lower.includes("socket closed") ||
    lower.includes("socket hang up") ||
    lower.includes("connection reset") ||
    lower.includes("econnreset") ||
    lower.includes("upstream connect error") ||
    lower.includes("fetch failed")
  ) {
    return createTransferError(502, "NetworkConnectionLost", stage, message, true);
  }
  if (lower.includes("dns") || lower.includes("enotfound")) {
    return createTransferError(502, "DnsFailure", stage, message, true);
  }
  if (lower.includes("invalid url") || lower.includes("unsupported protocol")) {
    return createTransferError(400, "InvalidUrl", stage, message, false);
  }

  return null;
}

function normalizeTransferError(error, fallbackStage = "unknown") {
  if (error instanceof TransferError) {
    return error;
  }

  const message = error?.message || "Unknown transfer error";
  const inferred = classifyLooseNetworkError(fallbackStage, message);
  if (inferred) {
    return inferred;
  }
  return createTransferError(500, "UnhandledError", fallbackStage, message, false);
}

function transferErrorResponse(error) {
  const normalized = normalizeTransferError(error);
  return new Response(JSON.stringify({
    error: {
      code: normalized.code,
      stage: normalized.stage,
      message: normalized.message,
      retryable: normalized.retryable,
      status_code: normalized.statusCode,
      details: normalized.details,
    },
  }), {
    status: normalized.statusCode,
    headers: {
      "Content-Type": "application/json",
      "X-Transfer-Error-Code": normalized.code,
      "X-Transfer-Error-Stage": normalized.stage,
      "X-Transfer-Retryable": normalized.retryable ? "true" : "false",
    },
  });
}

function parseS3Error(text) {
  const body = typeof text === "string" ? text : "";
  const codeMatch = body.match(/<Code>([^<]+)<\/Code>/i);
  const messageMatch = body.match(/<Message>([^<]+)<\/Message>/i);
  const code = codeMatch?.[1] || null;
  const message = messageMatch?.[1] || null;
  return { code, message };
}

function classifyRouteValidationError(message) {
  return createTransferError(400, "MissingParams", "request_decode", message, false);
}

function classifyConfigError(stage, message, details = null) {
  return createTransferError(500, "MissingEnv", stage, message, false, details);
}

function classifySourceResponseError(stage, status, statusText = "") {
  if (status === 404) {
    return createTransferError(404, "SourceNotFound", stage, `Source fetch returned ${status} ${statusText}`.trim(), false);
  }
  if (status === 403) {
    return createTransferError(403, "SourceAccessDenied", stage, `Source fetch returned ${status} ${statusText}`.trim(), false);
  }
  if (status === 416) {
    return createTransferError(416, "SourceInvalidRange", stage, `Source fetch returned ${status} ${statusText}`.trim(), false);
  }
  if (status === 429) {
    return createTransferError(429, "SourceRateLimited", stage, `Source fetch returned ${status} ${statusText}`.trim(), true);
  }
  if (status === 408) {
    return createTransferError(408, "SourceTimeout", stage, `Source fetch returned ${status} ${statusText}`.trim(), true);
  }
  if (status >= 500 && status <= 599) {
    return createTransferError(502, "SourceUpstreamError", stage, `Source fetch returned ${status} ${statusText}`.trim(), true);
  }
  return createTransferError(502, "SourceFetchFailed", stage, `Source fetch returned ${status} ${statusText}`.trim(), false);
}

function classifyFetchFailure(stage, error) {
  const message = error?.message || String(error);
  const inferred = classifyLooseNetworkError(stage, message);
  if (inferred) {
    return inferred;
  }

  return createTransferError(502, "FetchFailed", stage, message, true);
}

function classifyDestinationResponseError(status, errorText) {
  const parsed = parseS3Error(errorText);
  const code = parsed.code || "";
  const message = parsed.message || errorText || `Destination upload returned ${status}`;

  switch (code) {
    case "NoSuchBucket":
      return createTransferError(404, "DestNoSuchBucket", "dest_put", message, false, { s3_code: code });
    case "NoSuchKey":
      return createTransferError(404, "DestNoSuchKey", "dest_put", message, false, { s3_code: code });
    case "AccessDenied":
    case "InvalidAccessKeyId":
    case "SignatureDoesNotMatch":
    case "AuthorizationHeaderMalformed":
    case "ExpiredToken":
      return createTransferError(403, code, "dest_put", message, false, { s3_code: code });
    case "NoSuchUpload":
      return createTransferError(404, "DestNoSuchUpload", "dest_put", message, false, { s3_code: code });
    case "InvalidPart":
      return createTransferError(409, "DestInvalidPart", "dest_put", message, false, { s3_code: code });
    case "InvalidPartOrder":
      return createTransferError(409, "DestInvalidPartOrder", "dest_put", message, false, { s3_code: code });
    case "EntityTooSmall":
      return createTransferError(422, "DestEntityTooSmall", "dest_put", message, false, { s3_code: code });
    case "SlowDown":
      return createTransferError(429, "DestSlowDown", "dest_put", message, true, { s3_code: code });
    case "RequestTimeout":
      return createTransferError(504, "DestRequestTimeout", "dest_put", message, true, { s3_code: code });
    case "InternalError":
    case "ServiceUnavailable":
      return createTransferError(503, code, "dest_put", message, true, { s3_code: code });
  }

  if (status === 429) {
    return createTransferError(429, "DestRateLimited", "dest_put", message, true, { s3_code: code || null });
  }
  if (status === 408) {
    return createTransferError(504, "DestTimeout", "dest_put", message, true, { s3_code: code || null });
  }
  if (status >= 500 && status <= 599) {
    return createTransferError(503, "DestUpstreamError", "dest_put", message, true, { s3_code: code || null });
  }
  if (status === 404) {
    return createTransferError(404, "DestNotFound", "dest_put", message, false, { s3_code: code || null });
  }
  if (status === 403) {
    return createTransferError(403, "DestAccessDenied", "dest_put", message, false, { s3_code: code || null });
  }
  if (status === 400) {
    return createTransferError(400, "DestBadRequest", "dest_put", message, false, { s3_code: code || null });
  }

  return createTransferError(502, "DestUploadFailed", "dest_put", message, false, { s3_code: code || null });
}

export default {
  /**
   * Handle incoming HTTP requests
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   */
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    let pathname = url.pathname;
    if (pathname === "/worker") {
      pathname = "/";
    } else if (pathname.startsWith("/worker/")) {
      pathname = pathname.slice("/worker".length);
    }

    if (pathname === "/initiate-copy" && request.method === "POST") {
      try {
        const { r2Key, s3Url, size, offset, uploadId, partNumber } = await request.json();

        if (!r2Key || !s3Url) {
          return transferErrorResponse(classifyRouteValidationError("Missing required parameters"));
        }

        const task = {
          r2Key,
          s3Url,
          size: size || 0,
          offset: offset || 0,
          uploadId: uploadId || null,
          partNumber: partNumber || -1,
          failure: 0
        };

        const result = await processMessage(task, env);
        const safeKey = redactDestUrl(r2Key, task.partNumber);

        return new Response(JSON.stringify({
          message: `Successfully processed copy for ${safeKey} (Part: ${task.partNumber})`,
          etag: result.etag
        }), {
          headers: { "Content-Type": "application/json" },
          status: 200,
        });

      } catch (error) {
        console.error("Copy error:", error);
        return transferErrorResponse(normalizeTransferError(error, "copy_handler"));
      }
    } else if (pathname === "/upload-part") {
      if (request.method !== "POST") {
        return new Response("Method not allowed", { status: 405 });
      }
      try {
        const { r2Key, fileUrl, size, offset, uploadId, partNumber } = await request.json();

        if (!r2Key || !fileUrl) {
          return new Response("Missing required parameters", { status: 400 });
        }

        const task = {
          r2Key,
          fileUrl,
          size: size || 0,
          offset: offset || 0,
          uploadId: uploadId || null,
          partNumber: partNumber || -1,
          failure: 0
        };

        const result = await processDownloadMessage(task, env);
        if (!result?.etag) {
          throw createHttpError(502, "Upload succeeded but ETag is missing");
        }
        const safeKey = redactDestUrl(r2Key, task.partNumber);

        return new Response(JSON.stringify({
          message: `Successfully processed download for ${safeKey} (Part: ${task.partNumber})`,
          etag: result.etag
        }), {
          headers: { "Content-Type": "application/json" },
          status: 200,
        });
      } catch (error) {
        console.error("Upload-part error:", error);
        const status = Number.isInteger(error?.statusCode) ? error.statusCode : 500;
        return new Response(`Error processing upload-part: ${error.message}`, { status });
      }
    }

    return new Response("Worker is running. Send POST to /initiate-copy with task details.");
  },
};

/**
 * Process a single copy task
 * @param {Object} task
 * @param {Env} env
 * @param {AwsClient} sourceAwsClient
 * @param {AwsClient} destAwsClient
 */
async function processDownloadMessage(task, env) {
  const { r2Key, size, offset, fileUrl, uploadId, partNumber } = task;
  const start = offset
  const end = offset + size - 1
  const safeDest = redactDestUrl(r2Key, partNumber)
  const maxAttempts = 3

  try {
    console.log(`Download task: part=${partNumber} size=${size} range=${start}-${end} source=${fileUrl} dest=${safeDest}`)
    const userAgents = [
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15',
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0'
    ];
    const randomUA = userAgents[Math.floor(Math.random() * userAgents.length)];
    
    // Add realistic fetch headers and sec-ch-ua headers based on UA
    const isChrome = randomUA.includes('Chrome');
    const isWindows = randomUA.includes('Windows');
    
    const sourceHeaders = {
      'Range': `bytes=${start}-${end}`,
      'User-Agent': randomUA,
      'Accept': '*/*',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'gzip, deflate, br, zstd',
      'Connection': 'keep-alive',
      'Origin': 'https://www.youtube.com',
      'Referer': 'https://www.youtube.com/',
      'Sec-Fetch-Dest': 'empty',
      'Sec-Fetch-Mode': 'cors',
      'Sec-Fetch-Site': 'cross-site',
      'DNT': '1'
    };

    if (isChrome) {
      const chVersion = randomUA.match(/Chrome\/(\d+)/)?.[1] || '122';
      const platform = isWindows ? '"Windows"' : '"macOS"';
      sourceHeaders['sec-ch-ua'] = `"Chromium";v="${chVersion}", "Not(A:Brand";v="24", "Google Chrome";v="${chVersion}"`;
      sourceHeaders['sec-ch-ua-mobile'] = '?0';
      sourceHeaders['sec-ch-ua-platform'] = platform;
    }

    let r2KeyUrl = r2Key;
    const destUrl = new URL(r2KeyUrl);

    // Check if r2Key is already a presigned URL (contains X-Amz-Signature)
    const isPresignedUrl = destUrl.searchParams.has("X-Amz-Signature");

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      const sourceResponse = await fetch(fileUrl, {
        method: 'GET',
        headers: sourceHeaders,
        redirect: 'follow'
      });

      if (!sourceResponse.ok) {
        if (isRetryableStatus(sourceResponse.status) && attempt < maxAttempts) {
          console.error(`Source fetch failed for ${safeDest} part=${partNumber} attempt=${attempt}/${maxAttempts} status=${sourceResponse.status}`)
          await sleep(getRetryDelayMs(attempt, sourceResponse.headers));
          continue;
        }
        throw createHttpError(502, `Failed to download from source: ${sourceResponse.status} ${sourceResponse.statusText}`);
      }

      if (!sourceResponse.body) {
        if (attempt < maxAttempts) {
          console.error(`Source response missing body for ${safeDest} part=${partNumber} attempt=${attempt}/${maxAttempts}`)
          await sleep(backoffMs(attempt));
          continue;
        }
        throw createHttpError(502, "Source response has no body");
      }

      let requestHeaders = {
        "Content-Length": size.toString(),
      };

      if (!isPresignedUrl) {
        if (!env.SOURCE_ACCESS_KEY_ID || !env.SOURCE_SECRET_ACCESS_KEY) {
          throw new Error("Missing required environment variables: SOURCE_ACCESS_KEY_ID and SOURCE_SECRET_ACCESS_KEY must be set in wrangler.toml or as environment variables");
        }

        const destAwsClient = new AwsClient({
          accessKeyId: env.SOURCE_ACCESS_KEY_ID,
          secretAccessKey: env.SOURCE_SECRET_ACCESS_KEY,
          service: "s3",
          region: "auto",
        });

        if (partNumber !== -1) {
          destUrl.searchParams.set("partNumber", partNumber);
          destUrl.searchParams.set("uploadId", uploadId);
        }

        const signedRequest = await destAwsClient.sign(destUrl.toString(), {
          method: 'PUT',
          headers: {
            ...requestHeaders,
            "X-Amz-Content-Sha256": "UNSIGNED-PAYLOAD",
          },
        });
        requestHeaders = signedRequest.headers;
      } else if (partNumber !== -1) {
        destUrl.searchParams.set("partNumber", partNumber);
        destUrl.searchParams.set("uploadId", uploadId);
      }

      const s3Response = await fetch(destUrl.toString(), {
        method: 'PUT',
        body: sourceResponse.body,
        headers: requestHeaders,
      });

      if (!s3Response.ok) {
        const errorText = await s3Response.text();
        if (isRetryableStatus(s3Response.status) && attempt < maxAttempts) {
          console.error(`Upload failed for ${safeDest} part=${partNumber} attempt=${attempt}/${maxAttempts} status=${s3Response.status} body=${errorText}`)
          await sleep(getRetryDelayMs(attempt, s3Response.headers));
          continue;
        }
        throw new Error(`Failed to upload to S3: ${s3Response.status} ${errorText}`);
      }

      const etag = s3Response.headers.get("etag") || s3Response.headers.get("ETag");
      if (!etag) {
        if (attempt < maxAttempts) {
          console.error(`Upload ok but missing etag for ${safeDest} part=${partNumber} attempt=${attempt}/${maxAttempts}`)
          await sleep(backoffMs(attempt));
          continue;
        }
        throw createHttpError(502, `No ETag found in S3 response for uploadId=${uploadId} part=${partNumber}`);
      }

      console.log(`Upload success for ${safeDest} part=${partNumber} etag=${etag}`)
      return { etag };
    }

    throw createHttpError(502, `Upload failed after retries for ${safeDest} part=${partNumber}`);
  } catch (error) {
    console.error(`Processing failed for ${safeDest} (part ${partNumber}). Error: ${error.message}. source=${fileUrl}`);
    throw error; // Re-throw to be handled by caller
  }
}

function createHttpError(statusCode, message) {
  const err = new Error(message);
  err.statusCode = statusCode;
  return err;
}

function redactSourceUrl(fileUrl, start, end) {
  try {
    const u = new URL(fileUrl);
    const qp = u.searchParams;
    const redactKeys = new Set([]);
    const parts = [];
    for (const [k, v] of qp.entries()) {
      if (redactKeys.has(k)) {
        parts.push(`${k}=<redacted>`);
      } else {
        parts.push(`${k}=${v}`);
      }
    }
    parts.push(`range=${start}-${end}`);
    return `${u.protocol}//${u.host}${u.pathname}?${parts.join("&")}`;
  } catch {
    return "<invalid_source_url>";
  }
}

function redactDestUrl(r2Key, partNumber) {
  try {
    const u = new URL(r2Key);
    const qp = u.searchParams;
    const uploadId = qp.get("uploadId");
    const pn = qp.get("partNumber") || (Number.isInteger(partNumber) ? String(partNumber) : "");
    const out = [];
    if (pn) out.push(`partNumber=${pn}`);
    if (uploadId) out.push(`uploadId=${uploadId.slice(0, 8)}...`);
    return `${u.protocol}//${u.host}${u.pathname}${out.length ? "?" + out.join("&") : ""}`;
  } catch {
    return "<invalid_dest_url>";
  }
}


/**
 * Process a single copy task
 * @param {Object} task
 * @param {Env} env
 * @param {AwsClient} sourceAwsClient
 * @param {AwsClient} destAwsClient
 */
async function processMessage(task, env) {
  const { r2Key, size, offset, s3Url, uploadId, partNumber } = task;
  const safeKey = redactDestUrl(r2Key, partNumber);
  const maxAttempts = Math.max(1, Number.parseInt(env?.UPLOAD_RETRY_ATTEMPTS || "1", 10) || 1);
  const sourceFetchTimeoutMs = getEnvTimeoutMs(env, "SOURCE_FETCH_TIMEOUT_SECONDS", 5);
  const destUploadTimeoutMs = getEnvTimeoutMs(env, "DEST_UPLOAD_TIMEOUT_SECONDS", 180);
  const copyStartedAt = Date.now();
  let attemptsUsed = 0;
  let retryWaitMsTotal = 0;
  let lastSourceFetchMs = null;
  let lastDestUploadMs = null;
  let sourceFetchMsTotal = 0;
  let destUploadMsTotal = 0;

  try {
    // Validate required environment variables for source
    if (!env.SOURCE_ACCESS_KEY_ID || !env.SOURCE_SECRET_ACCESS_KEY) {
      throw classifyConfigError("config", "Missing required environment variables: SOURCE_ACCESS_KEY_ID and SOURCE_SECRET_ACCESS_KEY must be set in wrangler.toml or as environment variables");
    }

    let r2KeyUrl = r2Key;
    let sourceEndpoint = undefined;
    if (typeof r2KeyUrl === "string" && r2KeyUrl.startsWith("s3://")) {
      const withoutScheme = r2KeyUrl.slice("s3://".length);
      const firstSlash = withoutScheme.indexOf("/");
      if (firstSlash > 0) {
        const bucket = withoutScheme.slice(0, firstSlash);
        const key = withoutScheme.slice(firstSlash + 1);
        sourceEndpoint = `https://${bucket}.s3.amazonaws.com`;
        r2KeyUrl = `${sourceEndpoint}/${key}`;
      }
    }
    const sourceUrl = new URL(r2KeyUrl);

    // Initialize AwsClient for Source (Read)
    const sourceAwsClient = new AwsClient({
      accessKeyId: env.SOURCE_ACCESS_KEY_ID,
      secretAccessKey: env.SOURCE_SECRET_ACCESS_KEY,
      service: "s3",
      region: "auto",
      ...(sourceEndpoint ? { endpoint: sourceEndpoint } : {}),
    });

    // Initialize AwsClient for Destination (Write)
    const s3Host = new URL(s3Url).hostname;
    const destAccessKey = env[s3Host + "_ak"] || env.AWS_ACCESS_KEY_ID;
    const destSecretKey = env[s3Host + "_sk"] || env.AWS_SECRET_ACCESS_KEY;
    
    if (!destAccessKey || !destSecretKey) {
      throw classifyConfigError("config", `Missing required environment variables for destination S3 (${s3Host}): ${s3Host}_ak/${s3Host}_sk or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY`);
    }

    const destAwsClient = new AwsClient({
      accessKeyId: destAccessKey,
      secretAccessKey: destSecretKey,
      region: env.AWS_REGION || "auto",
      service: "s3",
    });

    // 1. Download range from Source R2 (using S3 API)
    const uploadUrl = new URL(s3Url);

    // If partNumber is -1, perform a standard PUT object upload
    if (partNumber !== -1) {
      uploadUrl.searchParams.set("partNumber", partNumber);
      uploadUrl.searchParams.set("uploadId", uploadId);
    }

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      attemptsUsed = attempt;
      let sourceResponse;
      const sourceFetchStartedAt = Date.now();
      try {
        sourceResponse = await fetchWithTimeout((signal) => {
          return sourceAwsClient.fetch(sourceUrl, {
            method: 'GET',
            headers: {
              'Range': `bytes=${offset}-${offset + size - 1}`
            },
            signal,
          });
        }, sourceFetchTimeoutMs);
        lastSourceFetchMs = elapsedMs(sourceFetchStartedAt);
        sourceFetchMsTotal += lastSourceFetchMs;
      } catch (error) {
        lastSourceFetchMs = elapsedMs(sourceFetchStartedAt);
        sourceFetchMsTotal += lastSourceFetchMs;
        if (isAbortError(error)) {
          error = createTransferError(
            408,
            "SourceTimeout",
            "source_fetch",
            `Source fetch timed out after ${Math.floor(sourceFetchTimeoutMs / 1000)}s`,
            true,
          );
        }
        const sourceErr = classifyFetchFailure("source_fetch", error);
        if (sourceErr.retryable && attempt < maxAttempts) {
          console.error(`Source download failed for ${safeKey} part=${partNumber} attempt=${attempt}/${maxAttempts} err=${sourceErr.message}`)
          const retryDelayMs = backoffMs(attempt);
          retryWaitMsTotal += retryDelayMs;
          await sleep(retryDelayMs);
          continue;
        }
        throw sourceErr;
      }

      if (!sourceResponse.ok) {
        const sourceErr = classifySourceResponseError("source_fetch", sourceResponse.status, sourceResponse.statusText || "");
        if (sourceErr.retryable && attempt < maxAttempts) {
          console.error(`Source download failed for ${safeKey} part=${partNumber} attempt=${attempt}/${maxAttempts} status=${sourceResponse.status}`)
          const retryDelayMs = getRetryDelayMs(attempt, sourceResponse.headers);
          retryWaitMsTotal += retryDelayMs;
          await sleep(retryDelayMs);
          continue;
        }
        throw sourceErr;
      }

      if (!sourceResponse.body) {
        if (attempt < maxAttempts) {
          console.error(`Source response missing body for ${safeKey} part=${partNumber} attempt=${attempt}/${maxAttempts}`)
          const retryDelayMs = backoffMs(attempt);
          retryWaitMsTotal += retryDelayMs;
          await sleep(retryDelayMs);
          continue;
        }
        throw createTransferError(502, "SourceBodyMissing", "source_fetch", "Source response has no body", true);
      }

      let signedRequest;
      try {
        signedRequest = await destAwsClient.sign(uploadUrl.toString(), {
          method: "PUT",
          headers: {
            "Content-Length": size.toString(),
            "X-Amz-Content-Sha256": "UNSIGNED-PAYLOAD",
          },
        });
      } catch (error) {
        throw createTransferError(403, "DestSignFailed", "dest_sign", error?.message || "Failed to sign destination request", false);
      }

      let s3Response;
      const destUploadStartedAt = Date.now();
      try {
        s3Response = await fetchWithTimeout((signal) => {
          return fetch(uploadUrl.toString(), {
            method: "PUT",
            body: sourceResponse.body,
            headers: signedRequest.headers,
            signal,
          });
        }, destUploadTimeoutMs);
        lastDestUploadMs = elapsedMs(destUploadStartedAt);
        destUploadMsTotal += lastDestUploadMs;
      } catch (error) {
        lastDestUploadMs = elapsedMs(destUploadStartedAt);
        destUploadMsTotal += lastDestUploadMs;
        if (isAbortError(error)) {
          error = createTransferError(
            504,
            "DestTimeout",
            "dest_put",
            `Destination upload timed out after ${Math.floor(destUploadTimeoutMs / 1000)}s`,
            true,
          );
        }
        const destErr = classifyFetchFailure("dest_put", error);
        if (destErr.retryable && attempt < maxAttempts) {
          console.error(`Upload failed for ${safeKey} part=${partNumber} attempt=${attempt}/${maxAttempts} err=${destErr.message}`)
          const retryDelayMs = backoffMs(attempt);
          retryWaitMsTotal += retryDelayMs;
          await sleep(retryDelayMs);
          continue;
        }
        throw destErr;
      }

      if (!s3Response.ok) {
        const errorText = await s3Response.text();
        const destErr = classifyDestinationResponseError(s3Response.status, errorText);
        if (destErr.retryable && attempt < maxAttempts) {
          console.error(`Upload failed for ${safeKey} part=${partNumber} attempt=${attempt}/${maxAttempts} status=${s3Response.status} body=${errorText}`)
          const retryDelayMs = getRetryDelayMs(attempt, s3Response.headers);
          retryWaitMsTotal += retryDelayMs;
          await sleep(retryDelayMs);
          continue;
        }
        throw destErr;
      }

      const etag = s3Response.headers.get("etag") || s3Response.headers.get("ETag");
      if (!etag) {
        throw createTransferError(502, "DestMissingETag", "response_parse", `No ETag found in S3 response for part=${partNumber}`, false);
      }
      console.log(
        `Copy timing success for ${safeKey} part=${partNumber} attempts=${attemptsUsed}/${maxAttempts} ` +
        `source_fetch_ms=${formatTimingMs(lastSourceFetchMs)} source_fetch_ms_total=${formatTimingMs(sourceFetchMsTotal)} ` +
        `dest_upload_ms=${formatTimingMs(lastDestUploadMs)} dest_upload_ms_total=${formatTimingMs(destUploadMsTotal)} ` +
        `retry_wait_ms_total=${formatTimingMs(retryWaitMsTotal)} total_copy_ms=${formatTimingMs(elapsedMs(copyStartedAt))}`
      );
      console.log(`Copy success for ${safeKey} part=${partNumber} etag=${etag || ""}`)
      return { etag };
    }

    throw createTransferError(502, "CopyRetryExhausted", "dest_put", `Copy failed after retries for ${safeKey} part=${partNumber}`, false);
  } catch (error) {
    const normalized = normalizeTransferError(error, "copy_pipeline");
    console.error(
      `Copy timing failed for ${safeKey} part=${partNumber} attempts=${attemptsUsed}/${maxAttempts} ` +
      `failed_stage=${normalized.stage} error_code=${normalized.code} ` +
      `source_fetch_ms=${formatTimingMs(lastSourceFetchMs)} source_fetch_ms_total=${formatTimingMs(sourceFetchMsTotal)} ` +
      `dest_upload_ms=${formatTimingMs(lastDestUploadMs)} dest_upload_ms_total=${formatTimingMs(destUploadMsTotal)} ` +
      `retry_wait_ms_total=${formatTimingMs(retryWaitMsTotal)} total_copy_ms=${formatTimingMs(elapsedMs(copyStartedAt))}`
    );
    console.error(`Processing failed for ${safeKey} (part ${partNumber}). Error: ${normalized.message}`);
    throw normalized;
  }
}
