import { AwsClient } from "aws4fetch";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function backoffMs(attempt) {
  const base = Math.min(1000 * 2 ** (attempt - 1), 8000);
  const jitter = Math.floor(Math.random() * 250);
  return base + jitter;
}

function isRetryableStatus(status) {
  return status === 408 || status === 429 || (status >= 500 && status <= 599);
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
          return new Response("Missing required parameters", { status: 400 });
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
        return new Response(`Error processing copy: ${error.message}`, { status: 500 });
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
  const maxAttempts = Math.max(1, Number.parseInt(env?.UPLOAD_RETRY_ATTEMPTS || "3", 10) || 3)

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
          await sleep(backoffMs(attempt));
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
          await sleep(backoffMs(attempt));
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

  try {
    // Validate required environment variables for source
    if (!env.SOURCE_ACCESS_KEY_ID || !env.SOURCE_SECRET_ACCESS_KEY) {
      throw new Error("Missing required environment variables: SOURCE_ACCESS_KEY_ID and SOURCE_SECRET_ACCESS_KEY must be set in wrangler.toml or as environment variables");
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
      throw new Error(`Missing required environment variables for destination S3 (${s3Host}): ${s3Host}_ak/${s3Host}_sk or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY`);
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

    const safeKey = redactDestUrl(r2Key, partNumber);
    const maxAttempts = Math.max(1, Number.parseInt(env?.UPLOAD_RETRY_ATTEMPTS || "3", 10) || 3)

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      const sourceResponse = await sourceAwsClient.fetch(sourceUrl, {
        method: 'GET',
        headers: {
          'Range': `bytes=${offset}-${offset + size - 1}`
        }
      });

      if (!sourceResponse.ok) {
        if (isRetryableStatus(sourceResponse.status) && attempt < maxAttempts) {
          console.error(`Source download failed for ${safeKey} part=${partNumber} attempt=${attempt}/${maxAttempts} status=${sourceResponse.status}`)
          await sleep(backoffMs(attempt));
          continue;
        }
        throw new Error(`Failed to download from source: ${sourceResponse.status}`);
      }

      if (!sourceResponse.body) {
        if (attempt < maxAttempts) {
          console.error(`Source response missing body for ${safeKey} part=${partNumber} attempt=${attempt}/${maxAttempts}`)
          await sleep(backoffMs(attempt));
          continue;
        }
        throw new Error("Source response has no body");
      }

      const signedRequest = await destAwsClient.sign(uploadUrl.toString(), {
        method: "PUT",
        headers: {
          "Content-Length": size.toString(),
          "X-Amz-Content-Sha256": "UNSIGNED-PAYLOAD",
        },
      });

      const s3Response = await fetch(uploadUrl.toString(), {
        method: "PUT",
        body: sourceResponse.body,
        headers: signedRequest.headers,
      });

      if (!s3Response.ok) {
        const errorText = await s3Response.text();
        if (isRetryableStatus(s3Response.status) && attempt < maxAttempts) {
          console.error(`Upload failed for ${safeKey} part=${partNumber} attempt=${attempt}/${maxAttempts} status=${s3Response.status} body=${errorText}`)
          await sleep(backoffMs(attempt));
          continue;
        }
        throw new Error(`Failed to upload to S3: ${s3Response.status} ${errorText}`);
      }

      const etag = s3Response.headers.get("etag") || s3Response.headers.get("ETag");
      console.log(`Copy success for ${safeKey} part=${partNumber} etag=${etag || ""}`)
      return { etag };
    }

    throw new Error(`Copy failed after retries for ${safeKey} part=${partNumber}`);
  } catch (error) {
    const safeKey = redactDestUrl(r2Key, partNumber);
    console.error(`Processing failed for ${safeKey} (part ${partNumber}). Error: ${error.message}`);
    throw error; // Re-throw to be handled by caller
  }
}
