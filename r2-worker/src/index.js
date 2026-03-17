import { AwsClient } from "aws4fetch";

export default {
  /**
   * Handle incoming HTTP requests
   * @param {Request} request
   * @param {Env} env
   * @param {ExecutionContext} ctx
   */
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/initiate-copy" && request.method === "POST") {
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

        return new Response(JSON.stringify({
          message: `Successfully processed copy for ${r2Key} (Part: ${task.partNumber})`,
          etag: result.etag
        }), {
          headers: { "Content-Type": "application/json" },
          status: 200,
        });

      } catch (error) {
        console.error("Copy error:", error);
        return new Response(`Error processing copy: ${error.message}`, { status: 500 });
      }
    } else if (url.pathname === '/upload-part') {
      const { r2Key, fileUrl, size, offset, uploadId, partNumber } = await request.json();

      if (!r2Key) {
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

      return new Response(JSON.stringify({
        message: `Successfully processed download for ${r2Key} (Part: ${task.partNumber})`,
        etag: result.etag
      }), {
        headers: { "Content-Type": "application/json" },
        status: 200,
      });

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

  try {
    const sourceResponse = await fetch(fileUrl, {
      method: 'GET',
      headers: {
        'Range': `bytes=${start}-${end}`,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-us,en;q=0.5',
        'Sec-Fetch-Mode': 'navigate'
      }
    });

    console.log(`Source Response for ${r2Key} (Part: ${partNumber}):`, JSON.stringify({
      status: sourceResponse.status,
      headers: Object.fromEntries(sourceResponse.headers.entries()),
    }, null, 2));

    if (!sourceResponse.ok) {
      return new Response(`Failed to download from source: ${sourceResponse.status} ${sourceResponse.statusText}`, { status: 502 });
    }

    if (!sourceResponse.body) {
      return new Response('Source response has no body', { status: 502 });
    }

    let r2KeyUrl = r2Key;
    const destUrl = new URL(r2KeyUrl);

    // Check if r2Key is already a presigned URL (contains X-Amz-Signature)
    const isPresignedUrl = destUrl.searchParams.has("X-Amz-Signature");

    let requestHeaders = {
      "Content-Length": size.toString(),
    };

    // If r2Key is already a presigned URL, use it directly without modification
    // Otherwise, sign it ourselves
    if (!isPresignedUrl) {
      // Validate required environment variables
      if (!env.SOURCE_ACCESS_KEY_ID || !env.SOURCE_SECRET_ACCESS_KEY) {
        throw new Error("Missing required environment variables: SOURCE_ACCESS_KEY_ID and SOURCE_SECRET_ACCESS_KEY must be set in wrangler.toml or as environment variables");
      }

      // Initialize AwsClient for Destination (Upload to R2)
      const destAwsClient = new AwsClient({
        accessKeyId: env.SOURCE_ACCESS_KEY_ID,
        secretAccessKey: env.SOURCE_SECRET_ACCESS_KEY,
        service: "s3",
        region: "auto",
      });

      // If partNumber is -1, perform a standard PUT object upload
      if (partNumber !== -1) {
        destUrl.searchParams.set("partNumber", partNumber);
        destUrl.searchParams.set("uploadId", uploadId);
      }

      // Sign the request
      const signedRequest = await destAwsClient.sign(destUrl.toString(), {
        method: 'PUT',
        headers: {
          ...requestHeaders,
          "X-Amz-Content-Sha256": "UNSIGNED-PAYLOAD",
        },
      });
      requestHeaders = signedRequest.headers;
    }

    // 1. Upload to R2 (using S3 API)
    const s3Response = await fetch(destUrl.toString(), {
      method: 'PUT',
      body: sourceResponse.body,
      headers: requestHeaders,
    });

    if (!s3Response.ok) {
      const errorText = await s3Response.text();
      throw new Error(`Failed to upload to S3: ${s3Response.status} ${errorText}`);
    }

    // Log full S3 response headers and content
    const s3Headers = Object.fromEntries(s3Response.headers.entries());
    const s3Body = await s3Response.text();
    console.log(`S3 Response for ${r2Key} (Part: ${partNumber}):`, JSON.stringify({
      status: s3Response.status,
      headers: s3Headers,
      body: s3Body
    }, null, 2));

    // 3. Check for ETag presence (D1 update removed)
    let etag = null;
    etag = s3Response.headers.get("etag");
    if (!etag) {
      console.warn(`No ETag found in S3 response for ${uploadId} part ${partNumber}`);
    }

    console.log(`Successfully copied ${partNumber === -1 ? 'file (single put)' : `part ${partNumber}`} for ${r2Key} to S3`);

    return { etag };
  } catch (error) {
    console.error(`Processing failed for ${r2Key} (part ${partNumber}). Error: ${error.message}`);
    throw error; // Re-throw to be handled by caller
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
    const sourceUrl = new URL(r2KeyUrl);

    // Initialize AwsClient for Source (Read)
    const sourceAwsClient = new AwsClient({
      accessKeyId: env.SOURCE_ACCESS_KEY_ID,
      secretAccessKey: env.SOURCE_SECRET_ACCESS_KEY,
      service: "s3",
      region: "auto",
    });

    // Initialize AwsClient for Destination (Write)
    const s3Host = new URL(s3Url).hostname;
    const destAccessKey = env[s3Host + "_ak"];
    const destSecretKey = env[s3Host + "_sk"];
    
    if (!destAccessKey || !destSecretKey) {
      throw new Error(`Missing required environment variables for destination S3 (${s3Host}): ${s3Host}_ak and ${s3Host}_sk must be set in wrangler.toml or as environment variables`);
    }

    const destAwsClient = new AwsClient({
      accessKeyId: destAccessKey,
      secretAccessKey: destSecretKey,
      region: env.AWS_REGION || "auto",
      service: "s3",
    });

    // 1. Download range from Source R2 (using S3 API)
    const sourceResponse = await sourceAwsClient.fetch(sourceUrl, {
      method: 'GET',
      headers: {
        'Range': `bytes=${offset}-${offset + size - 1}`
      }
    });

    if (!sourceResponse.ok) {
      throw new Error(`Failed to download from source: ${sourceResponse.status}`);
    }

    if (!sourceResponse.body) {
      throw new Error("Source response has no body");
    }

    // 2. Upload part to Destination S3 Compatible Storage
    const uploadUrl = new URL(s3Url);

    // If partNumber is -1, perform a standard PUT object upload
    if (partNumber !== -1) {
      uploadUrl.searchParams.set("partNumber", partNumber);
      uploadUrl.searchParams.set("uploadId", uploadId);
    }

    // Sign and execute the PUT request
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
      throw new Error(`Failed to upload to S3: ${s3Response.status} ${errorText}`);
    }

    // Log full S3 response headers and content
    const s3Headers = Object.fromEntries(s3Response.headers.entries());
    const s3Body = await s3Response.text();
    console.log(`S3 Response for ${r2Key} (Part: ${partNumber}):`, JSON.stringify({
      status: s3Response.status,
      headers: s3Headers,
      body: s3Body
    }, null, 2));

    // 3. Check for ETag presence (D1 update removed)
    let etag = null;
    etag = s3Response.headers.get("etag");
    if (!etag) {
      console.warn(`No ETag found in S3 response for ${uploadId} part ${partNumber}`);
    }

    console.log(`Successfully copied ${partNumber === -1 ? 'file (single put)' : `part ${partNumber}`} for ${r2Key} to S3`);

    return { etag };
  } catch (error) {
    console.error(`Processing failed for ${r2Key} (part ${partNumber}). Error: ${error.message}`);
    throw error; // Re-throw to be handled by caller
  }
}


