import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { getMarketWindowState } from "../_shared/nse_snapshot.ts";
import { buildNewsBundle } from "../_shared/news_feed.ts";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Content-Type": "application/json",
};

const DEFAULT_BUCKET = "public-data";
const DEFAULT_OBJECT_PATH = "live/live-snapshot.json";
const DEFAULT_CACHE_CONTROL = "60";

type RefreshRequestBody = {
  force?: boolean;
};

function jsonResponse(payload: unknown, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: CORS_HEADERS,
  });
}

function getEnv(name: string, fallback?: string) {
  const value = Deno.env.get(name) ?? fallback;
  if (!value) {
    throw new Error(`Missing ${name}.`);
  }
  return value;
}

function encodeObjectPath(objectPath: string) {
  return objectPath.split("/").map((segment) => encodeURIComponent(segment)).join("/");
}

function getPublicSnapshotUrl(supabaseUrl: string, bucket: string, objectPath: string) {
  return `${supabaseUrl.replace(/\/$/, "")}/storage/v1/object/public/${bucket}/${encodeObjectPath(objectPath)}`;
}

async function ensurePublicBucket(supabaseUrl: string, serviceRoleKey: string, bucket: string) {
  const headers = {
    Authorization: `Bearer ${serviceRoleKey}`,
    apikey: serviceRoleKey,
  };

  const existingBucket = await fetch(`${supabaseUrl}/storage/v1/bucket/${encodeURIComponent(bucket)}`, {
    headers,
  });

  if (existingBucket.ok) {
    return;
  }

  const existingBucketText = await existingBucket.text();
  const bucketMissing = existingBucket.status === 404 || /bucket not found/i.test(existingBucketText);
  if (!bucketMissing) {
    throw new Error(`Bucket lookup failed: ${existingBucket.status} ${existingBucketText}`);
  }

  const createdBucket = await fetch(`${supabaseUrl}/storage/v1/bucket`, {
    method: "POST",
    headers: {
      ...headers,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      id: bucket,
      name: bucket,
      public: true,
    }),
  });

  if (!createdBucket.ok) {
    throw new Error(`Bucket create failed: ${createdBucket.status} ${await createdBucket.text()}`);
  }
}

async function uploadSnapshot(
  supabaseUrl: string,
  serviceRoleKey: string,
  bucket: string,
  objectPath: string,
  cacheControl: string,
  snapshot: unknown,
) {
  const response = await fetch(`${supabaseUrl}/storage/v1/object/${bucket}/${encodeObjectPath(objectPath)}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${serviceRoleKey}`,
      apikey: serviceRoleKey,
      "Content-Type": "application/json",
      "Cache-Control": `max-age=${cacheControl}`,
      "x-upsert": "true",
    },
    body: JSON.stringify(snapshot),
  });

  if (!response.ok) {
    throw new Error(`Snapshot upload failed: ${response.status} ${await response.text()}`);
  }
}

async function loadExistingSnapshot(publicUrl: string) {
  const response = await fetch(`${publicUrl}${publicUrl.includes("?") ? "&" : "?"}t=${Date.now()}`, {
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Existing snapshot fetch failed: ${response.status}`);
  }

  return await response.json();
}

serve(async (request) => {
  if (request.method === "OPTIONS") {
    return new Response("ok", { headers: CORS_HEADERS });
  }

  if (request.method !== "POST") {
    return jsonResponse({ ok: false, error: "Method not allowed" }, 405);
  }

  try {
    const body = await request.json().catch(() => ({})) as RefreshRequestBody;
    const marketWindow = getMarketWindowState();
    const forceRefresh = body.force === true;

    if (!forceRefresh) {
      if (marketWindow.reason === "Weekend" || marketWindow.reason === "NSE holiday") {
        return jsonResponse({
          ok: true,
          skipped: true,
          session: marketWindow.session,
          reason: marketWindow.reason,
          at: marketWindow.at,
        });
      }

      if ((marketWindow.at.hour ?? 0) < 6) {
        return jsonResponse({
          ok: true,
          skipped: true,
          session: marketWindow.session,
          reason: "Outside news refresh window",
          at: marketWindow.at,
        });
      }
    }

    const supabaseUrl = getEnv("SUPABASE_URL").replace(/\/$/, "");
    const serviceRoleKey = getEnv("SUPABASE_SERVICE_ROLE_KEY");
    const bucket = Deno.env.get("LIVE_SNAPSHOT_BUCKET") ?? DEFAULT_BUCKET;
    const objectPath = Deno.env.get("LIVE_SNAPSHOT_OBJECT_PATH") ?? DEFAULT_OBJECT_PATH;
    const cacheControl = Deno.env.get("LIVE_SNAPSHOT_CACHE_CONTROL") ?? DEFAULT_CACHE_CONTROL;
    const publicUrl = getPublicSnapshotUrl(supabaseUrl, bucket, objectPath);

    const existingSnapshot = await loadExistingSnapshot(publicUrl);
    const newsBundle = await buildNewsBundle(existingSnapshot ?? undefined);
    const snapshotPayload = {
      ...existingSnapshot,
      ...newsBundle,
    };

    await ensurePublicBucket(supabaseUrl, serviceRoleKey, bucket);
    await uploadSnapshot(supabaseUrl, serviceRoleKey, bucket, objectPath, cacheControl, snapshotPayload);

    return jsonResponse({
      ok: true,
      skipped: false,
      session: marketWindow.session,
      storage: { bucket, objectPath, publicUrl },
      snapshot: snapshotPayload,
    });
  } catch (error) {
    return jsonResponse({
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    }, 500);
  }
});