import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { fetchNseSnapshot, getMarketWindowState } from "../_shared/nse_snapshot.ts";
import { buildNewsBundle } from "../_shared/news_feed.ts";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-snapshot-secret",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Content-Type": "application/json",
};

const DEFAULT_BUCKET = "public-data";
const DEFAULT_OBJECT_PATH = "live/live-snapshot.json";

function jsonResponse(payload: unknown, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: CORS_HEADERS,
  });
}

function encodeObjectPath(objectPath: string) {
  return objectPath.split("/").map((segment) => encodeURIComponent(segment)).join("/");
}

function getPublicSnapshotUrl(supabaseUrl: string, bucket: string, objectPath: string) {
  return `${supabaseUrl.replace(/\/$/, "")}/storage/v1/object/public/${bucket}/${encodeObjectPath(objectPath)}`;
}

async function loadExistingSnapshot(publicUrl: string) {
  try {
    const response = await fetch(`${publicUrl}${publicUrl.includes("?") ? "&" : "?"}t=${Date.now()}`, {
      cache: "no-store",
    });
    if (!response.ok) {
      return null;
    }
    return await response.json();
  } catch {
    return null;
  }
}

function hasValidSnapshotSecret(request: Request) {
  const allowPublic = Deno.env.get("NSE_SNAPSHOT_ALLOW_PUBLIC") === "true";
  if (allowPublic) {
    return true;
  }

  const expectedSecret = Deno.env.get("NSE_SNAPSHOT_SECRET");
  if (!expectedSecret) {
    return false;
  }

  const bearerToken = request.headers.get("authorization")?.replace(/^Bearer\s+/i, "");
  const headerToken = request.headers.get("x-snapshot-secret");
  return bearerToken === expectedSecret || headerToken === expectedSecret;
}

serve(async (request: Request) => {
  if (request.method === "OPTIONS") {
    return new Response("ok", { headers: CORS_HEADERS });
  }

  if (request.method !== "GET") {
    return jsonResponse({ ok: false, error: "Method not allowed" }, 405);
  }

  try {
    if (!hasValidSnapshotSecret(request)) {
      return jsonResponse({ ok: false, error: "Unauthorized" }, 401);
    }

    const marketWindow = getMarketWindowState();
    if (!marketWindow.canFetchSnapshot) {
      return jsonResponse({
        ok: true,
        skipped: true,
        session: marketWindow.session,
        reason: marketWindow.reason,
        at: marketWindow.at,
      });
    }

    const supabaseUrl = Deno.env.get("SUPABASE_URL")?.replace(/\/$/, "");
    const bucket = Deno.env.get("LIVE_SNAPSHOT_BUCKET") ?? DEFAULT_BUCKET;
    const objectPath = Deno.env.get("LIVE_SNAPSHOT_OBJECT_PATH") ?? DEFAULT_OBJECT_PATH;
    const existingSnapshot = supabaseUrl
      ? await loadExistingSnapshot(getPublicSnapshotUrl(supabaseUrl, bucket, objectPath))
      : null;
    const snapshot = await fetchNseSnapshot();
    const newsBundle = await buildNewsBundle(existingSnapshot ?? undefined);

    return jsonResponse({
      ...snapshot,
      ...newsBundle,
    });
  } catch (error) {
    return jsonResponse(
      {
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      },
      500,
    );
  }
});