import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { fetchNseSnapshot, getMarketWindowState } from "../_shared/nse_snapshot.ts";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Content-Type": "application/json",
};

function jsonResponse(payload: unknown, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: CORS_HEADERS,
  });
}

function hasValidSnapshotSecret(request: Request) {
  const expectedSecret = Deno.env.get("NSE_SNAPSHOT_SECRET");
  if (!expectedSecret) {
    return true;
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
    if (!marketWindow.open) {
      return jsonResponse({ ok: true, skipped: true, reason: marketWindow.reason, at: marketWindow.at });
    }

    const snapshot = await fetchNseSnapshot();

    return jsonResponse(snapshot);
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