import { serve } from "https://deno.land/std@0.224.0/http/server.ts";

type DispatchRequestBody = {
  target?: "pages-live-morning" | "pages-live-afternoon" | "pages-news-window";
};

serve(async (request) => {
  const body = request.method === "POST"
    ? await request.json().catch(() => ({})) as DispatchRequestBody
    : {};

  return Response.json({
    ok: true,
    skipped: true,
    legacy: true,
    target: body.target ?? null,
    reason: "Supabase now owns live snapshot and news refresh directly. trigger-pages-refresh is deprecated.",
  });
});