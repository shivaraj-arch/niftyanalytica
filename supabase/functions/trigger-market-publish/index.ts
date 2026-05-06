import { serve } from "https://deno.land/std@0.224.0/http/server.ts";

type HolidayConfig = {
  year: number;
  dates: string[];
};

const IST_OFFSET_MS = 5.5 * 60 * 60 * 1000;

function getIstNow() {
  const now = new Date();
  return new Date(now.getTime() + IST_OFFSET_MS);
}

function toIstDateParts(date: Date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hour = date.getUTCHours();
  const minute = date.getUTCMinutes();
  return {
    year,
    date: `${year}-${month}-${day}`,
    hour,
    minute,
    weekday: date.getUTCDay(),
  };
}

function loadHolidaySet(): Set<string> {
  const raw = Deno.env.get("NSE_TRADING_HOLIDAYS_JSON") ?? "[]";
  const parsed = JSON.parse(raw) as HolidayConfig[] | string[];
  if (Array.isArray(parsed) && parsed.length > 0 && typeof parsed[0] === "string") {
    return new Set(parsed as string[]);
  }

  const dates = (parsed as HolidayConfig[]).flatMap((entry) => entry.dates ?? []);
  return new Set(dates);
}

async function dispatchGithubWorkflow(triggerDate: string, triggerHour: number, newsletter: boolean) {
  const githubToken = Deno.env.get("GITHUB_TOKEN");
  const owner = Deno.env.get("GITHUB_OWNER");
  const repo = Deno.env.get("GITHUB_REPO_APP") ?? Deno.env.get("GITHUB_REPO");
  const workflowId = Deno.env.get("GITHUB_WORKFLOW_ID") ?? "publish-market-data.yml";
  const ref = Deno.env.get("GITHUB_WORKFLOW_REF") ?? "main";

  if (!githubToken || !owner || !repo) {
    throw new Error("Missing GITHUB_TOKEN, GITHUB_OWNER, or GITHUB_REPO_APP/GITHUB_REPO.");
  }

  const response = await fetch(
    `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflowId}/dispatches`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${githubToken}`,
        Accept: "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "supabase-edge-function",
      },
      body: JSON.stringify({
        ref,
        inputs: {
          telegram: "true",
          x: "false",
          linkedin: "false",
          buffer: "true",
          whatsapp: "false",
          dry_run: "false",
          generate_ai: "true",
          newsletter: newsletter ? "true" : "false",
        },
      }),
    },
  );

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`GitHub dispatch failed: ${response.status} ${text}`);
  }

  return {
    ok: true,
    triggerDate,
    triggerHour,
    workflowId,
    ref,
    newsletter,
  };
}

serve(async (request) => {
  try {
    const istNow = getIstNow();
    const parts = toIstDateParts(istNow);
    const holidays = loadHolidaySet();
    const body = request.method === "POST"
      ? await request.json().catch(() => ({})) as { newsletter?: boolean }
      : {};

    if (parts.weekday === 0 || parts.weekday === 6) {
      return Response.json({
        ok: true,
        skipped: true,
        reason: "Weekend",
        at: parts,
      });
    }

    if (holidays.has(parts.date)) {
      return Response.json({
        ok: true,
        skipped: true,
        reason: "NSE holiday",
        at: parts,
      });
    }

    const newsletter = typeof body.newsletter === "boolean" ? body.newsletter : parts.hour === 20;
    const result = await dispatchGithubWorkflow(parts.date, parts.hour, newsletter);
    return Response.json({
      ok: true,
      skipped: false,
      at: parts,
      dispatch: result,
    });
  } catch (error) {
    return Response.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      },
      { status: 500 },
    );
  }
});