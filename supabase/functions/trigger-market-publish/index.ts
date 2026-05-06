import { serve } from "https://deno.land/std@0.224.0/http/server.ts";

type HolidayConfig = {
  year: number;
  dates: string[];
};

const IST_TIME_ZONE = "Asia/Kolkata";
const IST_PARTS_FORMATTER = new Intl.DateTimeFormat("en-CA", {
  timeZone: IST_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  weekday: "short",
  hour12: false,
});

function toIstDateParts(date: Date) {
  const parts = Object.fromEntries(
    IST_PARTS_FORMATTER.formatToParts(date)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value]),
  ) as Record<string, string>;

  const weekdayMap: Record<string, number> = {
    Sun: 0,
    Mon: 1,
    Tue: 2,
    Wed: 3,
    Thu: 4,
    Fri: 5,
    Sat: 6,
  };

  return {
    year: Number(parts.year),
    date: `${parts.year}-${parts.month}-${parts.day}`,
    hour: Number(parts.hour),
    minute: Number(parts.minute),
    weekday: weekdayMap[parts.weekday] ?? -1,
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
  const repo = Deno.env.get("GITHUB_REPO_APP");
  const workflowId = Deno.env.get("GITHUB_WORKFLOW_ID") ?? "publish-market-data.yml";
  const ref = Deno.env.get("GITHUB_WORKFLOW_REF") ?? "main";

  if (!githubToken || !owner || !repo) {
    throw new Error("Missing GITHUB_TOKEN, GITHUB_OWNER, or GITHUB_REPO_APP.");
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
    const parts = toIstDateParts(new Date());
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