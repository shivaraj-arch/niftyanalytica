import { serve } from "https://deno.land/std@0.224.0/http/server.ts";

type HolidayConfig = {
  year: number;
  dates: string[];
};

type DispatchRequestBody = {
  target?: "pages-live-morning" | "pages-live-afternoon" | "pages-news-window";
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

function getPagesRepo() {
  return Deno.env.get("GITHUB_REPO_PAGES") ?? "niftyanalytica";
}

function getPagesRef() {
  return Deno.env.get("GITHUB_WORKFLOW_REF_PAGES")
    ?? Deno.env.get("GITHUB_WORKFLOW_REF")
    ?? "main";
}

function buildDispatchRequest(body: DispatchRequestBody) {
  const target = body.target;

  if (!target) {
    throw new Error("Missing dispatch target.");
  }

  if (target === "pages-live-morning") {
    return {
      target,
      repo: getPagesRepo(),
      workflowId: Deno.env.get("GITHUB_PAGES_LIVE_MORNING_WORKFLOW_ID") ?? "refresh-live-market-window-morning.yml",
      ref: getPagesRef(),
      inputs: undefined,
    };
  }

  if (target === "pages-live-afternoon") {
    return {
      target,
      repo: getPagesRepo(),
      workflowId: Deno.env.get("GITHUB_PAGES_LIVE_AFTERNOON_WORKFLOW_ID") ?? "refresh-live-market-window-afternoon.yml",
      ref: getPagesRef(),
      inputs: undefined,
    };
  }

  if (target === "pages-news-window") {
    return {
      target,
      repo: getPagesRepo(),
      workflowId: Deno.env.get("GITHUB_PAGES_NEWS_WINDOW_WORKFLOW_ID") ?? "refresh-news-rails-window.yml",
      ref: getPagesRef(),
      inputs: undefined,
    };
  }

  throw new Error(`Unsupported dispatch target: ${target}`);
}

async function dispatchGithubWorkflow(triggerDate: string, triggerHour: number, body: DispatchRequestBody) {
  const githubToken = Deno.env.get("GITHUB_TOKEN");
  const owner = Deno.env.get("GITHUB_OWNER");
  const dispatchRequest = buildDispatchRequest(body);
  const { target, repo, workflowId, ref, inputs } = dispatchRequest;

  if (!githubToken || !owner || !repo) {
    throw new Error("Missing GITHUB_TOKEN, GITHUB_OWNER, or target repository configuration.");
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
        ...(inputs ? { inputs } : {}),
      }),
    },
  );

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`GitHub dispatch failed: ${response.status} ${text}`);
  }

  return {
    ok: true,
    target,
    triggerDate,
    triggerHour,
    repo,
    workflowId,
    ref,
  };
}

serve(async (request) => {
  try {
    const parts = toIstDateParts(new Date());
    const holidays = loadHolidaySet();
    const body = request.method === "POST"
      ? await request.json().catch(() => ({})) as DispatchRequestBody
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

    const result = await dispatchGithubWorkflow(parts.date, parts.hour, body);
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