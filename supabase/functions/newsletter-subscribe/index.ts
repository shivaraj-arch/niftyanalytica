import { serve } from "https://deno.land/std@0.224.0/http/server.ts";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Content-Type": "application/json",
};

type SubscriberRecord = {
  email: string;
  subscribedAt: string;
  unsubscribedAt?: string | null;
  active: boolean;
  source: string;
};

type SubscriberPayload = {
  subscribers: SubscriberRecord[];
  updatedAt: string | null;
};

function normalizeEmail(value: string) {
  return String(value || "").trim().toLowerCase();
}

function isValidEmail(value: string) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

function toBase64(value: string) {
  return btoa(unescape(encodeURIComponent(value)));
}

function fromBase64(value: string) {
  return decodeURIComponent(escape(atob(value)));
}

function normalizeAction(value: string) {
  const action = String(value || "subscribe").trim().toLowerCase();
  return action === "unsubscribe" ? "unsubscribe" : "subscribe";
}

async function getGithubFile(owner: string, repo: string, path: string, branch: string, token: string) {
  const url = `https://api.github.com/repos/${owner}/${repo}/contents/${path}?ref=${branch}`;
  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "User-Agent": "supabase-edge-function",
    },
  });

  if (response.status === 404) {
    return null;
  }

  if (!response.ok) {
    throw new Error(`GitHub file read failed: ${response.status} ${await response.text()}`);
  }

  return await response.json();
}

async function putGithubFile(
  owner: string,
  repo: string,
  path: string,
  branch: string,
  token: string,
  content: string,
  sha?: string,
) {
  const response = await fetch(`https://api.github.com/repos/${owner}/${repo}/contents/${path}`, {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "Content-Type": "application/json",
      "User-Agent": "supabase-edge-function",
    },
    body: JSON.stringify({
      message: `Update newsletter subscribers (${new Date().toISOString()})`,
      content: toBase64(content),
      branch,
      ...(sha ? { sha } : {}),
    }),
  });

  if (!response.ok) {
    throw new Error(`GitHub file write failed: ${response.status} ${await response.text()}`);
  }
}

serve(async (request) => {
  if (request.method === "OPTIONS") {
    return new Response(null, { headers: CORS_HEADERS });
  }

  try {
    const bodyText = await request.text();
    const body = bodyText ? JSON.parse(bodyText) as { email?: string; action?: string; source?: string } : {};
    const email = normalizeEmail(body.email || "");
    const action = normalizeAction(body.action || "subscribe");
    const source = String(body.source || "website").trim() || "website";

    if (!isValidEmail(email)) {
      return new Response(JSON.stringify({ ok: false, error: "Enter a valid email address." }), {
        status: 400,
        headers: CORS_HEADERS,
      });
    }

    const token = Deno.env.get("GITHUB_TOKEN");
    const owner = Deno.env.get("GITHUB_OWNER");
    const repo = Deno.env.get("GITHUB_REPO_APP") ?? Deno.env.get("GITHUB_REPO");
    const branch = Deno.env.get("GITHUB_WORKFLOW_REF") ?? Deno.env.get("GITHUB_BRANCH") ?? "main";
    const subscribersPath = Deno.env.get("NEWSLETTER_SUBSCRIBERS_PATH") ?? "data/newsletter-subscribers.json";

    if (!token || !owner || !repo) {
      throw new Error("Missing GITHUB_TOKEN, GITHUB_OWNER, or GITHUB_REPO_APP/GITHUB_REPO.");
    }

    const existingFile = await getGithubFile(owner, repo, subscribersPath, branch, token);
    let payload: SubscriberPayload = { subscribers: [], updatedAt: null };

    if (existingFile?.content) {
      payload = JSON.parse(fromBase64(String(existingFile.content).replace(/\n/g, ""))) as SubscriberPayload;
    }

    const subscribers = Array.isArray(payload.subscribers) ? payload.subscribers : [];
    const existingSubscriber = subscribers.find((item) => normalizeEmail(item.email) === email);
    const nowIso = new Date().toISOString();

    if (action === "unsubscribe") {
      if (existingSubscriber) {
        payload.subscribers = subscribers.filter((item) => normalizeEmail(item.email) !== email);
      }
    } else {
      if (existingSubscriber) {
        existingSubscriber.active = true;
        existingSubscriber.subscribedAt = existingSubscriber.subscribedAt || nowIso;
        existingSubscriber.unsubscribedAt = null;
        existingSubscriber.source = existingSubscriber.source || source;
      } else {
        subscribers.push({
          email,
          subscribedAt: nowIso,
          unsubscribedAt: null,
          active: true,
          source,
        });
      }
    }

    payload = {
      subscribers: action === "unsubscribe"
        ? (payload.subscribers || [])
        : subscribers,
      updatedAt: nowIso,
    };

    await putGithubFile(
      owner,
      repo,
      subscribersPath,
      branch,
      token,
      JSON.stringify(payload, null, 2) + "\n",
      existingFile?.sha,
    );

    return new Response(JSON.stringify({
      ok: true,
      action,
      message: action === "unsubscribe"
        ? (existingSubscriber
          ? "You have been unsubscribed from the 8 PM AI Brief."
          : "This email was not subscribed, so no change was needed.")
        : (existingSubscriber && existingSubscriber.active
          ? "This email is already subscribed to the 8 PM AI Brief."
          : "Subscription saved. You will receive the 8 PM AI Brief newsletter on trading days."),
    }), {
      status: 200,
      headers: CORS_HEADERS,
    });
  } catch (error) {
    return new Response(JSON.stringify({
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    }), {
      status: 500,
      headers: CORS_HEADERS,
    });
  }
});