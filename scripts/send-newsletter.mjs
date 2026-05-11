import fs from "node:fs/promises";
import path from "node:path";

const INPUT_FILE = path.resolve(process.env.NEWSLETTER_INPUT_FILE || "tmp/newsletter/latest.json");
const SUBSCRIBERS_FILE = path.resolve(process.env.NEWSLETTER_SUBSCRIBERS_FILE || "data/newsletter-subscribers.json");
const DELIVERY_FILE = path.resolve(process.env.NEWSLETTER_DELIVERY_FILE || "tmp/newsletter/delivery.json");
const PROVIDER = String(process.env.NEWSLETTER_PROVIDER || "resend").trim().toLowerCase();
const FROM_EMAIL = String(process.env.NEWSLETTER_FROM_EMAIL || "").trim();
const REPLY_TO_EMAIL = String(process.env.NEWSLETTER_REPLY_TO_EMAIL || "").trim();
const DRY_RUN = String(process.env.NEWSLETTER_DRY_RUN || "false").trim().toLowerCase() === "true";

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function personalizeTemplate(template, email) {
  const encodedEmail = encodeURIComponent(email);
  return String(template || "")
    .replaceAll("{{EMAIL_ENCODED}}", encodedEmail)
    .replaceAll("{{EMAIL}}", email);
}

function resolveSubscribers(payload) {
  const rows = Array.isArray(payload?.subscribers) ? payload.subscribers : [];
  const seen = new Set();

  return rows
    .filter((row) => row && row.active !== false)
    .map((row) => normalizeEmail(row.email))
    .filter((email) => {
      if (!isValidEmail(email) || seen.has(email)) {
        return false;
      }
      seen.add(email);
      return true;
    });
}

async function sendViaResend(payload, email) {
  const apiKey = String(process.env.RESEND_API_KEY || "").trim();
  if (!apiKey) {
    throw new Error("Missing RESEND_API_KEY for newsletter delivery.");
  }

  if (!FROM_EMAIL) {
    throw new Error("Missing NEWSLETTER_FROM_EMAIL for newsletter delivery.");
  }

  const unsubscribeUrl = personalizeTemplate("https://www.niftyanalytica.com/unsubscribe/?email={{EMAIL_ENCODED}}", email);
  const body = {
    from: FROM_EMAIL,
    to: [email],
    subject: payload.subject,
    html: personalizeTemplate(payload.html, email),
    text: personalizeTemplate(payload.text, email),
    headers: {
      "List-Unsubscribe": `<${unsubscribeUrl}>`,
      "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
    },
  };

  if (REPLY_TO_EMAIL) {
    body.reply_to = REPLY_TO_EMAIL;
  }

  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const responsePayload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(responsePayload?.message || responsePayload?.error || `Resend request failed with ${response.status}`);
  }

  return {
    provider: "resend",
    id: responsePayload?.id || null,
  };
}

async function sendNewsletter(payload, email) {
  if (PROVIDER !== "resend") {
    throw new Error(`Unsupported newsletter provider: ${PROVIDER}`);
  }

  return await sendViaResend(payload, email);
}

async function main() {
  const [payload, subscribersPayload] = await Promise.all([
    readJson(INPUT_FILE),
    readJson(SUBSCRIBERS_FILE).catch(() => ({ subscribers: [] })),
  ]);

  const subscribers = resolveSubscribers(subscribersPayload);
  const report = {
    generatedAt: new Date().toISOString(),
    provider: PROVIDER,
    dryRun: DRY_RUN,
    subject: payload.subject,
    totalSubscribers: subscribers.length,
    sent: [],
    failed: [],
    skipped: [],
  };

  if (!subscribers.length) {
    report.skipped.push({ reason: "No active subscribers found." });
  } else if (DRY_RUN) {
    report.skipped.push({
      reason: "Dry run enabled. No emails were sent.",
      sampleRecipients: subscribers.slice(0, 3),
    });
  } else {
    for (const email of subscribers) {
      try {
        const result = await sendNewsletter(payload, email);
        report.sent.push({ email, ...result });
      } catch (error) {
        report.failed.push({
          email,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }
  }

  await fs.mkdir(path.dirname(DELIVERY_FILE), { recursive: true });
  await fs.writeFile(DELIVERY_FILE, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  if (report.failed.length && !DRY_RUN) {
    throw new Error(`Newsletter delivery failed for ${report.failed.length} subscribers.`);
  }

  console.log(`Newsletter delivery report written to ${path.relative(process.cwd(), DELIVERY_FILE)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});