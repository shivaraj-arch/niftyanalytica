# Project Guidelines

## Conventions

- For any GitHub write operation in this workspace, authenticate with `GITHUB_CLASSIC_PAT` by default.
- Do not rely on `gh` default auth, cached GitHub CLI credentials, or unauthenticated `origin` pushes for commits that need to be pushed remotely.
- If a normal push fails because of auth, retry immediately using `GITHUB_CLASSIC_PAT` instead of switching to another auth path.

## Workflow Contract

- Treat the market data workflow as fixed unless the user explicitly changes it.
- On trading days, Supabase polling runs from 09:00 IST to 16:00 IST every minute for live market data.
- On trading days, world markets tape and latest headlines refresh every 3 minutes during trading hours and must write to JSON independently of AI brief generation.
- Treat 15:30 IST to 16:00 IST as post-market session; when market state is needed, use the `metadata` section from `https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050`.
- Outside trading hours, from 06:00 IST until 00:00 IST, Supabase should trigger hourly refresh of world markets tape and latest headlines into JSON.
- On holidays, halt polling and refresh jobs and surface the next trading date in the UI.
- AI brief generation is independent of all other operations and runs only on trading days at 09:00 IST, 15:00 IST, and 20:00 IST.
- Newsletter generation and SMTP sending runs at 20:10 IST and must remain independent from the rest of the market data refresh flow.
- Never fall back to a previous date JSON when the current session should have produced a newer persisted file.