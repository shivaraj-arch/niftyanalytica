# Project Guidelines

## Conventions

- For any GitHub write operation in this workspace, authenticate with `GITHUB_CLASSIC_PAT` by default.
- Do not rely on `gh` default auth, cached GitHub CLI credentials, or unauthenticated `origin` pushes for commits that need to be pushed remotely.
- If a normal push fails because of auth, retry immediately using `GITHUB_CLASSIC_PAT` instead of switching to another auth path.
- When Supabase URL or publishable key is required, use `SUPABASE_URL` and `SUPABASE_PUBLISHABLE_KEY` from the shell environment by default instead of asking for them again or hardcoding replacements.

## Workflow Contract

- Treat the market data workflow as fixed unless the user explicitly changes it.
- Supabase should only dispatch GitHub Actions windows for market refresh ownership, not write the recurring JSON updates itself.
- On trading days, Supabase dispatches a pages repo live window workflow at 09:00 IST and again at 12:00 IST.
- The 09:00 IST live window runs until 12:00 IST and updates the live snapshot every minute.
- The 12:00 IST live window runs until 15:30 IST and updates the live snapshot every minute.
- During both live windows, world markets tape and latest headlines refresh every 3 minutes and must remain on the same cadence.
- Outside the live windows, from 06:00 IST to 08:30 IST and from 15:30 IST to 00:00 IST on trading days, Supabase dispatches a pages repo news window workflow every 30 minutes.
- Each off-hours news window refreshes world markets tape and latest headlines every 3 minutes for the duration of that 30-minute window.
- On holidays, halt polling and refresh jobs and surface the next trading date in the UI.
- AI brief generation is independent of all other operations and runs only on trading days at 09:00 IST, 15:00 IST, and 20:00 IST.
- Newsletter generation and SMTP sending runs at 20:10 IST and must remain independent from the rest of the market data refresh flow.
- Never fall back to a previous date JSON when the current session should have produced a newer persisted file.