do $$
begin
  if exists (
    select 1
    from cron.job
    where jobname = 'refresh-news-feed-hourly-ist'
  ) then
    perform cron.unschedule('refresh-news-feed-hourly-ist');
  end if;
end $$;

select cron.schedule(
  'refresh-news-feed-hourly-ist',
  '0 0,6-23 * * *',
  $$
  select net.http_post(
    url := 'https://gthnjueqoufdtwtzjcxg.supabase.co/functions/v1/refresh-news-feed',
    headers := '{"Content-Type": "application/json"}'::jsonb,
    body := '{}'::jsonb
  );
  $$
);