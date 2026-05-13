create extension if not exists pg_cron;
create extension if not exists pg_net;

do $$
declare
  scheduled_job record;
  function_url constant text := 'https://gthnjueqoufdtwtzjcxg.supabase.co/functions/v1/trigger-market-publish';
  request_headers constant jsonb := '{"Content-Type":"application/json"}'::jsonb;
begin
  for scheduled_job in
    select jobid
    from cron.job
    where jobname in (
      'refresh-live-snapshot-market-open-half-hour',
      'refresh-live-snapshot-market-hours',
      'refresh-live-snapshot-market-close',
      'refresh-live-snapshot-market-close-window',
      'refresh-news-feed-hourly-ist',
      'trigger-pages-live-window-morning',
      'trigger-pages-live-window-afternoon',
      'trigger-pages-news-window-pre-market',
      'trigger-pages-news-window-post-market-open',
      'trigger-pages-news-window-post-market',
      'trigger-pages-news-window-midnight'
    )
  loop
    perform cron.unschedule(scheduled_job.jobid);
  end loop;

  perform cron.schedule(
    'trigger-pages-live-window-morning',
    '0 9 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := '{"target":"pages-live-morning"}'::jsonb
      );
      $sql$,
      function_url,
      request_headers::text
    )
  );

  perform cron.schedule(
    'trigger-pages-live-window-afternoon',
    '0 12 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := '{"target":"pages-live-afternoon"}'::jsonb
      );
      $sql$,
      function_url,
      request_headers::text
    )
  );

  perform cron.schedule(
    'trigger-pages-news-window-pre-market',
    '0,30 6-8 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := '{"target":"pages-news-window"}'::jsonb
      );
      $sql$,
      function_url,
      request_headers::text
    )
  );

  perform cron.schedule(
    'trigger-pages-news-window-post-market-open',
    '30 15 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := '{"target":"pages-news-window"}'::jsonb
      );
      $sql$,
      function_url,
      request_headers::text
    )
  );

  perform cron.schedule(
    'trigger-pages-news-window-post-market',
    '0,30 16-23 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := '{"target":"pages-news-window"}'::jsonb
      );
      $sql$,
      function_url,
      request_headers::text
    )
  );

  perform cron.schedule(
    'trigger-pages-news-window-midnight',
    '0 0 * * 2-6',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := '{"target":"pages-news-window"}'::jsonb
      );
      $sql$,
      function_url,
      request_headers::text
    )
  );
end;
$$;