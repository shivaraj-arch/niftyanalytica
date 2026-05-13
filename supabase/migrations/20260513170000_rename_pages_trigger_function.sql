create extension if not exists pg_cron;
create extension if not exists pg_net;

do $$
declare
  scheduled_job record;
  function_url constant text := 'https://gthnjueqoufdtwtzjcxg.supabase.co/functions/v1/trigger-pages-refresh';
  request_headers constant jsonb := '{"Content-Type":"application/json"}'::jsonb;
begin
  for scheduled_job in
    select jobid
    from cron.job
    where jobname in (
      'trigger-pages-live-window-morning',
      'trigger-pages-live-window-afternoon',
      'trigger-pages-news-window-pre-market',
      'trigger-pages-news-window-post-market-open',
      'trigger-pages-news-window-post-market',
      'trigger-pages-news-window-midnight',
      'trigger-pages-news-window-pre-market-utc-30',
      'trigger-pages-news-window-pre-market-utc-00',
      'trigger-pages-news-window-post-market-utc'
    )
  loop
    perform cron.unschedule(scheduled_job.jobid);
  end loop;

  perform cron.schedule(
    'trigger-pages-live-window-morning',
    '30 3 * * 1-5',
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
    '30 6 * * 1-5',
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
    'trigger-pages-news-window-pre-market-utc-30',
    '30 0-2 * * 1-5',
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
    'trigger-pages-news-window-pre-market-utc-00',
    '0 1-3 * * 1-5',
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
    'trigger-pages-news-window-post-market-utc',
    '0,30 10-18 * * 1-5',
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