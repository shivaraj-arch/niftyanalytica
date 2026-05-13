create extension if not exists pg_cron;
create extension if not exists pg_net;

do $$
declare
  scheduled_job record;
  live_snapshot_function_url constant text := 'https://gthnjueqoufdtwtzjcxg.supabase.co/functions/v1/refresh-live-snapshot';
  news_feed_function_url constant text := 'https://gthnjueqoufdtwtzjcxg.supabase.co/functions/v1/refresh-news-feed';
  request_headers constant jsonb := '{"Content-Type":"application/json"}'::jsonb;
  request_body constant jsonb := '{}'::jsonb;
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
      'refresh-news-feed-market-pre-open',
      'refresh-news-feed-market-hours',
      'refresh-news-feed-market-close-window',
      'refresh-news-feed-post-market-open',
      'refresh-news-feed-post-market-evening',
      'refresh-news-feed-post-market-close',
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
    'refresh-live-snapshot-market-open-half-hour',
    '30-59 3 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := %L::jsonb
      );
      $sql$,
      live_snapshot_function_url,
      request_headers::text,
      request_body::text
    )
  );

  perform cron.schedule(
    'refresh-live-snapshot-market-hours',
    '* 4-9 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := %L::jsonb
      );
      $sql$,
      live_snapshot_function_url,
      request_headers::text,
      request_body::text
    )
  );

  perform cron.schedule(
    'refresh-live-snapshot-market-close-window',
    '0-30 10 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := %L::jsonb
      );
      $sql$,
      live_snapshot_function_url,
      request_headers::text,
      request_body::text
    )
  );

  perform cron.schedule(
    'refresh-news-feed-market-pre-open',
    '30-59/3 0 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := %L::jsonb
      );
      $sql$,
      news_feed_function_url,
      request_headers::text,
      request_body::text
    )
  );

  perform cron.schedule(
    'refresh-news-feed-market-hours',
    '0-59/3 1-9 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := %L::jsonb
      );
      $sql$,
      news_feed_function_url,
      request_headers::text,
      request_body::text
    )
  );

  perform cron.schedule(
    'refresh-news-feed-market-close-window',
    '0-27/3 10 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := %L::jsonb
      );
      $sql$,
      news_feed_function_url,
      request_headers::text,
      request_body::text
    )
  );

  perform cron.schedule(
    'refresh-news-feed-post-market-open',
    '30 10 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := %L::jsonb
      );
      $sql$,
      news_feed_function_url,
      request_headers::text,
      request_body::text
    )
  );

  perform cron.schedule(
    'refresh-news-feed-post-market-evening',
    '0,30 11-17 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := %L::jsonb
      );
      $sql$,
      news_feed_function_url,
      request_headers::text,
      request_body::text
    )
  );

  perform cron.schedule(
    'refresh-news-feed-post-market-close',
    '0 18 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := %L::jsonb
      );
      $sql$,
      news_feed_function_url,
      request_headers::text,
      request_body::text
    )
  );
end;
$$;