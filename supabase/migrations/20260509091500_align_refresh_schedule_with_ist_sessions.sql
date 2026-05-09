do $$
declare
  scheduled_job record;
  function_url constant text := 'https://gthnjueqoufdtwtzjcxg.supabase.co/functions/v1/refresh-live-snapshot';
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
      'refresh-live-snapshot-market-close-window'
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
      function_url,
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
      function_url,
      request_headers::text,
      request_body::text
    )
  );

  perform cron.schedule(
    'refresh-live-snapshot-market-close-window',
    '0-5 10 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := %L::jsonb
      );
      $sql$,
      function_url,
      request_headers::text,
      request_body::text
    )
  );
end;
$$;