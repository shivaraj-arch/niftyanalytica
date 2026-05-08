create extension if not exists pg_cron with schema extensions;
create extension if not exists pg_net with schema extensions;

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
      'refresh-live-snapshot-market-hours',
      'refresh-live-snapshot-market-close'
    )
  loop
    perform cron.unschedule(scheduled_job.jobid);
  end loop;

  perform cron.schedule(
    'refresh-live-snapshot-market-hours',
    '* 9-15 * * 1-5',
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
    'refresh-live-snapshot-market-close',
    '0 16 * * 1-5',
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