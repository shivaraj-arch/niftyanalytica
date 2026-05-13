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
      'trigger-market-publish-0900-ist',
      'trigger-market-publish-1500-ist',
      'trigger-market-publish-2000-ist'
    )
  loop
    perform cron.unschedule(scheduled_job.jobid);
  end loop;

  perform cron.schedule(
    'trigger-market-publish-0900-ist',
    '30 3 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := '{}'::jsonb
      );
      $sql$,
      function_url,
      request_headers::text
    )
  );

  perform cron.schedule(
    'trigger-market-publish-1500-ist',
    '30 9 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := '{}'::jsonb
      );
      $sql$,
      function_url,
      request_headers::text
    )
  );

  perform cron.schedule(
    'trigger-market-publish-2000-ist',
    '30 14 * * 1-5',
    format(
      $sql$
      select net.http_post(
        url := %L,
        headers := %L::jsonb,
        body := '{}'::jsonb
      );
      $sql$,
      function_url,
      request_headers::text
    )
  );
end;
$$;