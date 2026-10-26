-- MANUAL migration draft: explicit operator approval is required before use.
-- Run with psql, without --single-transaction or an enclosing BEGIN. No task,
-- run, checkpoint, event or wait row is deleted or modified.
\set ON_ERROR_STOP on
set lock_timeout = '2s';
set statement_timeout = '10min';

create or replace function absurd.ensure_queue_tables (p_queue_name text)
  returns void
  language plpgsql
as $$
begin
  perform absurd.validate_queue_name(p_queue_name);

  execute format(
    'create table if not exists absurd.%I (
        task_id uuid primary key,
        task_name text not null,
        params jsonb not null,
        headers jsonb,
        retry_strategy jsonb,
        max_attempts integer,
        cancellation jsonb,
        enqueue_at timestamptz not null default absurd.current_time(),
        first_started_at timestamptz,
        state text not null check (state in (''pending'', ''running'', ''sleeping'', ''completed'', ''failed'', ''cancelled'')),
        attempts integer not null default 0,
        last_attempt_run uuid,
        completed_payload jsonb,
        cancelled_at timestamptz,
        idempotency_key text unique
     ) with (fillfactor=70)',
    't_' || p_queue_name
  );

  execute format(
    'create table if not exists absurd.%I (
        run_id uuid primary key,
        task_id uuid not null,
        attempt integer not null,
        state text not null check (state in (''pending'', ''running'', ''sleeping'', ''completed'', ''failed'', ''cancelled'')),
        claimed_by text,
        claim_expires_at timestamptz,
        available_at timestamptz not null,
        wake_event text,
        event_payload jsonb,
        started_at timestamptz,
        completed_at timestamptz,
        failed_at timestamptz,
        result jsonb,
        failure_reason jsonb,
        created_at timestamptz not null default absurd.current_time()
     ) with (fillfactor=70)',
    'r_' || p_queue_name
  );

  execute format(
    'create table if not exists absurd.%I (
        task_id uuid not null,
        checkpoint_name text not null,
        state jsonb,
        status text not null default ''committed'',
        owner_run_id uuid,
        updated_at timestamptz not null default absurd.current_time(),
        primary key (task_id, checkpoint_name)
     ) with (fillfactor=70)',
    'c_' || p_queue_name
  );

  execute format(
    'create table if not exists absurd.%I (
        event_name text primary key,
        payload jsonb,
        emitted_at timestamptz not null default absurd.current_time()
     )',
    'e_' || p_queue_name
  );

  execute format(
    'create table if not exists absurd.%I (
        task_id uuid not null,
        run_id uuid not null,
        step_name text not null,
        event_name text not null,
        timeout_at timestamptz,
        created_at timestamptz not null default absurd.current_time(),
        primary key (run_id, step_name)
     )',
    'w_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (state)',
    ('t_' || p_queue_name) || '_si',
    't_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (state, available_at)',
    ('r_' || p_queue_name) || '_sai',
    'r_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (task_id)',
    ('r_' || p_queue_name) || '_ti',
    'r_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (claim_expires_at)
      where state = ''running''
        and claim_expires_at is not null',
    ('r_' || p_queue_name) || '_cei',
    'r_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (event_name)',
    ('w_' || p_queue_name) || '_eni',
    'w_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (task_id)',
    ('w_' || p_queue_name) || '_ti',
    'w_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on absurd.%I (emitted_at)',
    ('e_' || p_queue_name) || '_eai',
    'e_' || p_queue_name
  );
end;
$$;

-- Refuse to let IF NOT EXISTS silently preserve an invalid or conflicting
-- index. An interrupted concurrent build requires separate reviewed repair;
-- this migration never drops an index automatically.
do $check$
declare q record; existing record;
begin
  for q in select queue_name from absurd.queues order by queue_name loop
    perform absurd.validate_queue_name(q.queue_name);
    select i.indisvalid, i.indisready, i.indisunique, i.indisexclusion, i.indnkeyatts, i.indnatts,
      i.indpred is null as unfiltered, i.indexprs is null as plain_columns,
      am.amname, a.attname, ns.nspname, tbl.relname
      into existing
      from pg_class idx
      join pg_namespace ins on ins.oid=idx.relnamespace
      left join pg_index i on i.indexrelid=idx.oid
      left join pg_class tbl on tbl.oid=i.indrelid
      left join pg_namespace ns on ns.oid=tbl.relnamespace
      left join pg_am am on am.oid=idx.relam
      left join pg_attribute a on a.attrelid=tbl.oid and a.attnum=i.indkey[0]
      where ins.nspname='absurd' and idx.relname='t_' || q.queue_name || '_si';
    if found and not coalesce((existing.indisvalid and existing.indisready
        and not existing.indisunique and not existing.indisexclusion
        and existing.indnkeyatts=1 and existing.indnatts=1
        and existing.unfiltered and existing.plain_columns
        and existing.amname='btree' and existing.attname='state'
        and existing.nspname='absurd' and existing.relname='t_' || q.queue_name), false) then
      raise exception 'Invalid or conflicting task-state index for queue %; reviewed repair required', q.queue_name;
    end if;
  end loop;
end;
$check$;

select format('create index concurrently if not exists %I on absurd.%I (state);',
              't_' || queue_name || '_si', 't_' || queue_name)
from absurd.queues order by queue_name
\gexec

-- Readback: every listed row must report valid/ready before completion.
select q.queue_name, i.indisvalid, i.indisready, pg_get_indexdef(i.indexrelid)
from absurd.queues q
left join pg_class idx on idx.relname='t_' || q.queue_name || '_si'
  and idx.relnamespace='absurd'::regnamespace
left join pg_index i on i.indexrelid=idx.oid
order by q.queue_name;
