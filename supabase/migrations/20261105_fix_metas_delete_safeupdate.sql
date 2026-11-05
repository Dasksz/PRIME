-- Keep safeupdate enabled; target only rows with a non-null primary key.
DO $migration$
declare definition text;
begin
 select pg_get_functiondef('public.commit_metas_upload(uuid,jsonb)'::regprocedure) into definition;
 definition := replace(definition, 'delete from public.%I'',t)', 'delete from public.%I where id is not null'',t)');
 execute definition;
end $migration$;
