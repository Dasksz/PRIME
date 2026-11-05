DO $migration$
declare p record; q text; c text; ddl text;
begin
 for p in select * from pg_policies where schemaname='public' and tablename in ('data_detailed','data_history','data_clients','data_orders','data_stock','data_metadata','metas_upload_chunks') loop
  q:=p.qual; c:=p.with_check;
  if q is not null then q:=replace(replace(q,'is_admin()','(select public.is_admin())'),'is_approved()','(select public.is_approved())'); end if;
  if c is not null then c:=replace(replace(c,'is_admin()','(select public.is_admin())'),'is_approved()','(select public.is_approved())'); end if;
  ddl:=format('alter policy %I on public.%I',p.policyname,p.tablename);
  if q is not null then ddl:=ddl||' using ('||q||')'; end if;
  if c is not null then ddl:=ddl||' with check ('||c||')'; end if;
  execute ddl;
 end loop;
end $migration$;
alter function public.commit_metas_upload(uuid,jsonb) set statement_timeout='55s'; notify pgrst, 'reload schema';
