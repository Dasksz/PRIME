create table public.metas_upload_chunks (
 owner_id uuid not null default auth.uid(), upload_id uuid not null, table_name text not null,
 chunk_no integer not null check(chunk_no>=0), rows_data jsonb not null check(jsonb_typeof(rows_data)='array'),
 created_at timestamptz not null default now(), primary key(owner_id,upload_id,table_name,chunk_no),
 check(table_name in ('data_detailed','data_history','data_clients','data_orders','data_stock','data_metadata'))
);
alter table public.metas_upload_chunks enable row level security;
grant select,insert,update,delete on public.metas_upload_chunks to authenticated;
create policy metas_upload_owner on public.metas_upload_chunks to authenticated using(owner_id=auth.uid() and public.is_admin()) with check(owner_id=auth.uid() and public.is_admin());
create function public.commit_metas_upload(p_upload_id uuid,p_counts jsonb) returns void language plpgsql security invoker set search_path='' as $$
declare t text; n bigint; cols text;
begin
 if auth.uid() is null or not public.is_admin() then raise exception 'Apenas administradores'; end if;
 perform pg_advisory_xact_lock(721905);
 for t in select jsonb_object_keys(p_counts) loop
  if t not in ('data_detailed','data_history','data_clients','data_orders','data_stock','data_metadata') then raise exception 'Tabela inválida'; end if;
  select coalesce(sum(jsonb_array_length(rows_data)),0) into n from public.metas_upload_chunks where owner_id=auth.uid() and upload_id=p_upload_id and table_name=t;
  if n<>(p_counts->>t)::bigint or n=0 then raise exception 'Envio incompleto: %',t; end if;
 end loop;
 if not (p_counts ?& array['data_detailed','data_history','data_clients','data_metadata']) then raise exception 'Arquivos obrigatórios ausentes'; end if;
 for t in select jsonb_object_keys(p_counts) loop
  select string_agg(format('%I',column_name),',' order by ordinal_position) into cols from information_schema.columns where table_schema='public' and table_name=t and column_name<>'id';
  if t='data_metadata' then
   delete from public.data_metadata where key<>'groq_api_key';
  else execute format('delete from public.%I',t);
  end if;
  execute format('insert into public.%I (%s) select %s from public.metas_upload_chunks c cross join lateral jsonb_populate_recordset(null::public.%I,c.rows_data) r where c.owner_id=auth.uid() and c.upload_id=$1 and c.table_name=$2',t,cols,cols,t) using p_upload_id,t;
 end loop;
 delete from public.metas_upload_chunks where owner_id=auth.uid() and (upload_id=p_upload_id or created_at<now()-interval '1 day');
end $$;
revoke all on function public.commit_metas_upload(uuid,jsonb) from public,anon;
grant execute on function public.commit_metas_upload(uuid,jsonb) to authenticated;