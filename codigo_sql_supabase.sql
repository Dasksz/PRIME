-- ==============================================================================
-- 1. LIMPEZA PROFUNDA (Deep Clean)
-- ==============================================================================
-- Remover TODAS as tabelas "fantasmas" relatadas nos logs (CSV 11, 12, 14 e 16)
drop table if exists public.data_active_products CASCADE;

drop table if exists public.data_clients CASCADE;

drop table if exists public.goals_distribution CASCADE;

drop table if exists public.profiles CASCADE;

drop table if exists public.data_detailed CASCADE;

drop table if exists public.data_history CASCADE;

drop table if exists public.data_product_details CASCADE;

drop table if exists public.data_stock CASCADE;

drop table if exists public.data_innovations CASCADE;

-- Nova do CSV 16
drop table if exists public.data_metadata CASCADE;

-- Nova do CSV 16
drop table if exists public.data_orders CASCADE;

-- Nova do CSV 16
-- Remover tabelas do sistema atual para recriar limpo
drop table if exists public.sales_items CASCADE;

drop table if exists public.sales CASCADE;

drop table if exists public.products CASCADE;

drop table if exists public.user_profiles CASCADE;

-- Remover funções antigas (CSV 13 e 15)
drop function IF exists public.handle_new_user () CASCADE;

drop function IF exists public.check_admin_permission () CASCADE;

drop function IF exists public.update_product_stock (UUID, INTEGER) CASCADE;

drop function IF exists public.register_sale (TEXT, TEXT, JSONB) CASCADE;

drop function IF exists public.get_initial_dashboard_data () CASCADE;

drop function IF exists public.get_city_performance_data () CASCADE;

-- Nova do CSV 15
-- Removemos variações com e sem argumentos
drop function IF exists public.get_comparison_data (TEXT) CASCADE;

drop function IF exists public.get_comparison_data () CASCADE;

drop function IF exists public.get_stock_view_data () CASCADE;

drop function IF exists public.get_sales_view_data (TIMESTAMP, TIMESTAMP) CASCADE;

drop function IF exists public.get_sales_view_data () CASCADE;

-- ==============================================================================
-- 2. FUNÇÕES BASE (Trigger e Segurança)
-- ==============================================================================
-- 2.1 Trigger de Criação de Perfil
create or replace function public.handle_new_user () RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
BEGIN
  INSERT INTO public.user_profiles (id, email, full_name, role)
  VALUES (
    new.id, 
    new.email, 
    new.raw_user_meta_data->>'full_name',
    COALESCE(new.raw_user_meta_data->>'role', 'employee')
  );
  RETURN new;
END;
$$;

drop trigger IF exists on_auth_user_created on auth.users;

create trigger on_auth_user_created
after INSERT on auth.users for EACH row
execute FUNCTION public.handle_new_user ();

-- 2.2 Tabelas Principais
create table public.user_profiles (
  id UUID primary key references auth.users (id) on delete CASCADE,
  email TEXT,
  role TEXT default 'employee' check (role in ('admin', 'employee')),
  full_name TEXT,
  created_at timestamp with time zone default timezone ('utc'::text, now()) not null,
  updated_at timestamp with time zone default timezone ('utc'::text, now()) not null
);

-- 2.3 Função Helper de Permissão
create or replace function public.check_admin_permission () RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
BEGIN
  RETURN EXISTS (
    SELECT 1 FROM public.user_profiles
    WHERE id = (select auth.uid()) AND role = 'admin'
  );
END;
$$;

-- 2.4 Restante das Tabelas
create table public.products (
  id UUID default gen_random_uuid () primary key,
  name TEXT not null,
  description TEXT,
  price DECIMAL(10, 2) not null check (price >= 0),
  stock_quantity INTEGER not null default 0 check (stock_quantity >= 0),
  min_stock_level INTEGER default 5,
  category TEXT,
  sku TEXT unique,
  image_url TEXT,
  created_at timestamp with time zone default timezone ('utc'::text, now()) not null,
  updated_at timestamp with time zone default timezone ('utc'::text, now()) not null,
  created_by UUID references public.user_profiles (id)
);

create table public.sales (
  id UUID default gen_random_uuid () primary key,
  user_id UUID references public.user_profiles (id) not null,
  total_amount DECIMAL(10, 2) not null default 0,
  payment_method TEXT check (
    payment_method in (
      'credit_card',
      'debit_card',
      'cash',
      'pix',
      'transfer'
    )
  ),
  status TEXT default 'completed' check (
    status in ('completed', 'pending', 'cancelled', 'refunded')
  ),
  notes TEXT,
  created_at timestamp with time zone default timezone ('utc'::text, now()) not null
);

create table public.sales_items (
  id UUID default gen_random_uuid () primary key,
  sale_id UUID references public.sales (id) on delete CASCADE not null,
  product_id UUID references public.products (id) not null,
  quantity INTEGER not null check (quantity > 0),
  unit_price DECIMAL(10, 2) not null,
  subtotal DECIMAL(10, 2) not null,
  created_at timestamp with time zone default timezone ('utc'::text, now()) not null
);

-- ==============================================================================
-- 3. POLÍTICAS RLS (Sem Sobreposição)
-- ==============================================================================
alter table public.user_profiles ENABLE row LEVEL SECURITY;

alter table public.products ENABLE row LEVEL SECURITY;

alter table public.sales ENABLE row LEVEL SECURITY;

alter table public.sales_items ENABLE row LEVEL SECURITY;

-- 3.1 user_profiles
create policy "Unified view access for user_profiles" on public.user_profiles for
select
  using (
    (
      select
        auth.uid ()
    ) = id
    or public.check_admin_permission () = true
  );

create policy "Admin update access for user_profiles" on public.user_profiles
for update
  using (public.check_admin_permission () = true);

-- 3.2 products
create policy "View products (All authenticated)" on public.products for
select
  to authenticated using (true);

create policy "Insert products (Admin only)" on public.products for INSERT to authenticated
with
  check (public.check_admin_permission () = true);

create policy "Update products (Admin only)" on public.products
for update
  to authenticated using (public.check_admin_permission () = true);

create policy "Delete products (Admin only)" on public.products for DELETE to authenticated using (public.check_admin_permission () = true);

-- 3.3 sales
create policy "View sales (Owner or Admin)" on public.sales for
select
  to authenticated using (
    (
      select
        auth.uid ()
    ) = user_id
    or public.check_admin_permission () = true
  );

create policy "Create sales (Authenticated)" on public.sales for INSERT to authenticated
with
  check (
    (
      select
        auth.uid ()
    ) = user_id
  );

-- 3.4 sales_items
create policy "View sales items (Owner or Admin)" on public.sales_items for
select
  to authenticated using (
    exists (
      select
        1
      from
        public.sales
      where
        sales.id = sales_items.sale_id
        and (
          sales.user_id = (
            select
              auth.uid ()
          )
          or public.check_admin_permission () = true
        )
    )
  );

create policy "Create sales items (Authenticated)" on public.sales_items for INSERT to authenticated
with
  check (
    exists (
      select
        1
      from
        public.sales
      where
        sales.id = sales_items.sale_id
        and sales.user_id = (
          select
            auth.uid ()
        )
    )
  );

-- ==============================================================================
-- 4. FUNÇÕES DE NEGÓCIO (Com search_path corrigido)
-- ==============================================================================
create or replace function public.update_product_stock (p_product_id UUID, p_quantity INTEGER) RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
BEGIN
    UPDATE public.products
    SET stock_quantity = stock_quantity - p_quantity,
        updated_at = now()
    WHERE id = p_product_id;
    
    IF (SELECT stock_quantity FROM public.products WHERE id = p_product_id) < 0 THEN
        RAISE EXCEPTION 'Stock insuficiente para o produto %', p_product_id;
    END IF;
END;
$$;

create or replace function public.register_sale (
  p_payment_method TEXT,
  p_notes TEXT,
  p_items JSONB
) RETURNS UUID LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
DECLARE
    v_sale_id UUID;
    v_total_amount DECIMAL(10,2) := 0;
    v_item JSONB;
    v_subtotal DECIMAL(10,2);
    v_current_user UUID;
BEGIN
    v_current_user := (select auth.uid());

    INSERT INTO public.sales (user_id, payment_method, notes, status)
    VALUES (v_current_user, p_payment_method, p_notes, 'completed')
    RETURNING id INTO v_sale_id;

    FOR v_item IN SELECT * FROM jsonb_array_elements(p_items)
    LOOP
        v_subtotal := (v_item->>'quantity')::INTEGER * (v_item->>'unit_price')::DECIMAL;
        v_total_amount := v_total_amount + v_subtotal;

        INSERT INTO public.sales_items (sale_id, product_id, quantity, unit_price, subtotal)
        VALUES (
            v_sale_id,
            (v_item->>'product_id')::UUID,
            (v_item->>'quantity')::INTEGER,
            (v_item->>'unit_price')::DECIMAL,
            v_subtotal
        );

        PERFORM public.update_product_stock(
            (v_item->>'product_id')::UUID,
            (v_item->>'quantity')::INTEGER
        );
    END LOOP;

    UPDATE public.sales 
    SET total_amount = v_total_amount 
    WHERE id = v_sale_id;

    RETURN v_sale_id;
END;
$$;

-- Funções de Leitura (Views)
create or replace function public.get_initial_dashboard_data () RETURNS json LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
DECLARE
    result json;
BEGIN
    SELECT json_build_object(
        'total_sales_today', (SELECT COALESCE(SUM(total_amount), 0) FROM sales WHERE created_at >= CURRENT_DATE),
        'total_orders_today', (SELECT COUNT(*) FROM sales WHERE created_at >= CURRENT_DATE),
        'low_stock_count', (SELECT COUNT(*) FROM products WHERE stock_quantity <= min_stock_level),
        'recent_sales', (
            SELECT json_agg(t) FROM (
                SELECT s.id, s.total_amount, s.created_at, up.full_name as seller
                FROM sales s
                JOIN user_profiles up ON s.user_id = up.id
                ORDER BY s.created_at DESC LIMIT 5
            ) t
        )
    ) INTO result;
    RETURN result;
END;
$$;

create or replace function public.get_comparison_data (period text default '7_days') RETURNS json LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
DECLARE
    start_date timestamp;
    result json;
BEGIN
    IF period = '30_days' THEN start_date := NOW() - INTERVAL '30 days';
    ELSE start_date := NOW() - INTERVAL '7 days'; END IF;

    SELECT json_agg(t) INTO result FROM (
        SELECT DATE(created_at) as date, COUNT(*) as sales_count, SUM(total_amount) as revenue
        FROM sales WHERE created_at >= start_date
        GROUP BY DATE(created_at) ORDER BY DATE(created_at)
    ) t;

    RETURN json_build_object('period', period, 'data', COALESCE(result, '[]'::json));
END;
$$;

create or replace function public.get_stock_view_data () RETURNS json LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
DECLARE result json;
BEGIN
    SELECT json_agg(t) INTO result FROM (
        SELECT p.name, p.sku, p.stock_quantity, p.min_stock_level, p.category,
            CASE WHEN p.stock_quantity = 0 THEN 'Sem Stock'
                 WHEN p.stock_quantity <= p.min_stock_level THEN 'Baixo'
                 ELSE 'Normal' END as status
        FROM products p ORDER BY p.stock_quantity ASC
    ) t;
    RETURN COALESCE(result, '[]'::json);
END;
$$;

create or replace function public.get_sales_view_data (
  start_date timestamp default null,
  end_date timestamp default null
) RETURNS json LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
DECLARE
    result json;
    v_start timestamp := COALESCE(start_date, NOW() - INTERVAL '30 days');
    v_end timestamp := COALESCE(end_date, NOW());
BEGIN
    SELECT json_agg(t) INTO result FROM (
        SELECT s.id, s.created_at, s.total_amount, s.payment_method, s.status, up.full_name as seller_name,
            (SELECT json_agg(json_build_object('product_name', p.name, 'quantity', si.quantity, 'unit_price', si.unit_price, 'subtotal', si.subtotal))
             FROM sales_items si JOIN products p ON si.product_id = p.id WHERE si.sale_id = s.id) as items
        FROM sales s JOIN user_profiles up ON s.user_id = up.id
        WHERE s.created_at BETWEEN v_start AND v_end ORDER BY s.created_at DESC
    ) t;
    RETURN COALESCE(result, '[]'::json);
END;
$$;
