-- ============================================================================
-- ARQUIVO MESTRE DE ATUALIZAÇÃO DO BANCO DE DADOS (V3 - FINAL)
-- Contém: Índices de Performance, Funções de Filtro Otimizadas, Views de Dashboard
-- ============================================================================
-- 1. CRIAÇÃO DE ÍNDICES DE PERFORMANCE (OBRIGATÓRIO PARA EVITAR TIMEOUTS)
-- ============================================================================
-- Acelerar tabelas de Vendas (Mês Atual)
create index IF not exists idx_detailed_superv on data_detailed (superv);

create index IF not exists idx_detailed_nome on data_detailed (nome);

create index IF not exists idx_detailed_codfor on data_detailed (codfor);

create index IF not exists idx_detailed_produto on data_detailed (produto);

create index IF not exists idx_detailed_codcli on data_detailed (codcli);

create index IF not exists idx_detailed_filial on data_detailed (filial);

create index IF not exists idx_detailed_dtped on data_detailed (dtped);

create index IF not exists idx_detailed_tipovenda on data_detailed (tipovenda);

create index IF not exists idx_detailed_observacaofor on data_detailed (observacaofor);

-- Acelerar tabelas de Histórico
create index IF not exists idx_history_superv on data_history (superv);

create index IF not exists idx_history_nome on data_history (nome);

create index IF not exists idx_history_codfor on data_history (codfor);

create index IF not exists idx_history_produto on data_history (produto);

create index IF not exists idx_history_codcli on data_history (codcli);

create index IF not exists idx_history_filial on data_history (filial);

create index IF not exists idx_history_dtped on data_history (dtped);

create index IF not exists idx_history_tipovenda on data_history (tipovenda);

create index IF not exists idx_history_observacaofor on data_history (observacaofor);

-- Acelerar cruzamentos (Estoque, Clientes, Produtos)
create index IF not exists idx_stock_code on data_stock (product_code);

create index IF not exists idx_stock_filial on data_stock (filial);

create index IF not exists idx_clients_code on data_clients (codigo_cliente);

create index IF not exists idx_clients_cidade on data_clients (cidade);

create index IF not exists idx_clients_ramo on data_clients (ramo);

create index IF not exists idx_products_code on data_product_details (code);

create index IF not exists idx_products_codfor on data_product_details (codfor);

-- Otimizar estatísticas do banco
analyze data_detailed;

analyze data_history;

analyze data_stock;

analyze data_clients;

-- 2. FUNÇÃO AUXILIAR DE FILTRAGEM DE CLIENTES (CORRIGIDA PARA TEXTO VAZIO)
-- ============================================================================
drop function IF exists get_filtered_client_base_json (jsonb);

create or replace function get_filtered_client_base_json (p_filters jsonb) RETURNS table (codigo_cliente TEXT) as $$
DECLARE
    -- Transforma string vazia '' em NULL para ignorar o filtro corretamente
    p_supervisor_filter TEXT := NULLIF(p_filters->>'supervisor', '');
    
    p_sellers_filter TEXT[] := CASE 
        WHEN p_filters ? 'sellers' AND jsonb_typeof(p_filters->'sellers') = 'array' 
        THEN ARRAY(SELECT jsonb_array_elements_text(p_filters->'sellers'))
        ELSE NULL
    END;
    
    p_rede_group_filter TEXT := NULLIF(p_filters->>'rede_group', '');
    
    p_redes_filter TEXT[] := CASE
        WHEN p_filters ? 'redes' AND jsonb_typeof(p_filters->'redes') = 'array'
        THEN ARRAY(SELECT jsonb_array_elements_text(p_filters->'redes'))
        ELSE NULL
    END;
    
    p_city_filter TEXT := NULLIF(p_filters->>'city', '');
    p_filial_filter TEXT := NULLIF(p_filters->>'filial', '');
BEGIN
    RETURN QUERY
    SELECT c.codigo_cliente
    FROM data_clients c
    WHERE
        -- Filtro de Cidade
        (p_city_filter IS NULL OR c.cidade ILIKE p_city_filter)
        AND
        -- Filtro de Rede
        (p_rede_group_filter IS NULL OR
         (p_rede_group_filter = 'sem_rede' AND (c.ramo IS NULL OR c.ramo = 'N/A')) OR
         (p_rede_group_filter = 'com_rede' AND (p_redes_filter IS NULL OR c.ramo = ANY(p_redes_filter)))
        )
        AND
        -- Filtro de Filial (Trata 'ambas')
        (p_filial_filter IS NULL OR p_filial_filter = 'ambas' OR EXISTS (
             SELECT 1 FROM data_detailed d WHERE d.codcli = c.codigo_cliente AND d.filial = p_filial_filter
            UNION ALL
            SELECT 1 FROM data_history h WHERE h.codcli = c.codigo_cliente AND h.filial = p_filial_filter
            LIMIT 1
        ))
        AND
        -- Filtro de Supervisor
        (p_supervisor_filter IS NULL OR EXISTS (
            SELECT 1 FROM data_detailed d WHERE d.codcli = c.codigo_cliente AND d.superv = p_supervisor_filter
            UNION ALL
            SELECT 1 FROM data_history h WHERE h.codcli = c.codigo_cliente AND h.superv = p_supervisor_filter
            LIMIT 1
        ))
        AND
        -- Filtro de Vendedor
        (p_sellers_filter IS NULL OR EXISTS (
            SELECT 1 FROM data_detailed d WHERE d.codcli = c.codigo_cliente AND d.nome = ANY(p_sellers_filter)
            UNION ALL
            SELECT 1 FROM data_history h WHERE h.codcli = c.codigo_cliente AND h.nome = ANY(p_sellers_filter)
            LIMIT 1
        ));
END;
$$ LANGUAGE plpgsql;

-- 3. FUNÇÃO DE ESTOQUE (CORRIGIDA DUPLICIDADE DE FILIAIS)
-- 3. FUNÇÃO DE ESTOQUE (V11 - Média Diária Dinâmica e Otimizada)
-- 3. FUNÇÃO DE ESTOQUE (V12 - Otimização de Performance Anti-Timeout)
-- ============================================================================
drop function IF exists get_stock_view_data (text, text[], text[], text[], text, text[], text);

drop function IF exists get_stock_view_data (
  text,
  text[],
  text[],
  text[],
  text,
  text[],
  text,
  integer
);

create or replace function get_stock_view_data (
  p_supervisor_filter TEXT default '',
  p_sellers_filter text[] default null,
  p_suppliers_filter text[] default null,
  p_products_filter text[] default null,
  p_rede_group_filter TEXT default '',
  p_redes_filter text[] default null,
  p_filial_filter TEXT default 'ambas',
  p_custom_days INTEGER default 0,
  p_city_filter TEXT default ''
) RETURNS JSONB as $$
DECLARE
    result JSONB;
    current_month_start DATE;
    last_sale_date DATE;
    history_start_date DATE;
    history_end_date DATE;
    first_ever_sale_date DATE;
    max_working_days INTEGER;
BEGIN
    SELECT date_trunc('month', MAX(dtped))::DATE, MAX(dtped)::DATE
    INTO current_month_start, last_sale_date
    FROM data_detailed;

    history_end_date := current_month_start - INTERVAL '1 day';
    history_start_date := current_month_start - INTERVAL '3 months';

    SELECT MIN(d.dtped) INTO first_ever_sale_date FROM (
        SELECT dtped FROM data_detailed UNION ALL SELECT dtped FROM data_history
    ) d WHERE dtped IS NOT NULL;

    SELECT COUNT(*) INTO max_working_days
    FROM generate_series(first_ever_sale_date, last_sale_date, '1 day') d
    WHERE extract(isodow from d) < 6;

    WITH
    final_clients AS (
        SELECT c.codigo_cliente 
        FROM data_clients c
        WHERE (p_city_filter = '' OR c.cidade ILIKE p_city_filter)
          AND (p_rede_group_filter = '' OR
              (p_rede_group_filter = 'sem_rede' AND (c.ramo IS NULL OR c.ramo = 'N/A')) OR
              (p_rede_group_filter = 'com_rede' AND (p_redes_filter IS NULL OR c.ramo = ANY(p_redes_filter))))
          AND (p_supervisor_filter = '' OR EXISTS (
            SELECT 1 FROM data_detailed d WHERE d.codcli = c.codigo_cliente AND d.superv = p_supervisor_filter
            UNION ALL SELECT 1 FROM data_history h WHERE h.codcli = c.codigo_cliente AND h.superv = p_supervisor_filter LIMIT 1
          ))
          AND (p_sellers_filter IS NULL OR EXISTS (
            SELECT 1 FROM data_detailed d WHERE d.codcli = c.codigo_cliente AND d.nome = ANY(p_sellers_filter)
            UNION ALL SELECT 1 FROM data_history h WHERE h.codcli = c.codigo_cliente AND h.nome = ANY(p_sellers_filter) LIMIT 1
          ))
    ),
    all_sales AS (
      SELECT produto, qtvenda_embalagem_master, dtped FROM data_detailed WHERE codcli IN (SELECT codigo_cliente FROM final_clients) AND (p_filial_filter = 'ambas' OR filial = p_filial_filter)
      UNION ALL
      SELECT produto, qtvenda_embalagem_master, dtped FROM data_history WHERE codcli IN (SELECT codigo_cliente FROM final_clients) AND (p_filial_filter = 'ambas' OR filial = p_filial_filter)
    ),
    working_days_ranked AS (
        SELECT d::date as work_date, row_number() OVER (ORDER BY d DESC) as rn
        FROM generate_series(first_ever_sale_date, last_sale_date, '1 day'::interval) d
        WHERE extract(isodow from d) < 6
    ),
    sales_metrics AS (
      SELECT
        s.produto,
        COUNT(DISTINCT s.dtped) FILTER (WHERE extract(isodow from s.dtped) < 6) as product_lifetime_days,
        SUM(CASE WHEN s.dtped >= current_month_start THEN s.qtvenda_embalagem_master ELSE 0 END) as current_month_sales_qty,
        SUM(CASE WHEN s.dtped BETWEEN history_start_date AND history_end_date THEN s.qtvenda_embalagem_master ELSE 0 END) / 3.0 as history_avg_monthly_qty
      FROM all_sales s
      GROUP BY s.produto
    ),
    stock_aggregated AS (
        SELECT product_code, SUM(stock_qty) as total_stock
        FROM data_stock
        WHERE (p_filial_filter = 'ambas' OR filial = p_filial_filter)
        GROUP BY product_code
    ),
    final_data AS (
        SELECT
            pd.code as product_code,
            pd.descricao as product_description,
            pd.fornecedor as supplier_name,
            COALESCE(st.total_stock, 0) as stock_qty,
            COALESCE(sm.current_month_sales_qty, 0) as current_month_sales_qty,
            COALESCE(sm.history_avg_monthly_qty, 0) as history_avg_monthly_qty,
            GREATEST(LEAST(
                CASE WHEN p_custom_days <= 0 OR p_custom_days > sm.product_lifetime_days THEN sm.product_lifetime_days ELSE p_custom_days END,
                max_working_days
            ), 1)::INTEGER as days_divisor
        FROM data_product_details pd
        LEFT JOIN sales_metrics sm ON pd.code = sm.produto
        LEFT JOIN stock_aggregated st ON pd.code = st.product_code
        WHERE (p_suppliers_filter IS NULL OR pd.codfor = ANY(p_suppliers_filter))
          AND (p_products_filter IS NULL OR pd.code = ANY(p_products_filter))
          AND (COALESCE(st.total_stock, 0) > 0 OR COALESCE(sm.current_month_sales_qty, 0) > 0 OR COALESCE(sm.history_avg_monthly_qty, 0) > 0)
    ),
    -- OTIMIZAÇÃO DE PERFORMANCE: Pré-agrega as vendas diárias para evitar sub-selects lentos.
    daily_sales AS (
        SELECT produto, dtped, SUM(qtvenda_embalagem_master) as total_qty
        FROM all_sales
        GROUP BY produto, dtped
    ),
    -- OTIMIZAÇÃO DE PERFORMANCE: Junta vendas diárias com o ranking de dias úteis.
    product_sales_ranked AS (
        SELECT ds.produto, ds.total_qty, wdr.rn
        FROM daily_sales ds
        JOIN working_days_ranked wdr ON ds.dtped = wdr.work_date
    ),
    -- OTIMIZAÇÃO DE PERFORMANCE: Calcula o total de vendas para o período dinâmico de cada produto com um JOIN eficiente.
    sales_in_window AS (
        SELECT
            fd.product_code,
            SUM(psr.total_qty) as total_sales_for_avg
        FROM final_data fd
        JOIN product_sales_ranked psr ON fd.product_code = psr.produto AND psr.rn <= fd.days_divisor
        GROUP BY fd.product_code
    ),
    -- Junta os dados finais e calcula a média diária de forma performática.
    categorized_products AS (
        SELECT
            fd.*,
            COALESCE(siw.total_sales_for_avg, 0) / fd.days_divisor as daily_avg_qty,
            CASE
                WHEN fd.current_month_sales_qty > 0 AND fd.history_avg_monthly_qty > 0 AND fd.current_month_sales_qty >= fd.history_avg_monthly_qty THEN 'growth'
                WHEN fd.current_month_sales_qty > 0 AND fd.history_avg_monthly_qty > 0 AND fd.current_month_sales_qty < fd.history_avg_monthly_qty THEN 'decline'
                WHEN fd.current_month_sales_qty > 0 AND fd.history_avg_monthly_qty = 0 THEN 'new'
                WHEN fd.current_month_sales_qty = 0 AND fd.history_avg_monthly_qty > 0 THEN 'lost'
                ELSE NULL
            END as category
        FROM final_data fd
        LEFT JOIN sales_in_window siw ON fd.product_code = siw.product_code
    )
    SELECT jsonb_build_object(
        'max_working_days', max_working_days,
        'working_days_used', CASE WHEN p_custom_days > 0 AND p_custom_days < max_working_days THEN p_custom_days ELSE max_working_days END,
        'stock_table', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'product_code', cp.product_code,
                'product_description', cp.product_description,
                'supplier_name', cp.supplier_name,
                'stock_qty', cp.stock_qty,
                'avg_monthly_qty', cp.history_avg_monthly_qty,
                'daily_avg_qty', cp.daily_avg_qty,
                'trend_days', CASE WHEN cp.daily_avg_qty > 0 THEN (cp.stock_qty / cp.daily_avg_qty) ELSE 999 END
            ) ORDER BY (CASE WHEN cp.daily_avg_qty > 0 THEN (cp.stock_qty / cp.daily_avg_qty) ELSE 999 END) ASC), '[]'::jsonb)
            FROM categorized_products cp
        ),
        'growth_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY (current_month_sales_qty - history_avg_monthly_qty) DESC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'growth'),
        'decline_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY (current_month_sales_qty - history_avg_monthly_qty) ASC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'decline'),
        'new_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY current_month_sales_qty DESC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'new'),
        'lost_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY history_avg_monthly_qty DESC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'lost')
    ) INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- 4. FUNÇÃO DE ACOMPANHAMENTO SEMANAL (RECRIADA)
-- ============================================================================
create or replace function get_weekly_view_data (
  p_supervisor TEXT default null,
  p_fornecedor TEXT default null
) RETURNS JSONB as $$
DECLARE
    result JSONB;
    current_month_start DATE;
BEGIN
    -- Define o início do mês atual com base na última venda
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start FROM data_detailed;

    WITH filtered_sales AS (
        SELECT * FROM data_detailed
        WHERE dtped >= current_month_start
          AND (p_supervisor IS NULL OR superv = p_supervisor)
          AND (p_fornecedor IS NULL OR observacaofor = p_fornecedor)
    ),
    -- Agrupamento por Semana e Dia
    weekly_agg AS (
        SELECT 
            (EXTRACT(DAY FROM dtped)::INT - 1) / 7 + 1 AS week_num,
            CASE EXTRACT(DOW FROM dtped)
                WHEN 1 THEN 'Segunda' WHEN 2 THEN 'Terça' WHEN 3 THEN 'Quarta'
                WHEN 4 THEN 'Quinta' WHEN 5 THEN 'Sexta' ELSE 'Outro'
            END as day_name,
            EXTRACT(DOW FROM dtped) as day_idx,
            SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0 END) as fat
        FROM filtered_sales
        WHERE EXTRACT(DOW FROM dtped) BETWEEN 1 AND 5 -- Apenas Seg a Sex
        GROUP BY 1, 2, 3
    ),
    -- Rankings
    ranking_positivacao AS (
        SELECT nome, COUNT(DISTINCT codcli) as total
        FROM filtered_sales GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    ),
    ranking_vendedores AS (
        SELECT nome, SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0 END) as total
        FROM filtered_sales GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    )
    SELECT jsonb_build_object(
        'total_month', (SELECT COALESCE(SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0 END), 0) FROM filtered_sales),
        'weekly_data', (SELECT COALESCE(jsonb_agg(w), '[]'::jsonb) FROM weekly_agg w),
        'positivacao_rank', (SELECT COALESCE(jsonb_agg(r), '[]'::jsonb) FROM ranking_positivacao r),
        'sellers_rank', (SELECT COALESCE(jsonb_agg(r), '[]'::jsonb) FROM ranking_vendedores r)
    ) INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- 5. FUNÇÃO DE COBERTURA (V4 - CORRIGIDA: KPI FILTRADO + MÉDIA HISTÓRICA)
-- ============================================================================
create or replace function get_coverage_view_data (
  p_supervisor_filter TEXT default '',
  p_sellers_filter text[] default null,
  p_suppliers_filter text[] default null,
  p_products_filter text[] default null,
  p_filial_filter TEXT default 'ambas',
  p_city_filter TEXT default ''
) RETURNS JSONB as $$
DECLARE
    result JSONB;
    current_month_start DATE;
    previous_month_start DATE;
    history_start DATE;
    history_end DATE;
BEGIN
    -- Define datas
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start FROM data_detailed;
    previous_month_start := current_month_start - INTERVAL '1 month';
    
    -- Datas para média histórica (3 meses para trás) - NECESSÁRIO PARA O CÁLCULO DE DIAS
    history_end := current_month_start - INTERVAL '1 day';
    history_start := current_month_start - INTERVAL '3 months';

    WITH
    -- 1. Filtro de Clientes (CORREÇÃO: Respeita os filtros para o KPI)
    filtered_clients AS (
        SELECT DISTINCT c.codigo_cliente
        FROM data_clients c
        LEFT JOIN data_detailed d ON c.codigo_cliente = d.codcli
        WHERE c.bloqueio <> 'S'
          AND (p_city_filter = '' OR c.cidade ILIKE p_city_filter)
          AND (p_supervisor_filter = '' OR EXISTS (
              SELECT 1 FROM data_detailed d2 WHERE d2.codcli = c.codigo_cliente AND d2.superv = p_supervisor_filter
          ))
          AND (p_sellers_filter IS NULL OR EXISTS (
              SELECT 1 FROM data_detailed d3 WHERE d3.codcli = c.codigo_cliente AND d3.nome = ANY(p_sellers_filter)
          ))
    ),

    -- 2. Dados de Estoque
    stock_sum AS (
        SELECT product_code, SUM(stock_qty) as total_stock 
        FROM data_stock 
        WHERE (p_filial_filter = 'ambas' OR filial = p_filial_filter)
        GROUP BY product_code
    ),
    
    -- 3. Média de Venda Histórica (ADICIONADO: Para cálculo de dias de estoque)
    history_avg AS (
        SELECT produto, SUM(qtvenda_embalagem_master) / 3.0 as avg_qty
        FROM data_history
        WHERE dtped BETWEEN history_start AND history_end
          AND (p_filial_filter = 'ambas' OR filial = p_filial_filter)
        GROUP BY 1
    ),

    -- 4. Base de Produtos com Estoque e Média
    base_products AS (
        SELECT 
            pd.code, 
            pd.descricao, 
            COALESCE(s.total_stock, 0) as stock_qty,
            COALESCE(h.avg_qty, 0) as history_avg_qty -- Campo essencial para o frontend
        FROM data_product_details pd
        LEFT JOIN stock_sum s ON pd.code = s.product_code
        LEFT JOIN history_avg h ON pd.code = h.produto
        WHERE (p_suppliers_filter IS NULL OR pd.codfor = ANY(p_suppliers_filter))
          AND (p_products_filter IS NULL OR pd.code = ANY(p_products_filter))
    ),

    -- 5. Vendas Recentes
    sales_data AS (
        SELECT 
            s.produto, 
            s.codcli, 
            CASE WHEN s.dtped >= current_month_start THEN 1 ELSE 0 END as is_current
        FROM (
            SELECT produto, codcli, dtped, superv, nome, filial, codfor FROM data_detailed
            UNION ALL
            SELECT produto, codcli, dtped, superv, nome, filial, codfor FROM data_history
        ) s
        JOIN data_clients c ON s.codcli = c.codigo_cliente
        WHERE s.dtped >= previous_month_start
          AND (p_supervisor_filter = '' OR s.superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR s.nome = ANY(p_sellers_filter))
          AND (p_filial_filter = 'ambas' OR s.filial = p_filial_filter)
          AND (p_suppliers_filter IS NULL OR s.codfor = ANY(p_suppliers_filter))
          AND (p_city_filter = '' OR c.cidade ILIKE p_city_filter)
    ),

    -- 6. Agregação Final
    final_agg AS (
        SELECT 
            bp.code,
            bp.descricao,
            bp.stock_qty,
            bp.history_avg_qty,
            COUNT(DISTINCT CASE WHEN sd.is_current = 0 THEN sd.codcli END) as clients_prev,
            COUNT(DISTINCT CASE WHEN sd.is_current = 1 THEN sd.codcli END) as clients_curr
        FROM base_products bp
        LEFT JOIN sales_data sd ON bp.code = sd.produto
        GROUP BY 1, 2, 3, 4
    )
    
    SELECT jsonb_build_object(
        'coverage_table', (SELECT COALESCE(jsonb_agg(fa ORDER BY clients_curr DESC), '[]'::jsonb) FROM final_agg fa),
        'active_clients_count', (SELECT COUNT(*) FROM filtered_clients) -- Usa a contagem filtrada
    ) INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;
