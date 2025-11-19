-- =================================================================
-- FUNÇÃO AUXILIAR OTIMIZADA (V2) PARA FILTRAGEM DE CLIENTES (JSONB)
-- =================================================================
drop function IF exists get_filtered_client_base_json (jsonb);

create or replace function get_filtered_client_base_json (p_filters jsonb) RETURNS table (codigo_cliente TEXT) as $$
DECLARE
    -- Extrai os valores do JSON para variáveis locais, com tratamento de nulos (V2)
    p_supervisor_filter TEXT := p_filters->>'supervisor';
    p_sellers_filter TEXT[] := CASE 
        WHEN p_filters ? 'sellers' AND jsonb_typeof(p_filters->'sellers') = 'array' 
        THEN ARRAY(SELECT jsonb_array_elements_text(p_filters->'sellers'))
        ELSE NULL
    END;
    p_rede_group_filter TEXT := p_filters->>'rede_group';
    p_redes_filter TEXT[] := CASE
        WHEN p_filters ? 'redes' AND jsonb_typeof(p_filters->'redes') = 'array'
        THEN ARRAY(SELECT jsonb_array_elements_text(p_filters->'redes'))
        ELSE NULL
    END;
    p_city_filter TEXT := p_filters->>'city';
    p_filial_filter TEXT := p_filters->>'filial';
BEGIN
    RETURN QUERY
    SELECT c.codigo_cliente
    FROM data_clients c
    WHERE
        -- Filtros diretos na tabela de clientes
        (p_city_filter IS NULL OR c.cidade ILIKE p_city_filter)
        AND
        (p_rede_group_filter IS NULL OR
         (p_rede_group_filter = 'sem_rede' AND (c.ramo IS NULL OR c.ramo = 'N/A')) OR
         (p_rede_group_filter = 'com_rede' AND (p_redes_filter IS NULL OR c.ramo = ANY(p_redes_filter)))
        )
        AND
        -- Filtros que dependem de dados de vendas (usando subconsultas EXISTS mais eficientes)
        (p_supervisor_filter IS NULL OR EXISTS (
            SELECT 1 FROM data_detailed d WHERE d.codcli = c.codigo_cliente AND d.superv = p_supervisor_filter
            UNION ALL
            SELECT 1 FROM data_history h WHERE h.codcli = c.codigo_cliente AND h.superv = p_supervisor_filter
            LIMIT 1
        ))
        AND
        (p_sellers_filter IS NULL OR EXISTS (
            SELECT 1 FROM data_detailed d WHERE d.codcli = c.codigo_cliente AND d.nome = ANY(p_sellers_filter)
            UNION ALL
            SELECT 1 FROM data_history h WHERE h.codcli = c.codigo_cliente AND h.nome = ANY(p_sellers_filter)
            LIMIT 1
        ))
        AND
        (p_filial_filter = 'ambas' OR EXISTS (
             SELECT 1 FROM data_detailed d WHERE d.codcli = c.codigo_cliente AND d.filial = p_filial_filter
            UNION ALL
            SELECT 1 FROM data_history h WHERE h.codcli = c.codigo_cliente AND h.filial = p_filial_filter
            LIMIT 1
        ));
END;
$$ LANGUAGE plpgsql;

-- ATUALIZAÇÃO 1: OTIMIZAÇÃO DA FUNÇÃO get_comparison_data PARA CORRIGIR O TIMEOUT
-- Esta versão pré-agrega os dados históricos para evitar recálculos caros.
create or replace function get_comparison_data (
  p_supervisor_filter TEXT default null,
  p_sellers_filter text[] default null,
  p_suppliers_filter text[] default null,
  p_products_filter text[] default null,
  p_pasta_filter TEXT default null,
  p_city_filter TEXT default null,
  p_filial_filter TEXT default 'ambas',
  p_rede_group_filter TEXT default null,
  p_redes_filter text[] default null
) RETURNS JSONB as $$
DECLARE
    result JSONB;
    current_month_start DATE;
    history_start_date DATE;
    history_end_date DATE;
BEGIN
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start FROM data_detailed;
    history_end_date := current_month_start - 1;
    history_start_date := current_month_start - INTERVAL '3 months';

    WITH
    -- OTIMIZAÇÃO V2: Usa a função auxiliar JSONB
    filtered_clients AS (
        SELECT * FROM get_filtered_client_base_json(jsonb_build_object(
            'supervisor', p_supervisor_filter,
            'sellers', p_sellers_filter,
            'rede_group', p_rede_group_filter,
            'redes', p_redes_filter,
            'city', p_city_filter,
            'filial', p_filial_filter
        ))
    ),
    -- OTIMIZAÇÃO: Faz JOIN com os clientes filtrados
    current_sales AS (
        SELECT s.*
        FROM data_detailed s
        JOIN filtered_clients fc ON s.codcli = fc.codigo_cliente
        WHERE (p_suppliers_filter IS NULL OR s.codfor = ANY(p_suppliers_filter))
          AND (p_products_filter IS NULL OR s.produto = ANY(p_products_filter))
          AND (p_pasta_filter IS NULL OR s.observacaofor = p_pasta_filter)
    ),
    history_sales AS (
        SELECT s.*
        FROM data_history s
        JOIN filtered_clients fc ON s.codcli = fc.codigo_cliente
        WHERE s.dtped BETWEEN history_start_date AND history_end_date
          AND (p_suppliers_filter IS NULL OR s.codfor = ANY(p_suppliers_filter))
          AND (p_products_filter IS NULL OR s.produto = ANY(p_products_filter))
          AND (p_pasta_filter IS NULL OR s.observacaofor = p_pasta_filter)
    ),
    -- KPIs
    kpis_current AS (
        SELECT
            COALESCE(SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0::numeric END), 0) AS total_fat,
            COALESCE(SUM(totpesoliq), 0) AS total_peso,
            COUNT(DISTINCT codcli) AS total_clients
        FROM current_sales
    ),
    kpis_history_monthly AS (
        SELECT
            date_trunc('month', dtped) as month,
            COALESCE(SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0::numeric END), 0) AS total_fat,
            COALESCE(SUM(totpesoliq), 0) AS total_peso,
            COUNT(DISTINCT codcli) AS total_clients
        FROM history_sales
        GROUP BY 1
    ),
    kpis_history_avg AS (
        SELECT
            COALESCE(AVG(total_fat), 0) AS avg_fat,
            COALESCE(AVG(total_peso), 0) AS avg_peso,
            COALESCE(AVG(total_clients), 0) AS avg_clients
        FROM kpis_history_monthly
    ),

    -- CORREÇÃO: Lógica de cálculo da média histórica semanal
    history_sales_with_week_num AS (
        SELECT
            s.dtped,
            s.tipovenda,
            s.vlvenda,
            floor((extract(day from s.dtped) - 1) / 7) + 1 AS week_of_month
        FROM history_sales s
    ),
    history_avg_by_week_num AS (
        SELECT
            week_of_month,
            COALESCE(SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0::numeric END) / 3.0, 0) AS avg_faturamento
        FROM history_sales_with_week_num
        GROUP BY week_of_month
    ),
    history_supervisor_agg AS (
        SELECT
            superv,
            SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0::numeric END) as total_fat
        FROM history_sales
        GROUP BY superv
    ),

    -- Geração de semanas para o mês atual
    week_series AS (
        SELECT
            n as week_num,
            (date_trunc('week', current_month_start)::date + ((n-1) || ' weeks')::interval) as week_start,
            (date_trunc('week', current_month_start)::date + (n || ' weeks')::interval - '1 day'::interval) as week_end
        FROM generate_series(1, 6) n
        WHERE (date_trunc('week', current_month_start)::date + ((n-1) || ' weeks')::interval) < (current_month_start + '1 month'::interval)
    ),
    -- Gráfico de comparação semanal (CORRIGIDO para usar nova lógica de média e tipos corretos)
    weekly_comparison_chart AS (
        SELECT
            COALESCE(jsonb_agg(
                jsonb_build_object(
                    'week_num', ws.week_num,
                    'current_faturamento', (
                        SELECT COALESCE(SUM(CASE WHEN cs.tipovenda IN ('1', '9') THEN cs.vlvenda ELSE 0::numeric END), 0)
                        FROM current_sales cs
                        WHERE cs.dtped BETWEEN ws.week_start AND ws.week_end
                    ),
                    'history_avg_faturamento', (
                         SELECT COALESCE(haw.avg_faturamento, 0)
                         FROM history_avg_by_week_num haw
                         WHERE haw.week_of_month = ws.week_num
                    )
                ) ORDER BY ws.week_num
            ), '[]'::jsonb) AS data
        FROM week_series ws
    ),
    -- Tabela de Supervisores (MODIFICADO para usar a CTE otimizada)
    supervisor_comparison_table AS (
        SELECT COALESCE(jsonb_agg(t ORDER BY current_faturamento DESC), '[]'::jsonb) AS data
        FROM (
            SELECT
                s.superv,
                COALESCE(SUM(CASE WHEN s.tipovenda IN ('1', '9') THEN s.vlvenda ELSE 0::numeric END), 0) AS current_faturamento,
                (SELECT COALESCE(hsa.total_fat / 3.0, 0) FROM history_supervisor_agg hsa WHERE hsa.superv = s.superv) AS history_avg_faturamento
            FROM current_sales s
            WHERE s.superv IS NOT NULL
            GROUP BY s.superv
        ) t
    )

    -- Montagem final do JSON
    SELECT jsonb_build_object(
        'kpis', (
            SELECT jsonb_build_object(
                'current_total_fat', kc.total_fat,
                'history_avg_fat', kha.avg_fat,
                'current_total_peso', kc.total_peso,
                'history_avg_peso', kha.avg_peso,
                'current_clients', kc.total_clients,
                'history_avg_clients', kha.avg_clients,
                'current_ticket', CASE WHEN kc.total_clients > 0 THEN kc.total_fat / kc.total_clients ELSE 0 END,
                'history_avg_ticket', CASE WHEN kha.avg_clients > 0 THEN kha.avg_fat / kha.avg_clients ELSE 0 END
            ) FROM kpis_current kc, kpis_history_avg kha
        ),
        'charts', (
            SELECT jsonb_build_object(
                'weekly_comparison', wc.data
            ) FROM weekly_comparison_chart wc
        ),
        'tables', (
            SELECT jsonb_build_object(
                'supervisor_comparison', sc.data
            ) FROM supervisor_comparison_table sc
        )
    )
    INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- NOVA FUNÇÃO: get_detailed_orders para a nova visualização de Pedidos
create or replace function get_detailed_orders (
  p_supervisor_filter TEXT default '',
  p_sellers_filter text[] default null,
  p_tipo_venda_filter text[] default null,
  p_fornecedor_filter TEXT default '',
  p_codcli_filter TEXT default '',
  p_posicao_filter TEXT default '',
  p_rede_group_filter TEXT default '',
  p_redes_filter text[] default null,
  page_number INT default 1,
  page_size INT default 50
) RETURNS JSONB as $$
DECLARE
    result JSONB;
    offset_val INT;
BEGIN
    offset_val := (page_number - 1) * page_size;

    WITH filtered_clients AS (
        SELECT codigo_cliente
        FROM data_clients
        WHERE (p_rede_group_filter = '' OR p_rede_group_filter IS NULL)
           OR (p_rede_group_filter = 'sem_rede' AND (ramo IS NULL OR ramo = 'N/A'))
           OR (p_rede_group_filter = 'com_rede' AND (p_redes_filter IS NULL OR ramo = ANY(p_redes_filter)))
    ),
    filtered_orders AS (
        SELECT *
        FROM data_orders
        WHERE codcli IN (SELECT codigo_cliente FROM filtered_clients)
          AND (p_supervisor_filter = '' OR superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR nome = ANY(p_sellers_filter))
          AND (p_tipo_venda_filter IS NULL OR tipovenda = ANY(p_tipo_venda_filter))
          AND (p_fornecedor_filter = '' OR codfor = p_fornecedor_filter)
          AND (p_codcli_filter = '' OR codcli = p_codcli_filter)
          AND (p_posicao_filter = '' OR posicao = p_posicao_filter)
    ),
    paginated_orders AS (
        SELECT *
        FROM filtered_orders
        ORDER BY dtped DESC
        LIMIT page_size
        OFFSET offset_val
    )
    SELECT jsonb_build_object(
        'orders', (SELECT COALESCE(jsonb_agg(po), '[]'::jsonb) FROM paginated_orders po),
        'total_count', (SELECT COUNT(*) FROM filtered_orders)
    )
    INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- ATUALIZAÇÃO 3: OTIMIZAÇÃO DA FUNÇÃO get_initial_dashboard_data PARA PERFORMANCE
-- Esta versão quebra a consulta monolítica em CTEs menores e mais eficientes.
create or replace function get_initial_dashboard_data (
  supervisor_filter TEXT default null,
  pasta_filter TEXT default null,
  sellers_filter text[] default null,
  fornecedor_filter TEXT default null,
  posicao_filter TEXT default null,
  codcli_filter TEXT default null
) RETURNS JSONB as $$
DECLARE
    result JSONB;
BEGIN
    WITH
    -- Base de vendas filtrada, usada por todas as outras CTEs
    filtered_sales AS (
        SELECT s.*
        FROM data_detailed s
        WHERE
            (supervisor_filter IS NULL OR s.superv = supervisor_filter)
            AND (pasta_filter IS NULL OR s.observacaofor = pasta_filter)
            AND (sellers_filter IS NULL OR s.nome = ANY(sellers_filter))
            AND (fornecedor_filter IS NULL OR s.codfor = fornecedor_filter)
            AND (posicao_filter IS NULL OR s.posicao = posicao_filter)
            AND (codcli_filter IS NULL OR s.codcli = codcli_filter)
    ),
    -- CTE para KPIs principais
    kpi_agg AS (
        SELECT
            COALESCE(SUM(s.vlvenda), 0) AS total_faturamento,
            COALESCE(SUM(s.totpesoliq), 0) AS total_peso,
            COUNT(DISTINCT CASE WHEN s.vlvenda > 0 OR s.vlbonific > 0 THEN s.codcli END) AS positivacao_count,
            COUNT(CASE WHEN s.vlvenda > 0 OR s.vlbonific > 0 THEN 1 END) AS total_skus
        FROM filtered_sales s
    ),
    -- CTE para contagem total de clientes (independente dos filtros de venda)
    total_clients_agg AS (
        SELECT COUNT(DISTINCT codigo_cliente) AS total_clients_for_coverage
        FROM data_clients
        WHERE (razaosocial ILIKE '%AMERICANAS%' OR rca1 <> '53')
    ),
    -- CTE para agregação por supervisor
    sales_by_supervisor_agg AS (
        SELECT s.superv, SUM(s.vlvenda) AS total_faturamento
        FROM filtered_sales s
        GROUP BY s.superv
    ),
    -- CTE para agregação por pasta/categoria
    sales_by_pasta_agg AS (
        SELECT s.observacaofor, SUM(s.vlvenda) AS total_faturamento
        FROM filtered_sales s
        GROUP BY s.observacaofor
    ),
    -- CTE para Top 10 Produtos por Faturamento
    top_10_products_faturamento_agg AS (
        SELECT s.produto, s.descricao, SUM(s.vlvenda) AS faturamento
        FROM filtered_sales s
        GROUP BY s.produto, s.descricao
        ORDER BY faturamento DESC
        LIMIT 10
    ),
    -- CTE para Top 10 Produtos por Peso
    top_10_products_peso_agg AS (
        SELECT s.produto, s.descricao, SUM(s.totpesoliq) AS peso
        FROM filtered_sales s
        GROUP BY s.produto, s.descricao
        ORDER BY peso DESC
        LIMIT 10
    ),
    -- CTE para a média histórica (gráfico de tendência)
    history_trend_agg AS (
        SELECT AVG(monthly_revenue) as avg_revenue
        FROM (
            SELECT SUM(vlvenda) AS monthly_revenue
            FROM data_history
            WHERE dtped >= date_trunc('month', NOW()) - INTERVAL '3 months'
            AND dtped < date_trunc('month', NOW())
            GROUP BY date_trunc('month', dtped)
        ) AS monthly_data
    ),
    -- CTE para os filtros (agora busca de todas as vendas, não apenas as filtradas)
    filters_cte AS (
        SELECT
            (SELECT jsonb_agg(DISTINCT superv ORDER BY superv) FROM data_detailed WHERE superv IS NOT NULL) AS supervisors,
            (SELECT jsonb_agg(DISTINCT nome ORDER BY nome) FROM data_detailed WHERE nome IS NOT NULL) AS sellers,
            (SELECT jsonb_agg(DISTINCT jsonb_build_object('codfor', codfor, 'fornecedor', fornecedor)) FROM (SELECT DISTINCT codfor, fornecedor FROM data_detailed WHERE codfor IS NOT NULL AND fornecedor IS NOT NULL ORDER BY fornecedor) s) AS suppliers,
            (SELECT jsonb_agg(DISTINCT tipovenda ORDER BY tipovenda) FROM data_detailed WHERE tipovenda IS NOT NULL) AS sale_types
    ),
    -- CTE para metadados
    metadata_cte AS (
        SELECT
            (SELECT value FROM data_metadata WHERE key = 'last_update' LIMIT 1) AS last_update,
            (SELECT value::INTEGER FROM data_metadata WHERE key = 'passed_working_days' LIMIT 1) AS passed_working_days
    )

    -- Montagem final do JSON
    SELECT jsonb_build_object(
        'kpis', (SELECT to_jsonb(k) FROM kpi_agg k) || (SELECT to_jsonb(tc) FROM total_clients_agg tc),
        'charts', jsonb_build_object(
            'sales_by_supervisor', (SELECT COALESCE(jsonb_agg(sbs ORDER BY total_faturamento DESC), '[]'::jsonb) FROM sales_by_supervisor_agg sbs),
            'sales_by_pasta', (SELECT COALESCE(jsonb_agg(sbp ORDER BY total_faturamento ASC), '[]'::jsonb) FROM sales_by_pasta_agg sbp),
            'top_10_products_faturamento', (SELECT COALESCE(jsonb_agg(tpf), '[]'::jsonb) FROM top_10_products_faturamento_agg tpf),
            'top_10_products_peso', (SELECT COALESCE(jsonb_agg(tpp), '[]'::jsonb) FROM top_10_products_peso_agg tpp),
            'trend', jsonb_build_object(
                'avg_revenue', (SELECT avg_revenue FROM history_trend_agg),
                'trend_revenue', (SELECT total_faturamento FROM kpi_agg) / GREATEST((SELECT passed_working_days FROM metadata_cte), 1) * 22
            )
        ),
        'filters', (SELECT to_jsonb(f) FROM filters_cte f)
    )
    INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- ========= FIM DO BLOCO DE CÓDIGO SQL =========
-- NOVA FUNÇÃO: get_stock_view_data para a visualização de Estoque (RESTAURADA)
drop function IF exists get_stock_view_data (TEXT, text[], text[], text[], TEXT, TEXT);

create or replace function get_stock_view_data (
  p_supervisor_filter TEXT default '',
  p_sellers_filter text[] default null,
  p_suppliers_filter text[] default null,
  p_products_filter text[] default null,
  p_rede_group_filter TEXT default '',
  p_redes_filter text[] default null
) RETURNS JSONB as $$
DECLARE
    result JSONB;
    current_month_start DATE;
    history_start_date DATE;
    history_end_date DATE;
BEGIN
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start FROM data_detailed;
    history_end_date := current_month_start - INTERVAL '1 day';
    history_start_date := current_month_start - INTERVAL '3 months';

    WITH
    -- OTIMIZAÇÃO V2: Usa a função auxiliar JSONB
    filtered_clients AS (
        SELECT * FROM get_filtered_client_base_json(jsonb_build_object(
            'supervisor', p_supervisor_filter,
            'sellers', p_sellers_filter,
            'rede_group', p_rede_group_filter,
            'redes', p_redes_filter,
            'city', null, -- city_filter não é usado aqui
            'filial', 'ambas' -- filial_filter não é usado aqui
        ))
    ),
    -- OTIMIZAÇÃO: Faz JOIN com os clientes filtrados
    current_sales AS (
        SELECT s.produto, s.qtvenda_embalagem_master
        FROM data_detailed s
        JOIN filtered_clients fc ON s.codcli = fc.codigo_cliente
    ),
    history_sales AS (
        SELECT s.produto, s.qtvenda_embalagem_master
        FROM data_history s
        JOIN filtered_clients fc ON s.codcli = fc.codigo_cliente
        WHERE s.dtped BETWEEN history_start_date AND history_end_date
    ),
    -- 2. Agregações de vendas por produto
    current_sales_agg AS (
        SELECT produto, SUM(qtvenda_embalagem_master) as total_qty
        FROM current_sales GROUP BY produto
    ),
    history_sales_agg AS (
        SELECT produto, SUM(qtvenda_embalagem_master) / 3.0 as avg_monthly_qty
        FROM history_sales GROUP BY produto
    ),
    -- 3. Combina todos os dados para análise
    product_analysis AS (
        SELECT
            pd.code AS product_code,
            pd.descricao AS product_description,
            pd.fornecedor AS supplier_name,
            COALESCE(s.stock_qty, 0) as stock_qty,
            COALESCE(csa.total_qty, 0) as current_month_sales_qty,
            COALESCE(hsa.avg_monthly_qty, 0) as history_avg_monthly_qty
        FROM data_product_details pd
        LEFT JOIN data_stock s ON pd.code = s.product_code
        LEFT JOIN current_sales_agg csa ON pd.code = csa.produto
        LEFT JOIN history_sales_agg hsa ON pd.code = hsa.produto
        WHERE
            (p_suppliers_filter IS NULL OR pd.codfor = ANY(p_suppliers_filter))
            AND (p_products_filter IS NULL OR pd.code = ANY(p_products_filter))
            AND (COALESCE(s.stock_qty, 0) > 0 OR COALESCE(csa.total_qty, 0) > 0 OR COALESCE(hsa.avg_monthly_qty, 0) > 0)
    ),
    -- 4. Classifica os produtos nas 4 categorias
    categorized_products AS (
        SELECT
            *,
            CASE
                WHEN current_month_sales_qty > 0 AND history_avg_monthly_qty > 0 AND current_month_sales_qty >= history_avg_monthly_qty THEN 'growth'
                WHEN current_month_sales_qty > 0 AND history_avg_monthly_qty > 0 AND current_month_sales_qty < history_avg_monthly_qty THEN 'decline'
                WHEN current_month_sales_qty > 0 AND history_avg_monthly_qty = 0 THEN 'new'
                WHEN current_month_sales_qty = 0 AND history_avg_monthly_qty > 0 THEN 'lost'
                ELSE NULL
            END as category
        FROM product_analysis
    )
    -- 5. Monta o JSON final com todas as tabelas
    SELECT jsonb_build_object(
        'stock_table', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'product_code', pa.product_code,
                'product_description', pa.product_description,
                'supplier_name', pa.supplier_name,
                'stock_qty', pa.stock_qty,
                'avg_monthly_qty', pa.history_avg_monthly_qty,
                'daily_avg_qty', pa.history_avg_monthly_qty / 22.0,
                'trend_days', CASE WHEN pa.history_avg_monthly_qty > 0 THEN (pa.stock_qty / (pa.history_avg_monthly_qty / 22.0)) ELSE 999 END
            ) ORDER BY (CASE WHEN pa.history_avg_monthly_qty > 0 THEN (pa.stock_qty / (pa.history_avg_monthly_qty / 22.0)) ELSE 999 END) ASC), '[]'::jsonb)
            FROM product_analysis pa
        ),
        'growth_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY (current_month_sales_qty - history_avg_monthly_qty) DESC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'growth'),
        'decline_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY (current_month_sales_qty - history_avg_monthly_qty) ASC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'decline'),
        'new_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY current_month_sales_qty DESC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'new'),
        'lost_table', (SELECT COALESCE(jsonb_agg(cp ORDER BY history_avg_monthly_qty DESC), '[]'::jsonb) FROM categorized_products cp WHERE category = 'lost')
    ) INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- NOVA FUNÇÃO: get_innovations_data_v2 para a visualização de Inovações (RESTAURADA E RENOMEADA)
drop function IF exists get_innovations_view_data (TEXT, text[], TEXT, text[]);

-- Remove a antiga versão
drop function IF exists get_innovations_data_v2 (TEXT, text[], text[], BOOLEAN, TEXT, TEXT, text[]);

-- Garante que a nova não existe
create or replace function get_innovations_data_v2 (
  p_supervisor_filter TEXT default '',
  p_sellers_filter text[] default null,
  p_product_codes text[] default null,
  p_include_bonus BOOLEAN default true,
  p_city_filter TEXT default '',
  p_filial_filter TEXT default 'ambas',
  p_redes_filter text[] default null
) RETURNS JSONB as $$
DECLARE
    result JSONB;
    current_month_start DATE;
    previous_month_start DATE;
    previous_month_end DATE;
BEGIN
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start FROM data_detailed;
    previous_month_start := current_month_start - INTERVAL '1 month';
    previous_month_end := current_month_start - INTERVAL '1 day';

    WITH
    -- 1. Clientes ativos no filtro geral
    active_clients AS (
        SELECT c.codigo_cliente, c.rca1 FROM data_clients c
        WHERE c.bloqueio <> 'S'
          AND (p_redes_filter IS NULL OR c.ramo = ANY(p_redes_filter))
          AND (p_city_filter = '' OR c.cidade ILIKE p_city_filter)
          -- Adicionar lógica de filial se necessário
    ),
    -- 2. Vendas dos produtos selecionados
    sales AS (
        -- Mês Atual
        SELECT codcli, produto, tipovenda FROM data_detailed
        WHERE produto = ANY(p_product_codes)
          AND (p_supervisor_filter = '' OR superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR nome = ANY(p_sellers_filter))
          AND (p_filial_filter = 'ambas' OR filial = p_filial_filter)
          AND (p_include_bonus OR tipovenda <> '4')
        UNION ALL
        -- Mês Anterior
        SELECT codcli, produto, tipovenda FROM data_history
        WHERE dtped BETWEEN previous_month_start AND previous_month_end
          AND produto = ANY(p_product_codes)
          AND (p_supervisor_filter = '' OR superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR nome = ANY(p_sellers_filter))
          AND (p_filial_filter = 'ambas' OR filial = p_filial_filter)
          AND (p_include_bonus OR tipovenda <> '4')
    ),
    -- 3. Análise por produto
    product_coverage AS (
        SELECT
            p.code AS product_code,
            p.descricao,
            COALESCE(s.stock_qty, 0) as stock_qty,
            -- Contagem de clientes únicos do mês ATUAL
            COUNT(DISTINCT CASE WHEN sa.codcli IS NOT NULL THEN sa.codcli END) FILTER (WHERE sa.dtped >= current_month_start) as current_clients,
            -- Contagem de clientes únicos do mês ANTERIOR
            COUNT(DISTINCT CASE WHEN sa.codcli IS NOT NULL THEN sa.codcli END) FILTER (WHERE sa.dtped < current_month_start) as previous_clients
        FROM unnest(p_product_codes) pc(code)
        JOIN data_product_details p ON p.code = pc.code
        LEFT JOIN data_stock s ON s.product_code = p.code
        LEFT JOIN (
             SELECT s_agg.codcli, s_agg.produto, d.dtped FROM sales s_agg JOIN data_detailed d ON s_agg.codcli = d.codcli AND s_agg.produto = d.produto
             UNION ALL
             SELECT s_agg.codcli, s_agg.produto, h.dtped FROM sales s_agg JOIN data_history h ON s_agg.codcli = h.codcli AND s_agg.produto = h.produto
        ) sa ON sa.produto = p.code AND sa.codcli IN (SELECT codigo_cliente FROM active_clients)
        GROUP BY p.code, p.descricao, s.stock_qty
    )
    -- 4. Montagem do JSON final
    SELECT jsonb_build_object(
        'active_clients_count', (SELECT COUNT(*) FROM active_clients),
        'coverage_table', (SELECT COALESCE(jsonb_agg(pc ORDER BY current_clients DESC), '[]'::jsonb) FROM product_coverage pc)
    ) INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;
