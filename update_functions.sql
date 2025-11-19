-- ========= INÍCIO DO BLOCO DE CÓDIGO SQL =========
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
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start
    FROM (
        SELECT dtped FROM data_detailed
        UNION ALL
        SELECT dtped FROM data_history
    ) AS all_dates;
    history_end_date := current_month_start - 1;
    history_start_date := current_month_start - INTERVAL '3 months';

    WITH
    -- Base de clientes filtrada
    filtered_clients AS (
        SELECT codigo_cliente FROM data_clients
        WHERE
            (p_rede_group_filter IS NULL) OR
            (p_rede_group_filter = 'sem_rede' AND (ramo IS NULL OR ramo = 'N/A')) OR
            (p_rede_group_filter = 'com_rede' AND (p_redes_filter IS NULL OR ramo = ANY(p_redes_filter)))
    ),
    -- Vendas do mês atual filtradas
    current_sales AS (
        SELECT s.* FROM data_detailed s
        WHERE s.codcli IN (SELECT codigo_cliente FROM filtered_clients)
        AND (p_supervisor_filter IS NULL OR s.superv = p_supervisor_filter)
        AND (p_sellers_filter IS NULL OR s.nome = ANY(p_sellers_filter))
        AND (p_suppliers_filter IS NULL OR s.codfor = ANY(p_suppliers_filter))
        AND (p_products_filter IS NULL OR s.produto = ANY(p_products_filter))
        AND (p_pasta_filter IS NULL OR s.observacaofor = p_pasta_filter)
        AND (p_city_filter IS NULL OR s.cidade ILIKE p_city_filter)
        AND (p_filial_filter = 'ambas' OR s.filial = p_filial_filter)
    ),
    -- Vendas históricas filtradas
    history_sales AS (
        SELECT s.* FROM data_history s
        WHERE s.dtped BETWEEN history_start_date AND history_end_date
        AND s.codcli IN (SELECT codigo_cliente FROM filtered_clients)
        AND (p_supervisor_filter IS NULL OR s.superv = p_supervisor_filter)
        AND (p_sellers_filter IS NULL OR s.nome = ANY(p_sellers_filter))
        AND (p_suppliers_filter IS NULL OR s.codfor = ANY(p_suppliers_filter))
        AND (p_products_filter IS NULL OR s.produto = ANY(p_products_filter))
        AND (p_pasta_filter IS NULL OR s.observacaofor = p_pasta_filter)
        AND (p_city_filter IS NULL OR s.cidade ILIKE p_city_filter)
        AND (p_filial_filter = 'ambas' OR s.filial = p_filial_filter)
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

    -- OTIMIZAÇÃO 1: Pré-agregar vendas históricas por semana e supervisor
    history_weekly_sales AS (
        SELECT
            date_trunc('week', dtped)::date as week_start,
            SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0 END) as weekly_faturamento
        FROM history_sales
        GROUP BY 1
    ),
    history_supervisor_agg AS (
        SELECT
            superv,
            SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0 END) as total_fat
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
    -- Gráfico de comparação semanal (MODIFICADO para usar a CTE otimizada 'history_weekly_sales')
    weekly_comparison_chart AS (
        SELECT
            COALESCE(jsonb_agg(
                jsonb_build_object(
                    'week_num', ws.week_num,
                    'current_faturamento', (
                        SELECT COALESCE(SUM(CASE WHEN cs.tipovenda IN ('1', '9') THEN cs.vlvenda ELSE 0 END), 0)
                        FROM current_sales cs
                        WHERE cs.dtped BETWEEN ws.week_start AND ws.week_end
                    ),
                    'history_avg_faturamento', (
                        (SELECT COALESCE(hws.weekly_faturamento, 0) FROM history_weekly_sales hws WHERE hws.week_start = (date_trunc('week', ws.week_start - '1 month'::interval))::date) +
                        (SELECT COALESCE(hws.weekly_faturamento, 0) FROM history_weekly_sales hws WHERE hws.week_start = (date_trunc('week', ws.week_start - '2 months'::interval))::date) +
                        (SELECT COALESCE(hws.weekly_faturamento, 0) FROM history_weekly_sales hws WHERE hws.week_start = (date_trunc('week', ws.week_start - '3 months'::interval))::date)
                    ) / 3.0
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

-- NOVA FUNÇÃO: get_stock_view_data para a página de Análise de Estoque
CREATE OR REPLACE FUNCTION get_stock_view_data(
    p_filial_filter TEXT DEFAULT 'ambas',
    p_rede_group_filter TEXT DEFAULT NULL,
    p_redes_filter TEXT[] DEFAULT NULL,
    p_supervisor_filter TEXT DEFAULT NULL,
    p_sellers_filter TEXT[] DEFAULT NULL,
    p_suppliers_filter TEXT[] DEFAULT NULL,
    p_products_filter TEXT[] DEFAULT NULL,
    p_pasta_filter TEXT DEFAULT NULL,
    p_city_filter TEXT DEFAULT NULL,
    p_working_days INT DEFAULT 0
)
RETURNS JSONB AS $$
DECLARE
    result JSONB;
    current_month_start DATE;
    history_start_date DATE;
    history_end_date DATE;
    days_divisor INT;
BEGIN
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start
    FROM (
        SELECT dtped FROM data_detailed
        UNION ALL
        SELECT dtped FROM data_history
    ) AS all_dates;
    history_end_date := current_month_start - INTERVAL '1 day';
    history_start_date := current_month_start - INTERVAL '3 months';

    IF p_working_days > 0 THEN
        days_divisor := p_working_days;
    ELSE
        SELECT value::INT INTO days_divisor FROM data_metadata WHERE key = 'passed_working_days';
        days_divisor := GREATEST(days_divisor, 1);
    END IF;

    WITH
    filtered_clients AS (
        SELECT codigo_cliente FROM data_clients
        WHERE (p_rede_group_filter IS NULL)
           OR (p_rede_group_filter = 'sem_rede' AND (ramo IS NULL OR ramo = 'N/A'))
           OR (p_rede_group_filter = 'com_rede' AND (p_redes_filter IS NULL OR ramo = ANY(p_redes_filter)))
    ),
    sales_base AS (
        SELECT 'current' as type, s.produto, s.qtvenda_embalagem_master, s.dtped FROM data_detailed s
        JOIN filtered_clients fc ON s.codcli = fc.codigo_cliente
        WHERE (p_supervisor_filter IS NULL OR s.superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR s.nome = ANY(p_sellers_filter))
          AND (p_suppliers_filter IS NULL OR s.codfor = ANY(p_suppliers_filter))
          AND (p_products_filter IS NULL OR s.produto = ANY(p_products_filter))
          AND (p_pasta_filter IS NULL OR s.observacaofor = p_pasta_filter)
          AND (p_city_filter IS NULL OR s.cidade ILIKE p_city_filter)
          AND (p_filial_filter = 'ambas' OR s.filial = p_filial_filter)
        UNION ALL
        SELECT 'history' as type, s.produto, s.qtvenda_embalagem_master, s.dtped FROM data_history s
        JOIN filtered_clients fc ON s.codcli = fc.codigo_cliente
        WHERE s.dtped BETWEEN history_start_date AND history_end_date
          AND (p_supervisor_filter IS NULL OR s.superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR s.nome = ANY(p_sellers_filter))
          AND (p_suppliers_filter IS NULL OR s.codfor = ANY(p_suppliers_filter))
          AND (p_products_filter IS NULL OR s.produto = ANY(p_products_filter))
          AND (p_pasta_filter IS NULL OR s.observacaofor = p_pasta_filter)
          AND (p_city_filter IS NULL OR s.cidade ILIKE p_city_filter)
          AND (p_filial_filter = 'ambas' OR s.filial = p_filial_filter)
    ),
    product_list AS (
      SELECT DISTINCT produto FROM sales_base
    ),
    stock_agg AS (
        SELECT
            product_code,
            SUM(stock_qty) as stock
        FROM data_stock
        WHERE product_code IN (SELECT produto FROM product_list)
          AND (p_filial_filter = 'ambas' OR filial = p_filial_filter)
        GROUP BY product_code
    ),
    monthly_avg_agg AS (
        SELECT
            produto,
            COALESCE(SUM(qtvenda_embalagem_master) / 3.0, 0) as monthly_avg_sale
        FROM sales_base
        WHERE type = 'history'
        GROUP BY produto
    ),
    daily_avg_agg AS (
        SELECT
            produto,
            COALESCE(SUM(qtvenda_embalagem_master) / days_divisor, 0) as daily_avg_sale
        FROM sales_base
        GROUP BY produto
    ),
    current_sales_agg AS (
        SELECT
            produto,
            COALESCE(SUM(qtvenda_embalagem_master), 0) as current_month_sales
        FROM sales_base
        WHERE type = 'current'
        GROUP BY produto
    ),
    product_analysis AS (
        SELECT
            p.produto AS code,
            pd.descricao,
            pd.fornecedor,
            COALESCE(s.stock, 0) as stock,
            COALESCE(ma.monthly_avg_sale, 0) as monthly_avg_sale,
            COALESCE(da.daily_avg_sale, 0) as daily_avg_sale,
            (CASE WHEN COALESCE(da.daily_avg_sale, 0) > 0 THEN COALESCE(s.stock, 0) / da.daily_avg_sale ELSE 9999 END) as trend_days,
            COALESCE(cs.current_month_sales, 0) as current_month_sales_qty,
            (ma.monthly_avg_sale IS NULL OR ma.monthly_avg_sale = 0) as is_new
        FROM product_list p
        LEFT JOIN data_product_details pd ON p.produto = pd.code
        LEFT JOIN stock_agg s ON p.produto = s.product_code
        LEFT JOIN monthly_avg_agg ma ON p.produto = ma.produto
        LEFT JOIN daily_avg_agg da ON p.produto = da.produto
        LEFT JOIN current_sales_agg cs ON p.produto = cs.produto
        WHERE pd.code IS NOT NULL
          AND (COALESCE(s.stock, 0) > 0 OR COALESCE(cs.current_month_sales, 0) > 0 OR COALESCE(ma.monthly_avg_sale, 0) > 0)
    )
    SELECT jsonb_build_object(
        'main_table', (SELECT COALESCE(jsonb_agg(pa ORDER BY trend_days DESC), '[]'::jsonb) FROM product_analysis pa),
        'growth_table', (SELECT COALESCE(jsonb_agg(pa ORDER BY (current_month_sales_qty - monthly_avg_sale) DESC), '[]'::jsonb) FROM product_analysis pa WHERE current_month_sales_qty >= monthly_avg_sale AND monthly_avg_sale > 0),
        'decline_table', (SELECT COALESCE(jsonb_agg(pa ORDER BY (current_month_sales_qty - monthly_avg_sale) ASC), '[]'::jsonb) FROM product_analysis pa WHERE current_month_sales_qty < monthly_avg_sale),
        'new_products_table', (SELECT COALESCE(jsonb_agg(pa ORDER BY current_month_sales_qty DESC), '[]'::jsonb) FROM product_analysis pa WHERE is_new AND current_month_sales_qty > 0),
        'lost_products_table', (SELECT COALESCE(jsonb_agg(pa ORDER BY monthly_avg_sale DESC), '[]'::jsonb) FROM product_analysis pa WHERE NOT is_new AND current_month_sales_qty = 0)
    )
    INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- NOVA FUNÇÃO: get_detailed_orders para a nova visualização de Pedidos
CREATE OR REPLACE FUNCTION get_detailed_orders(
    p_supervisor_filter TEXT DEFAULT '',
    p_sellers_filter TEXT[] DEFAULT NULL,
    p_tipo_venda_filter TEXT[] DEFAULT NULL,
    p_fornecedor_filter TEXT DEFAULT '',
    p_codcli_filter TEXT DEFAULT '',
    p_posicao_filter TEXT DEFAULT '',
    p_rede_group_filter TEXT DEFAULT '',
    p_redes_filter TEXT[] DEFAULT NULL,
    page_number INT DEFAULT 1,
    page_size INT DEFAULT 50
)
RETURNS JSONB AS $$
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

-- ADIÇÃO DA NOVA FUNÇÃO get_city_view_data
-- CORREÇÃO: Adiciona DROP FUNCTION para permitir a renomeação de parâmetros na recriação.
DROP FUNCTION IF EXISTS get_city_view_data(TEXT, TEXT[], TEXT, TEXT[], TEXT, TEXT);
-- CORREÇÃO: Padroniza os parâmetros com prefixo p_ e defaults para strings vazias,
-- tornando a função mais robusta contra erros de 'parâmetro não encontrado' (400 Bad Request).
CREATE OR REPLACE FUNCTION get_city_view_data(
    p_supervisor_filter TEXT DEFAULT '',
    p_sellers_filter TEXT[] DEFAULT NULL,
    p_rede_group_filter TEXT DEFAULT '',
    p_redes_filter TEXT[] DEFAULT NULL,
    p_city_filter TEXT DEFAULT '',
    p_codcli_filter TEXT DEFAULT ''
)
RETURNS JSONB AS $$
DECLARE
    result JSONB;
    current_month_start DATE;
BEGIN
    -- Determina o primeiro dia do mês corrente com base na venda mais recente
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start FROM data_detailed;

    WITH
    -- 1. Filtra os clientes com base nos filtros de rede
    clients_base AS (
        SELECT c.codigo_cliente, c.ramo, c.rca1, c.rca2, c.cidade, c.bairro, c.fantasia, c.razaosocial, c.ultimacompra, c.datacadastro, c.bloqueio
        FROM data_clients c
        WHERE (p_rede_group_filter = '')
           OR (p_rede_group_filter = 'sem_rede' AND (c.ramo IS NULL OR c.ramo = 'N/A'))
           OR (p_rede_group_filter = 'com_rede' AND (p_redes_filter IS NULL OR c.ramo = ANY(p_redes_filter)))
    ),
    -- 2. Filtra as vendas do mês corrente com base em todos os filtros
    filtered_sales AS (
        SELECT s.codcli, s.cidade, s.bairro, s.vlvenda, s.observacaofor, s.tipovenda
        FROM data_detailed s
        JOIN clients_base cb ON s.codcli = cb.codigo_cliente -- Garante que as vendas são de clientes já filtrados pela rede
        WHERE s.dtped >= current_month_start
          AND (p_supervisor_filter = '' OR s.superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR s.nome = ANY(p_sellers_filter))
          AND (p_city_filter = '' OR s.cidade ILIKE p_city_filter)
          AND (p_codcli_filter = '' OR s.codcli = p_codcli_filter)
    ),
    -- 3. Agrega as vendas por cliente
    client_sales_agg AS (
        SELECT
            codcli,
            SUM(vlvenda) as total_faturamento,
            SUM(CASE WHEN observacaofor = 'PEPSICO' THEN vlvenda ELSE 0 END) as pepsico_total,
            SUM(CASE WHEN observacaofor = 'MULTIMARCAS' THEN vlvenda ELSE 0 END) as multimarcas_total
        FROM filtered_sales
        WHERE tipovenda IN ('1', '9')
        GROUP BY codcli
    ),
    -- 4. Classifica os clientes como ativos ou inativos
    client_status AS (
        SELECT
            c.codigo_cliente,
            c.fantasia,
            c.razaosocial,
            c.cidade,
            c.bairro,
            c.rca1,
            c.ultimacompra,
            c.datacadastro,
            COALESCE(csa.total_faturamento, 0) as total_faturamento,
            COALESCE(csa.pepsico_total, 0) as pepsico,
            COALESCE(csa.multimarcas_total, 0) as multimarcas,
            (csa.codcli IS NOT NULL AND csa.total_faturamento > 0) as is_active,
            (c.datacadastro::date >= current_month_start) as is_new
        FROM clients_base c
        LEFT JOIN client_sales_agg csa ON c.codigo_cliente = csa.codcli
        WHERE c.codigo_cliente <> '6720' AND c.bloqueio <> 'S'
          AND NOT (c.rca1 = '53' AND c.razaosocial NOT ILIKE '%AMERICANAS%')
    ),
    -- 5. Prepara os dados para a tabela de clientes ativos
    active_clients_table AS (
        SELECT COALESCE(jsonb_agg(act ORDER BY total_faturamento DESC), '[]'::jsonb) as data
        FROM client_status act
        WHERE is_active
    ),
    -- 6. Prepara os dados para a tabela de clientes inativos
    inactive_clients_table AS (
        SELECT COALESCE(jsonb_agg(ict ORDER BY ict.ultimacompra::date DESC), '[]'::jsonb) as data
        FROM client_status ict
        WHERE NOT is_active
    ),
    -- 7. Prepara os dados para o gráfico de Top 10
    top_10_chart AS (
        SELECT COALESCE(jsonb_agg(chart_data), '[]'::jsonb) as data
        FROM (
            SELECT
                CASE WHEN p_city_filter <> '' THEN bairro ELSE cidade END as label,
                SUM(vlvenda) as total
            FROM filtered_sales
            WHERE tipovenda IN ('1', '9')
            GROUP BY 1
            ORDER BY total DESC
            LIMIT 10
        ) chart_data
    )
    -- 8. Monta o JSON final
    SELECT jsonb_build_object(
        'total_faturamento', (SELECT SUM(vlvenda) FROM filtered_sales WHERE tipovenda IN ('1', '9')),
        'status_chart', (
            SELECT jsonb_build_object(
                'active', COUNT(*) FILTER (WHERE is_active),
                'inactive', COUNT(*) FILTER (WHERE NOT is_active)
            ) FROM client_status
        ),
        'top_10_chart', (SELECT data FROM top_10_chart),
        'active_clients', (SELECT data FROM active_clients_table),
        'inactive_clients', (SELECT data FROM inactive_clients_table)
    )
    INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- ========= FIM DO BLOCO DE CÓDIGO SQL =========

-- NOVA FUNÇÃO: get_weekly_view_data para a página de Acompanhamento Semanal
CREATE OR REPLACE FUNCTION get_weekly_view_data(
    p_supervisors_filter TEXT[] DEFAULT NULL,
    p_pasta_filter TEXT DEFAULT NULL
)
RETURNS JSONB AS $$
DECLARE
    result JSONB;
    current_month_start DATE;
    previous_month_start DATE;
    previous_month_end DATE;
BEGIN
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start
    FROM (
        SELECT dtped FROM data_detailed
        UNION ALL
        SELECT dtped FROM data_history
    ) AS all_dates;
    previous_month_start := current_month_start - INTERVAL '1 month';
    previous_month_end := current_month_start - INTERVAL '1 day';

    WITH
    -- 1. Vendas filtradas do mês atual
    current_sales AS (
        SELECT * FROM data_detailed
        WHERE dtped >= current_month_start
          AND (p_supervisors_filter IS NULL OR superv = ANY(p_supervisors_filter))
          AND (p_pasta_filter IS NULL OR observacaofor = p_pasta_filter)
    ),
    -- 2. Vendas filtradas do mês anterior (para o histórico)
    previous_month_sales AS (
        SELECT * FROM data_history
        WHERE dtped BETWEEN previous_month_start AND previous_month_end
          AND (p_supervisors_filter IS NULL OR superv = ANY(p_supervisors_filter))
          AND (p_pasta_filter IS NULL OR observacaofor = p_pasta_filter)
    ),
    -- 3. Total do mês
    total_revenue AS (
        SELECT COALESCE(SUM(vlvenda), 0) as total FROM current_sales WHERE tipovenda IN ('1', '9')
    ),
    -- 4. Vendas por semana e dia
    sales_by_week_day AS (
        SELECT
            -- Usa a fórmula ISO 8601 para a semana, mas ajustada
            EXTRACT(WEEK FROM dtped) - EXTRACT(WEEK FROM date_trunc('month', dtped)) + 1 as week_num,
            -- Segunda=1, Terça=2 ... Domingo=7
            EXTRACT(ISODOW FROM dtped) as day_of_week,
            SUM(vlvenda) as daily_total
        FROM current_sales
        WHERE tipovenda IN ('1', '9') AND EXTRACT(ISODOW FROM dtped) BETWEEN 1 AND 5
        GROUP BY 1, 2
    ),
    -- 5. Histórico de melhor dia por supervisor
    historical_best_day AS (
        SELECT
            superv,
            EXTRACT(ISODOW FROM dtped) as day_of_week,
            MAX(daily_total) as best_day_total
        FROM (
            SELECT superv, dtped, EXTRACT(ISODOW FROM dtped), SUM(vlvenda) as daily_total
            FROM previous_month_sales
            WHERE tipovenda IN ('1', '9') AND superv <> 'BALCAO'
            GROUP BY superv, dtped
        ) as daily_sales
        GROUP BY superv, day_of_week
    ),
    -- 6. Ranking de positivação
    positivacao_rank AS (
        SELECT nome, COUNT(DISTINCT codcli) as total
        FROM current_sales
        WHERE nome IS NOT NULL AND superv <> 'BALCAO'
        GROUP BY nome
        ORDER BY total DESC
        LIMIT 10
    ),
    -- 7. Ranking de Top Sellers
    top_sellers_rank AS (
        SELECT nome, SUM(vlvenda) as total
        FROM current_sales
        WHERE nome IS NOT NULL AND superv <> 'BALCAO' AND tipovenda IN ('1', '9')
        GROUP BY nome
        ORDER BY total DESC
        LIMIT 10
    ),
    -- 8. Ranking de Mix de produtos
    mix_rank_cte AS (
        SELECT
            nome,
            codcli,
            COUNT(DISTINCT produto) as mix
        FROM current_sales
        WHERE nome IS NOT NULL AND superv <> 'BALCAO'
        GROUP BY nome, codcli
    ),
    avg_mix_rank AS (
        SELECT
            nome,
            AVG(mix) as avg_mix
        FROM mix_rank_cte
        GROUP BY nome
        ORDER BY avg_mix DESC
        LIMIT 10
    )
    -- 9. Montagem final do JSON
    SELECT jsonb_build_object(
        'total_faturamento', (SELECT total FROM total_revenue),
        'sales_by_week_day', (SELECT COALESCE(jsonb_agg(swd), '[]'::jsonb) FROM sales_by_week_day swd),
        'historical_best_days', (SELECT COALESCE(jsonb_agg(hbd), '[]'::jsonb) FROM historical_best_day hbd),
        'positivacao_rank', (SELECT COALESCE(jsonb_agg(pr), '[]'::jsonb) FROM positivacao_rank pr),
        'top_sellers_rank', (SELECT COALESCE(jsonb_agg(tsr), '[]'::jsonb) FROM top_sellers_rank tsr),
        'mix_rank', (SELECT COALESCE(jsonb_agg(amr), '[]'::jsonb) FROM avg_mix_rank amr)
    )
    INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- NOVA FUNÇÃO: get_coverage_view_data para a página de Cobertura de Estoque
CREATE OR REPLACE FUNCTION get_coverage_view_data(
    p_supervisor_filter TEXT DEFAULT NULL,
    p_sellers_filter TEXT[] DEFAULT NULL,
    p_city_filter TEXT DEFAULT NULL,
    p_filial_filter TEXT DEFAULT 'ambas',
    p_suppliers_filter TEXT[] DEFAULT NULL,
    p_products_filter TEXT[] DEFAULT NULL,
    p_include_bonus BOOLEAN DEFAULT true
)
RETURNS JSONB AS $$
DECLARE
    result JSONB;
    current_month_start DATE;
    previous_month_start DATE;
    previous_month_end DATE;
    days_divisor INT;
BEGIN
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start
    FROM (
        SELECT dtped FROM data_detailed
        UNION ALL
        SELECT dtped FROM data_history
    ) AS all_dates;
    previous_month_start := current_month_start - INTERVAL '1 month';
    previous_month_end := current_month_start - INTERVAL '1 day';
    SELECT value::INT INTO days_divisor FROM data_metadata WHERE key = 'passed_working_days';
    days_divisor := GREATEST(days_divisor, 1);

    WITH
    -- 1. Base de clientes filtrada
    filtered_clients AS (
        SELECT DISTINCT c.codigo_cliente
        FROM data_clients c
        LEFT JOIN data_detailed s ON c.codigo_cliente = s.codcli AND s.dtped >= current_month_start
        WHERE (p_supervisor_filter = '' OR s.superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR s.nome = ANY(p_sellers_filter))
          AND (p_city_filter = '' OR c.cidade ILIKE p_city_filter)
          AND (p_filial_filter = 'ambas' OR s.filial = p_filial_filter)
    ),
    active_clients_for_coverage AS (
        SELECT codigo_cliente
        FROM data_clients
        WHERE codigo_cliente IN (SELECT codigo_cliente FROM filtered_clients)
          AND codigo_cliente <> '6720' AND bloqueio <> 'S'
          AND NOT (rca1 = '53' AND razaosocial NOT ILIKE '%AMERICANAS%')
    ),
    active_client_count_cte AS (
        SELECT COUNT(*) as count FROM active_clients_for_coverage
    ),
    -- 2. Produtos a serem analisados
    products_to_analyze AS (
        SELECT code FROM data_product_details
        WHERE (p_products_filter IS NULL OR code = ANY(p_products_filter))
          AND (p_suppliers_filter IS NULL OR codfor = ANY(p_suppliers_filter))
    ),
    -- 3. Vendas
    sales_data AS (
        SELECT produto, codcli, qtvenda_embalagem_master
        FROM data_detailed
        WHERE produto IN (SELECT code FROM products_to_analyze)
          AND codcli IN (SELECT codigo_cliente FROM active_clients_for_coverage)
          AND dtped >= current_month_start
          AND (vlvenda > 0 OR (p_include_bonus AND vlbonific > 0))
        UNION ALL
        SELECT produto, codcli, qtvenda_embalagem_master
        FROM data_history
        WHERE produto IN (SELECT code FROM products_to_analyze)
          AND codcli IN (SELECT codigo_cliente FROM active_clients_for_coverage)
          AND dtped BETWEEN previous_month_start AND previous_month_end
          AND (vlvenda > 0 OR (p_include_bonus AND vlbonific > 0))
    ),
    -- 4. Análise de cobertura
    coverage_analysis AS (
        SELECT
            p.code AS product_code,
            pd.descricao,
            COALESCE(s.stock, 0) as stock,
            (SELECT COUNT(DISTINCT sd.codcli) FROM sales_data sd WHERE sd.produto = p.code AND sd.dtped >= current_month_start) as pdvs_current,
            (SELECT COUNT(DISTINCT sd.codcli) FROM sales_data sd WHERE sd.produto = p.code AND sd.dtped < current_month_start) as pdvs_previous,
            (SELECT SUM(sd.qtvenda_embalagem_master) FROM sales_data sd WHERE sd.produto = p.code) / days_divisor as daily_avg_sale
        FROM products_to_analyze p
        LEFT JOIN data_product_details pd ON p.code = pd.code
        LEFT JOIN (
            SELECT product_code, SUM(stock_qty) as stock
            FROM data_stock
            WHERE (p_filial_filter = 'ambas' OR filial = p_filial_filter)
            GROUP BY product_code
        ) s ON p.code = s.product_code
    )
    -- 5. Montagem do JSON
    SELECT jsonb_build_object(
        'kpis', jsonb_build_object(
            'active_clients', (SELECT count FROM active_client_count_cte),
            'selection_coverage_current', (SELECT COUNT(DISTINCT codcli) FROM sales_data WHERE dtped >= current_month_start),
            'selection_coverage_previous', (SELECT COUNT(DISTINCT codcli) FROM sales_data WHERE dtped < current_month_start)
        ),
        'table_data', (
            SELECT COALESCE(jsonb_agg(ca ORDER BY (CASE WHEN ca.daily_avg_sale > 0 THEN ca.stock / ca.daily_avg_sale ELSE 9999 END) DESC), '[]'::jsonb)
            FROM (
                SELECT *,
                       (CASE WHEN daily_avg_sale > 0 THEN stock / daily_avg_sale ELSE 9999 END) as trend_days,
                       (CASE WHEN (SELECT count FROM active_client_count_cte) > 0 THEN (pdvs_current::decimal / (SELECT count FROM active_client_count_cte)) * 100 ELSE 0 END) as coverage_pdvs_percent
                FROM coverage_analysis
                WHERE stock > 0 OR pdvs_current > 0 OR pdvs_previous > 0
            ) ca
        )
    )
    INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- NOVA FUNÇÃO: get_innovations_view_data para a página de Análise de Inovações
CREATE OR REPLACE FUNCTION get_innovations_view_data(
    p_supervisor_filter TEXT DEFAULT NULL,
    p_sellers_filter TEXT[] DEFAULT NULL,
    p_city_filter TEXT DEFAULT NULL,
    p_filial_filter TEXT DEFAULT 'ambas',
    p_suppliers_filter TEXT[] DEFAULT NULL,
    p_products_filter TEXT[] DEFAULT NULL,
    p_include_bonus BOOLEAN DEFAULT true
)
RETURNS JSONB AS $$
DECLARE
    result JSONB;
    current_month_start DATE;
    previous_month_start DATE;
    previous_month_end DATE;
    active_client_count INT;
BEGIN
    -- Adiciona uma guarda para retornar uma estrutura vazia se nenhum produto for selecionado
    IF p_products_filter IS NULL OR array_length(p_products_filter, 1) IS NULL THEN
        SELECT jsonb_build_object(
            'kpis', jsonb_build_object(
                'active_clients', 0,
                'selection_coverage_current', 0,
                'selection_coverage_previous', 0,
                'bonus_coverage_current', 0,
                'bonus_coverage_previous', 0
            ),
            'table_data', '[]'::jsonb
        )
        INTO result;
        RETURN result;
    END IF;

    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start
    FROM (
        SELECT dtped FROM data_detailed
        UNION ALL
        SELECT dtped FROM data_history
    ) AS all_dates;
    previous_month_start := current_month_start - INTERVAL '1 month';
    previous_month_end := current_month_start - INTERVAL '1 day';

    WITH
    -- 1. Base de clientes filtrada por RCA, Supervisor, Vendedor, Cidade, Filial
    filtered_clients AS (
        SELECT DISTINCT c.codigo_cliente
        FROM data_clients c
        LEFT JOIN data_detailed s ON c.codigo_cliente = s.codcli AND s.dtped >= current_month_start
        WHERE (p_supervisor_filter = '' OR s.superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR s.nome = ANY(p_sellers_filter))
          AND (p_city_filter = '' OR c.cidade ILIKE p_city_filter)
          AND (p_filial_filter = 'ambas' OR s.filial = p_filial_filter)
    ),
    -- 2. Aplica a regra de negócio de cobertura para obter a base de clientes ativa
    active_clients_for_coverage AS (
        SELECT codigo_cliente
        FROM data_clients
        WHERE codigo_cliente IN (SELECT codigo_cliente FROM filtered_clients)
          AND codigo_cliente <> '6720' AND bloqueio <> 'S'
          AND NOT (rca1 = '53' AND razaosocial NOT ILIKE '%AMERICANAS%')
    ),
    -- 3. Contagem de clientes ativos para cálculos de percentual
    active_client_count_cte AS (
        SELECT COUNT(*) as count FROM active_clients_for_coverage
    ),
    -- 4. CTE base de produtos a serem analisados
    products_to_analyze AS (
        SELECT unnest(p_products_filter) as product_code
    ),
    -- 5. Vendas (atuais e bônus) dos produtos no mês corrente
    current_sales AS (
        SELECT
            s.produto,
            s.codcli,
            (s.vlvenda > 0 AND s.tipovenda IN ('1','9')) as is_sale,
            (s.vlbonific > 0) as is_bonus
        FROM data_detailed s
        WHERE s.produto = ANY(p_products_filter)
          AND s.codcli IN (SELECT codigo_cliente FROM active_clients_for_coverage)
          AND s.dtped >= current_month_start
    ),
    -- 6. Vendas (atuais e bônus) dos produtos no mês anterior
    previous_sales AS (
        SELECT
            s.produto,
            s.codcli,
            (s.vlvenda > 0 AND s.tipovenda IN ('1','9')) as is_sale,
            (s.vlbonific > 0) as is_bonus
        FROM data_history s
        WHERE s.produto = ANY(p_products_filter)
          AND s.codcli IN (SELECT codigo_cliente FROM active_clients_for_coverage)
          AND s.dtped BETWEEN previous_month_start AND previous_month_end
    ),
    -- 7. Análise por produto
    product_analysis AS (
        SELECT
            p.product_code,
            -- Contagem de clientes únicos que compraram (com ou sem bônus) no mês atual
            COUNT(DISTINCT cs.codcli) FILTER (WHERE cs.is_sale OR (p_include_bonus AND cs.is_bonus)) as clients_current,
            -- Contagem de clientes únicos que compraram (com ou sem bônus) no mês anterior
            COUNT(DISTINCT ps.codcli) FILTER (WHERE ps.is_sale OR (p_include_bonus AND ps.is_bonus)) as clients_previous
        FROM products_to_analyze p
        LEFT JOIN current_sales cs ON p.product_code = cs.produto
        LEFT JOIN previous_sales ps ON p.product_code = ps.produto
        GROUP BY p.product_code
    ),
    -- 8. Tabela final de dados com cálculos de cobertura
    final_table_data AS (
        SELECT
            pa.product_code,
            pd.descricao,
            s.stock,
            acc.count as total_clients,
            pa.clients_current,
            pa.clients_previous,
            (CASE WHEN acc.count > 0 THEN (pa.clients_current::decimal / acc.count) * 100 ELSE 0 END) as coverage_current,
            (CASE WHEN acc.count > 0 THEN (pa.clients_previous::decimal / acc.count) * 100 ELSE 0 END) as coverage_previous
        FROM product_analysis pa
        JOIN active_client_count_cte acc ON true
        LEFT JOIN data_product_details pd ON pa.product_code = pd.code
        LEFT JOIN (
            SELECT product_code, SUM(stock_qty) as stock
            FROM data_stock
            WHERE (p_filial_filter = 'ambas' OR filial = p_filial_filter)
            GROUP BY product_code
        ) s ON pa.product_code = s.product_code
    )
    -- 9. Monta o JSON final
    SELECT jsonb_build_object(
        'kpis', jsonb_build_object(
            'active_clients', (SELECT count FROM active_client_count_cte),
            'selection_coverage_current', (SELECT COUNT(DISTINCT codcli) FROM current_sales WHERE is_sale OR (p_include_bonus AND is_bonus)),
            'selection_coverage_previous', (SELECT COUNT(DISTINCT codcli) FROM previous_sales WHERE is_sale OR (p_include_bonus AND is_bonus)),
            'bonus_coverage_current', (SELECT COUNT(DISTINCT codcli) FROM current_sales WHERE is_bonus),
            'bonus_coverage_previous', (SELECT COUNT(DISTINCT codcli) FROM previous_sales WHERE is_bonus)
        ),
        'table_data', (SELECT COALESCE(jsonb_agg(ftd ORDER BY coverage_current DESC), '[]'::jsonb) FROM final_table_data ftd)
    )
    INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;
