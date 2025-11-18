-- Ativa a extensão pg_stat_statements para monitorizar o desempenho das consultas.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Otimização 1: Índice composto principal para filtros comuns.
CREATE INDEX IF NOT EXISTS idx_data_detailed_filters ON data_detailed (superv, nome, codcli);

-- Otimização 2: Índices para colunas frequentemente usadas em GROUP BY e filtros individuais.
CREATE INDEX IF NOT EXISTS idx_data_detailed_dtped ON data_detailed (dtped);
CREATE INDEX IF NOT EXISTS idx_data_detailed_observacaofor ON data_detailed (observacaofor);
CREATE INDEX IF NOT EXISTS idx_data_detailed_codfor ON data_detailed (codfor);
CREATE INDEX IF NOT EXISTS idx_data_detailed_posicao ON data_detailed (posicao);

-- Otimização 3: Índice na tabela de histórico.
CREATE INDEX IF NOT EXISTS idx_data_history_dtped ON data_history (dtped);

-- Função RPC principal para buscar dados agregados do dashboard.
-- Esta versão foi reescrita para performance, usando CTEs granulares para evitar
-- múltiplas varreduras da tabela de vendas filtrada.
CREATE OR REPLACE FUNCTION get_initial_dashboard_data(
    supervisor_filter TEXT DEFAULT NULL,
    pasta_filter TEXT DEFAULT NULL,
    sellers_filter TEXT[] DEFAULT NULL,
    fornecedor_filter TEXT DEFAULT NULL,
    posicao_filter TEXT DEFAULT NULL,
    codcli_filter TEXT DEFAULT NULL
)
RETURNS JSONB AS $$
DECLARE
    result JSONB;
BEGIN
    WITH filtered_sales AS (
        -- CTE base para selecionar e filtrar os dados de vendas do mês atual.
        SELECT
            s.vlvenda,
            s.totpesoliq,
            s.vlbonific,
            s.codcli,
            s.superv,
            s.observacaofor,
            s.produto,
            s.descricao
        FROM data_detailed s
        WHERE
            (supervisor_filter IS NULL OR s.superv = supervisor_filter)
        AND (pasta_filter IS NULL OR s.observacaofor = pasta_filter)
        AND (sellers_filter IS NULL OR s.nome = ANY(sellers_filter))
        AND (fornecedor_filter IS NULL OR s.codfor = fornecedor_filter)
        AND (posicao_filter IS NULL OR s.posicao = posicao_filter)
        AND (codcli_filter IS NULL OR s.codcli = codcli_filter)
    ),
    kpi_agg AS (
        -- Agrega todos os KPIs em uma única passagem sobre os dados filtrados.
        SELECT
            COALESCE(SUM(s.vlvenda), 0) AS total_faturamento,
            COALESCE(SUM(s.totpesoliq), 0) AS total_peso,
            COUNT(DISTINCT CASE WHEN s.vlvenda > 0 OR s.vlbonific > 0 THEN s.codcli END) AS positivacao_count,
            COUNT(CASE WHEN s.vlvenda > 0 OR s.vlbonific > 0 THEN 1 END) AS total_skus
        FROM filtered_sales s
    ),
    total_clients_agg AS (
        -- Subconsulta independente para o total de clientes da cobertura (não muda com os filtros).
        SELECT COUNT(DISTINCT codigo_cliente) AS total_clients_for_coverage
        FROM data_clients
        WHERE (razaosocial ILIKE '%AMERICANAS%' OR rca1 <> '53')
    ),
    sales_by_supervisor_agg AS (
        -- Agregação para o gráfico de vendas por supervisor.
        SELECT COALESCE(jsonb_agg(json_data ORDER BY total_faturamento DESC), '[]'::jsonb) AS data
        FROM (
            SELECT s.superv, SUM(s.vlvenda) AS total_faturamento
            FROM filtered_sales s
            GROUP BY s.superv
        ) AS json_data
    ),
    sales_by_pasta_agg AS (
        -- Agregação para o gráfico de vendas por pasta/categoria.
        SELECT COALESCE(jsonb_agg(json_data ORDER BY total_faturamento ASC), '[]'::jsonb) AS data
        FROM (
            SELECT s.observacaofor, SUM(s.vlvenda) AS total_faturamento
            FROM filtered_sales s
            GROUP BY s.observacaofor
        ) AS json_data
    ),
    top_10_products_faturamento_agg AS (
        -- Agregação para o top 10 produtos por faturamento.
        SELECT COALESCE(jsonb_agg(json_data), '[]'::jsonb) AS data
        FROM (
            SELECT s.produto, s.descricao, SUM(s.vlvenda) AS faturamento
            FROM filtered_sales s
            GROUP BY s.produto, s.descricao
            ORDER BY faturamento DESC
            LIMIT 10
        ) AS json_data
    ),
    top_10_products_peso_agg AS (
        -- Agregação para o top 10 produtos por peso.
        SELECT COALESCE(jsonb_agg(json_data), '[]'::jsonb) AS data
        FROM (
            SELECT s.produto, s.descricao, SUM(s.totpesoliq) AS peso
            FROM filtered_sales s
            GROUP BY s.produto, s.descricao
            ORDER BY peso DESC
            LIMIT 10
        ) AS json_data
    ),
    history_trend_agg AS (
        -- Subconsulta independente para a média de faturamento histórico para o gráfico de tendência.
        SELECT AVG(monthly_revenue) as avg_revenue
        FROM (
            SELECT SUM(vlvenda) AS monthly_revenue
            FROM data_history
            WHERE dtped >= date_trunc('month', NOW()) - INTERVAL '3 months'
              AND dtped < date_trunc('month', NOW())
            GROUP BY date_trunc('month', dtped)
        ) AS monthly_data
    ),
    filters_agg AS (
        -- Agrega dados para os filtros da UI. Executado de forma independente para sempre mostrar todas as opções.
        SELECT
            (SELECT jsonb_agg(DISTINCT superv) FROM data_detailed WHERE superv IS NOT NULL) AS supervisors,
            (SELECT jsonb_agg(DISTINCT nome) FROM data_detailed WHERE nome IS NOT NULL) AS sellers,
            (SELECT jsonb_agg(DISTINCT jsonb_build_object('codfor', codfor, 'fornecedor', fornecedor)) FROM data_detailed WHERE codfor IS NOT NULL AND fornecedor IS NOT NULL) AS suppliers
    ),
    metadata AS (
        -- Busca metadados essenciais.
        SELECT
            (SELECT value FROM data_metadata WHERE key = 'last_update' LIMIT 1) AS last_update,
            (SELECT value::INTEGER FROM data_metadata WHERE key = 'passed_working_days' LIMIT 1) AS passed_working_days
    )
    -- Monta o resultado final combinando os JSONs de cada CTE.
    SELECT jsonb_build_object(
        'kpis', (
            SELECT jsonb_build_object(
                'total_faturamento', k.total_faturamento,
                'total_peso', k.total_peso,
                'positivacao_count', k.positivacao_count,
                'total_skus', k.total_skus,
                'total_clients_for_coverage', tc.total_clients_for_coverage
            )
            FROM kpi_agg k, total_clients_agg tc
        ),
        'charts', (
            SELECT jsonb_build_object(
                'sales_by_supervisor', s_sup.data,
                'sales_by_pasta', s_pasta.data,
                'top_10_products_faturamento', top_fat.data,
                'top_10_products_peso', top_peso.data,
                'trend', jsonb_build_object(
                    'avg_revenue', h_trend.avg_revenue,
                    'trend_revenue', (SELECT k.total_faturamento FROM kpi_agg k) / GREATEST((SELECT m.passed_working_days FROM metadata m), 1) * 22
                )
            )
            FROM sales_by_supervisor_agg s_sup,
                 sales_by_pasta_agg s_pasta,
                 top_10_products_faturamento_agg top_fat,
                 top_10_products_peso_agg top_peso,
                 history_trend_agg h_trend
        ),
        'filters', (SELECT to_jsonb(f) FROM filters_agg f),
        'last_update', (SELECT last_update FROM metadata),
        'passed_working_days', (SELECT passed_working_days FROM metadata)
    )
    INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Função RPC para buscar os dados da tela de Comparativo.
-- Similar à função do dashboard, ela pré-agrega os dados no servidor
-- para evitar a transferência e processamento de grandes volumes de dados no cliente.
CREATE OR REPLACE FUNCTION get_comparison_data(
    p_supervisor_filter TEXT DEFAULT NULL,
    p_sellers_filter TEXT[] DEFAULT NULL,
    p_suppliers_filter TEXT[] DEFAULT NULL,
    p_products_filter TEXT[] DEFAULT NULL,
    p_pasta_filter TEXT DEFAULT NULL,
    p_city_filter TEXT DEFAULT NULL,
    p_filial_filter TEXT DEFAULT 'ambas',
    p_rede_group_filter TEXT DEFAULT NULL,
    p_redes_filter TEXT[] DEFAULT NULL
)
RETURNS JSONB AS $$
DECLARE
    result JSONB;
    current_month_start DATE;
    history_start_date DATE;
    history_end_date DATE;
BEGIN
    -- Determina os intervalos de data. O histórico são os 3 meses completos antes do mês atual.
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start FROM data_detailed;
    history_end_date := current_month_start - 1;
    history_start_date := current_month_start - INTERVAL '3 months';

    WITH
    -- 1. Filtra a base de clientes de acordo com os filtros de rede.
    filtered_clients AS (
        SELECT codigo_cliente FROM data_clients
        WHERE
            (p_rede_group_filter IS NULL) OR
            (p_rede_group_filter = 'sem_rede' AND (ramo IS NULL OR ramo = 'N/A')) OR
            (p_rede_group_filter = 'com_rede' AND (p_redes_filter IS NULL OR ramo = ANY(p_redes_filter)))
    ),
    -- 2. Filtra as vendas do mês atual com base em todos os filtros aplicados.
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
    -- 3. Filtra as vendas históricas (últimos 3 meses) com os mesmos filtros.
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
    -- 4. Cálculos de KPI
    -- KPIs do mês atual
    kpis_current AS (
        SELECT
            COALESCE(SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0 END), 0) AS total_fat,
            COALESCE(SUM(totpesoliq), 0) AS total_peso,
            COUNT(DISTINCT codcli) AS total_clients
        FROM current_sales
    ),
    -- Agregações mensais do histórico para calcular a média corretamente
    kpis_history_monthly AS (
        SELECT
            date_trunc('month', dtped) as month,
            COALESCE(SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0 END), 0) AS total_fat,
            COALESCE(SUM(totpesoliq), 0) AS total_peso,
            COUNT(DISTINCT codcli) AS total_clients
        FROM history_sales
        GROUP BY 1
    ),
    -- Média dos KPIs do histórico
    kpis_history_avg AS (
        SELECT
            COALESCE(AVG(total_fat), 0) AS avg_fat,
            COALESCE(AVG(total_peso), 0) AS avg_peso,
            COALESCE(AVG(total_clients), 0) AS avg_clients
        FROM kpis_history_monthly
    ),
    -- 5. Dados para Gráficos
    -- Gera as semanas do mês atual para o gráfico
    week_series AS (
      SELECT
        n as week_num,
        (date_trunc('week', current_month_start)::date + ((n-1) || ' weeks')::interval) as week_start,
        (date_trunc('week', current_month_start)::date + (n || ' weeks')::interval - '1 day'::interval) as week_end
      FROM generate_series(1, 6) n
      WHERE (date_trunc('week', current_month_start)::date + ((n-1) || ' weeks')::interval) < (current_month_start + '1 month'::interval)
    ),
    -- Faturamento semanal do mês atual
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
                    -- Média histórica para a mesma janela de semana nos 3 meses anteriores
                    'history_avg_faturamento', (
                        SELECT COALESCE(SUM(CASE WHEN hs.tipovenda IN ('1', '9') THEN hs.vlvenda ELSE 0 END) / 3.0, 0)
                        FROM history_sales hs
                        WHERE (hs.dtped BETWEEN (ws.week_start - '1 month'::interval) AND (ws.week_end - '1 month'::interval))
                           OR (hs.dtped BETWEEN (ws.week_start - '2 months'::interval) AND (ws.week_end - '2 months'::interval))
                           OR (hs.dtped BETWEEN (ws.week_start - '3 months'::interval) AND (ws.week_end - '3 months'::interval))
                    )
                ) ORDER BY ws.week_num
            ), '[]'::jsonb) AS data
        FROM week_series ws
    ),
    -- 6. Dados para a Tabela de Supervisores
    supervisor_comparison_table AS (
        SELECT COALESCE(jsonb_agg(t ORDER BY current_faturamento DESC), '[]'::jsonb) AS data
        FROM (
            SELECT
                s.superv,
                COALESCE(SUM(CASE WHEN s.tipovenda IN ('1', '9') THEN s.vlvenda ELSE 0 END), 0) AS current_faturamento,
                (
                    SELECT COALESCE(SUM(CASE WHEN hs.tipovenda IN ('1', '9') THEN hs.vlvenda ELSE 0 END) / 3.0, 0)
                    FROM history_sales hs
                    WHERE hs.superv = s.superv
                ) AS history_avg_faturamento
            FROM current_sales s
            WHERE s.superv IS NOT NULL
            GROUP BY s.superv
        ) t
    )
    -- Montagem final do JSON de retorno
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
