
-- ============================================================================
-- 6. FUNÇÃO DE DASHBOARD INICIAL (RECRIADA)
-- ============================================================================
create or replace function get_initial_dashboard_data(
    supervisor_filter text DEFAULT NULL,
    pasta_filter text DEFAULT NULL,
    sellers_filter text[] DEFAULT NULL,
    fornecedor_filter text DEFAULT NULL,
    posicao_filter text DEFAULT NULL,
    codcli_filter text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
AS $function$
DECLARE
    -- Variables
    v_kpis jsonb;
    v_charts jsonb;
    v_filters jsonb;

    -- Dates
    v_current_month_start date;
    v_history_start_date date;
    v_last_sale_date date;

    -- Working days
    v_passed_working_days int;
    v_total_working_days_month int;

    -- Trend
    v_current_fat numeric;
    v_history_avg_fat numeric;
    v_trend_fat numeric;

BEGIN
    -- 1. Date Calculations
    SELECT date_trunc('month', MAX(dtped))::DATE, MAX(dtped)::DATE
    INTO v_current_month_start, v_last_sale_date
    FROM data_detailed;

    IF v_current_month_start IS NULL THEN
        -- Fallback if table is empty
        v_current_month_start := date_trunc('month', CURRENT_DATE);
        v_last_sale_date := CURRENT_DATE;
    END IF;

    v_history_start_date := v_current_month_start - INTERVAL '3 months';

    -- Calculate working days for Trend Projection
    -- Count M-F in current month up to last sale date
    SELECT COUNT(*) INTO v_passed_working_days
    FROM generate_series(v_current_month_start, v_last_sale_date, '1 day'::interval) d
    WHERE extract(isodow from d) < 6;

    -- Total working days in current month
    SELECT COUNT(*) INTO v_total_working_days_month
    FROM generate_series(
        v_current_month_start,
        (v_current_month_start + INTERVAL '1 month' - INTERVAL '1 day')::date,
        '1 day'::interval
    ) d
    WHERE extract(isodow from d) < 6;

    IF v_passed_working_days IS NULL OR v_passed_working_days = 0 THEN v_passed_working_days := 1; END IF;
    IF v_total_working_days_month IS NULL OR v_total_working_days_month = 0 THEN v_total_working_days_month := 1; END IF;

    -- 2. Build Filtered Dataset (Temporary Table or CTE logic in queries)

    -- 3. Calculate KPIs
    SELECT jsonb_build_object(
        'total_faturamento', COALESCE(SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0 END), 0),
        'total_peso', COALESCE(SUM(totpesoliq), 0),
        'positivacao_count', COUNT(DISTINCT codcli) FILTER (WHERE vlvenda > 0),
        'total_skus', COUNT(DISTINCT produto) FILTER (WHERE vlvenda > 0),
        -- Placeholder for coverage base, updated below
        'total_clients_for_coverage', 0
    ) INTO v_kpis
    FROM data_detailed d
    WHERE (supervisor_filter IS NULL OR d.superv = supervisor_filter)
      AND (pasta_filter IS NULL OR d.observacaofor = pasta_filter)
      AND (sellers_filter IS NULL OR d.nome = ANY(sellers_filter))
      AND (fornecedor_filter IS NULL OR d.codfor = fornecedor_filter)
      AND (posicao_filter IS NULL OR d.posicao = posicao_filter)
      AND (codcli_filter IS NULL OR d.codcli = codcli_filter);

    -- Update total_clients_for_coverage
    -- Base: Clients active in last 3 months (Detailed + History)
    -- Filtering applies to clients matching the criteria
    WITH active_base AS (
        SELECT codcli FROM data_detailed d
        WHERE (supervisor_filter IS NULL OR d.superv = supervisor_filter)
          AND (pasta_filter IS NULL OR d.observacaofor = pasta_filter)
          AND (sellers_filter IS NULL OR d.nome = ANY(sellers_filter))
          AND (fornecedor_filter IS NULL OR d.codfor = fornecedor_filter)
          AND (codcli_filter IS NULL OR d.codcli = codcli_filter)
        UNION
        SELECT codcli FROM data_history h
        WHERE (supervisor_filter IS NULL OR h.superv = supervisor_filter)
          AND (pasta_filter IS NULL OR h.observacaofor = pasta_filter)
          AND (sellers_filter IS NULL OR h.nome = ANY(sellers_filter))
          AND (fornecedor_filter IS NULL OR h.codfor = fornecedor_filter)
          AND (codcli_filter IS NULL OR h.codcli = codcli_filter)
    )
    SELECT jsonb_set(v_kpis, '{total_clients_for_coverage}', to_jsonb(COUNT(DISTINCT codcli)))
    INTO v_kpis
    FROM active_base;

    -- 4. Calculate Charts

    -- 4.3 Trend Data
    -- Current Fat
    SELECT COALESCE(SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0 END), 0)
    INTO v_current_fat
    FROM data_detailed d
    WHERE (supervisor_filter IS NULL OR d.superv = supervisor_filter)
      AND (pasta_filter IS NULL OR d.observacaofor = pasta_filter)
      AND (sellers_filter IS NULL OR d.nome = ANY(sellers_filter))
      AND (fornecedor_filter IS NULL OR d.codfor = fornecedor_filter)
      AND (posicao_filter IS NULL OR d.posicao = posicao_filter)
      AND (codcli_filter IS NULL OR d.codcli = codcli_filter);

    -- History Avg Fat
    SELECT COALESCE(SUM(CASE WHEN tipovenda IN ('1', '9') THEN vlvenda ELSE 0 END), 0) / 3.0
    INTO v_history_avg_fat
    FROM data_history h
    WHERE (supervisor_filter IS NULL OR h.superv = supervisor_filter)
      AND (pasta_filter IS NULL OR h.observacaofor = pasta_filter)
      AND (sellers_filter IS NULL OR h.nome = ANY(sellers_filter))
      AND (fornecedor_filter IS NULL OR h.codfor = fornecedor_filter)
      AND (codcli_filter IS NULL OR h.codcli = codcli_filter);

    v_trend_fat := CASE
        WHEN v_passed_working_days > 0 THEN (v_current_fat / v_passed_working_days) * v_total_working_days_month
        ELSE 0
    END;

    v_charts := jsonb_build_object(
        'sales_by_supervisor', (
            SELECT COALESCE(jsonb_agg(t), '[]'::jsonb) FROM (
                SELECT superv, SUM(vlvenda) as total_faturamento
                FROM data_detailed d
                WHERE tipovenda IN ('1', '9')
                  AND (supervisor_filter IS NULL OR d.superv = supervisor_filter)
                  AND (pasta_filter IS NULL OR d.observacaofor = pasta_filter)
                  AND (sellers_filter IS NULL OR d.nome = ANY(sellers_filter))
                  AND (fornecedor_filter IS NULL OR d.codfor = fornecedor_filter)
                  AND (posicao_filter IS NULL OR d.posicao = posicao_filter)
                  AND (codcli_filter IS NULL OR d.codcli = codcli_filter)
                GROUP BY superv
                ORDER BY total_faturamento DESC
            ) t
        ),
        'sales_by_pasta', (
             SELECT COALESCE(jsonb_agg(t), '[]'::jsonb) FROM (
                SELECT observacaofor, SUM(vlvenda) as total_faturamento
                FROM data_detailed d
                WHERE tipovenda IN ('1', '9')
                  AND (supervisor_filter IS NULL OR d.superv = supervisor_filter)
                  AND (pasta_filter IS NULL OR d.observacaofor = pasta_filter)
                  AND (sellers_filter IS NULL OR d.nome = ANY(sellers_filter))
                  AND (fornecedor_filter IS NULL OR d.codfor = fornecedor_filter)
                  AND (posicao_filter IS NULL OR d.posicao = posicao_filter)
                  AND (codcli_filter IS NULL OR d.codcli = codcli_filter)
                GROUP BY observacaofor
                ORDER BY total_faturamento DESC
            ) t
        ),
        'trend', jsonb_build_object(
            'avg_revenue', v_history_avg_fat,
            'trend_revenue', v_trend_fat
        ),
        'top_10_products_faturamento', (
            SELECT COALESCE(jsonb_agg(t), '[]'::jsonb) FROM (
                SELECT d.produto, d.descricao, SUM(d.vlvenda) as faturamento
                FROM data_detailed d
                WHERE d.tipovenda IN ('1', '9')
                  AND (supervisor_filter IS NULL OR d.superv = supervisor_filter)
                  AND (pasta_filter IS NULL OR d.observacaofor = pasta_filter)
                  AND (sellers_filter IS NULL OR d.nome = ANY(sellers_filter))
                  AND (fornecedor_filter IS NULL OR d.codfor = fornecedor_filter)
                  AND (posicao_filter IS NULL OR d.posicao = posicao_filter)
                  AND (codcli_filter IS NULL OR d.codcli = codcli_filter)
                GROUP BY d.produto, d.descricao
                ORDER BY faturamento DESC
                LIMIT 10
            ) t
        ),
        'top_10_products_peso', (
            SELECT COALESCE(jsonb_agg(t), '[]'::jsonb) FROM (
                SELECT d.produto, d.descricao, SUM(d.totpesoliq) as peso
                FROM data_detailed d
                WHERE (supervisor_filter IS NULL OR d.superv = supervisor_filter)
                  AND (pasta_filter IS NULL OR d.observacaofor = pasta_filter)
                  AND (sellers_filter IS NULL OR d.nome = ANY(sellers_filter))
                  AND (fornecedor_filter IS NULL OR d.codfor = fornecedor_filter)
                  AND (posicao_filter IS NULL OR d.posicao = posicao_filter)
                  AND (codcli_filter IS NULL OR d.codcli = codcli_filter)
                GROUP BY d.produto, d.descricao
                ORDER BY peso DESC
                LIMIT 10
            ) t
        )
    );

    -- 5. Calculate Filters (Distinct Lists)
    -- Uses full dataset to populate options
    v_filters := jsonb_build_object(
        'supervisors', (SELECT jsonb_agg(DISTINCT superv) FROM data_detailed WHERE superv IS NOT NULL),
        'suppliers', (
            SELECT jsonb_agg(DISTINCT jsonb_build_object('codfor', codfor, 'fornecedor', fornecedor))
            FROM data_product_details
            WHERE codfor IS NOT NULL
        ),
        'sellers', (SELECT jsonb_agg(DISTINCT nome) FROM data_detailed WHERE nome IS NOT NULL),
        'sale_types', (SELECT jsonb_agg(DISTINCT tipovenda) FROM data_detailed WHERE tipovenda IS NOT NULL)
    );

    -- 6. Return Result
    RETURN jsonb_build_object(
        'kpis', v_kpis,
        'charts', v_charts,
        'filters', v_filters
    );

END;
$function$;
