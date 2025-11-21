-- Atualização da função get_stock_view_data para incluir filtro de cidade e corrigir comportamento
DROP FUNCTION IF EXISTS get_stock_view_data(text, text[], text[], text[], text, text[], text, integer);

CREATE OR REPLACE FUNCTION get_stock_view_data (
  p_supervisor_filter TEXT DEFAULT '',
  p_sellers_filter text[] DEFAULT NULL,
  p_suppliers_filter text[] DEFAULT NULL,
  p_products_filter text[] DEFAULT NULL,
  p_rede_group_filter TEXT DEFAULT '',
  p_redes_filter text[] DEFAULT NULL,
  p_filial_filter TEXT DEFAULT 'ambas',
  p_city_filter TEXT DEFAULT '', -- Novo parâmetro adicionado
  p_custom_days INTEGER DEFAULT 0
) RETURNS JSONB AS $$
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
        WHERE (p_rede_group_filter = '' OR
              (p_rede_group_filter = 'sem_rede' AND (c.ramo IS NULL OR c.ramo = 'N/A')) OR
              (p_rede_group_filter = 'com_rede' AND (p_redes_filter IS NULL OR c.ramo = ANY(p_redes_filter))))
          AND (p_city_filter = '' OR c.cidade ILIKE p_city_filter) -- Filtro de Cidade Aplicado
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
    daily_sales AS (
        SELECT produto, dtped, SUM(qtvenda_embalagem_master) as total_qty
        FROM all_sales
        GROUP BY produto, dtped
    ),
    product_sales_ranked AS (
        SELECT ds.produto, ds.total_qty, wdr.rn
        FROM daily_sales ds
        JOIN working_days_ranked wdr ON ds.dtped = wdr.work_date
    ),
    sales_in_window AS (
        SELECT
            fd.product_code,
            SUM(psr.total_qty) as total_sales_for_avg
        FROM final_data fd
        JOIN product_sales_ranked psr ON fd.product_code = psr.produto AND psr.rn <= fd.days_divisor
        GROUP BY fd.product_code
    ),
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
