-- ============================================================================
-- ARQUIVO MESTRE DE ATUALIZAÇÃO DO BANCO DE DADOS (V3 - FINAL)
-- Contém: Índices de Performance, Funções de Filtro Otimizadas, Views de Dashboard
-- ============================================================================

-- 1. CRIAÇÃO DE ÍNDICES DE PERFORMANCE (OBRIGATÓRIO PARA EVITAR TIMEOUTS)
-- ============================================================================

-- Acelerar tabelas de Vendas (Mês Atual)
CREATE INDEX IF NOT EXISTS idx_detailed_superv ON data_detailed(superv);
CREATE INDEX IF NOT EXISTS idx_detailed_nome ON data_detailed(nome);
CREATE INDEX IF NOT EXISTS idx_detailed_codfor ON data_detailed(codfor);
CREATE INDEX IF NOT EXISTS idx_detailed_produto ON data_detailed(produto);
CREATE INDEX IF NOT EXISTS idx_detailed_codcli ON data_detailed(codcli);
CREATE INDEX IF NOT EXISTS idx_detailed_filial ON data_detailed(filial);
CREATE INDEX IF NOT EXISTS idx_detailed_dtped ON data_detailed(dtped);
CREATE INDEX IF NOT EXISTS idx_detailed_tipovenda ON data_detailed(tipovenda);
CREATE INDEX IF NOT EXISTS idx_detailed_observacaofor ON data_detailed(observacaofor);

-- Acelerar tabelas de Histórico
CREATE INDEX IF NOT EXISTS idx_history_superv ON data_history(superv);
CREATE INDEX IF NOT EXISTS idx_history_nome ON data_history(nome);
CREATE INDEX IF NOT EXISTS idx_history_codfor ON data_history(codfor);
CREATE INDEX IF NOT EXISTS idx_history_produto ON data_history(produto);
CREATE INDEX IF NOT EXISTS idx_history_codcli ON data_history(codcli);
CREATE INDEX IF NOT EXISTS idx_history_filial ON data_history(filial);
CREATE INDEX IF NOT EXISTS idx_history_dtped ON data_history(dtped);
CREATE INDEX IF NOT EXISTS idx_history_tipovenda ON data_history(tipovenda);
CREATE INDEX IF NOT EXISTS idx_history_observacaofor ON data_history(observacaofor);

-- Acelerar cruzamentos (Estoque, Clientes, Produtos)
CREATE INDEX IF NOT EXISTS idx_stock_code ON data_stock(product_code);
CREATE INDEX IF NOT EXISTS idx_stock_filial ON data_stock(filial);
CREATE INDEX IF NOT EXISTS idx_clients_code ON data_clients(codigo_cliente);
CREATE INDEX IF NOT EXISTS idx_clients_cidade ON data_clients(cidade);
CREATE INDEX IF NOT EXISTS idx_clients_ramo ON data_clients(ramo);
CREATE INDEX IF NOT EXISTS idx_products_code ON data_product_details(code);
CREATE INDEX IF NOT EXISTS idx_products_codfor ON data_product_details(codfor);

-- Otimizar estatísticas do banco
ANALYZE data_detailed;
ANALYZE data_history;
ANALYZE data_stock;
ANALYZE data_clients;


-- 2. FUNÇÃO AUXILIAR DE FILTRAGEM DE CLIENTES (CORRIGIDA PARA TEXTO VAZIO)
-- ============================================================================
DROP FUNCTION IF EXISTS get_filtered_client_base_json (jsonb);

CREATE OR REPLACE FUNCTION get_filtered_client_base_json (p_filters jsonb) RETURNS table (codigo_cliente TEXT) as $$
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
-- 3. FUNÇÃO DE ESTOQUE (V7 - Média Diária Corrigida)
-- ============================================================================
-- Remove versões anteriores para garantir a substituição correta
DROP FUNCTION IF EXISTS get_stock_view_data(text, text[], text[], text[], text, text[], text);
DROP FUNCTION IF EXISTS get_stock_view_data(text, text[], text[], text[], text, text[], text, integer);

-- Criar a Função V7
CREATE OR REPLACE FUNCTION get_stock_view_data (
  p_supervisor_filter TEXT default '',
  p_sellers_filter text[] default null,
  p_suppliers_filter text[] default null,
  p_products_filter text[] default null,
  p_rede_group_filter TEXT default '',
  p_redes_filter text[] default null,
  p_filial_filter TEXT default 'ambas',
  p_custom_days INTEGER default 0
) RETURNS JSONB as $$
DECLARE
    result JSONB;
    -- Datas base
    current_month_start DATE;
    last_sale_date DATE;
    history_start_date DATE;
    history_end_date DATE;
    first_ever_sale_date DATE;

    -- Dias úteis globais
    max_working_days INTEGER;
BEGIN
    -- Define datas base a partir dos dados existentes
    SELECT date_trunc('month', MAX(dtped))::DATE, MAX(dtped)::DATE
    INTO current_month_start, last_sale_date
    FROM data_detailed;

    history_end_date := current_month_start - INTERVAL '1 day';
    history_start_date := current_month_start - INTERVAL '3 months';

    -- Calcula o MÁXIMO de dias úteis disponíveis em todo o histórico de vendas
    SELECT MIN(d.dtped) INTO first_ever_sale_date FROM (
        SELECT dtped FROM data_detailed UNION ALL SELECT dtped FROM data_history
    ) d WHERE dtped IS NOT NULL;

    SELECT COUNT(*)
    INTO max_working_days
    FROM generate_series(first_ever_sale_date, last_sale_date, '1 day') d
    WHERE extract(isodow from d) < 6;

    WITH
    -- 1. Filtra Clientes (lógica inalterada)
    sales_clients AS (
        SELECT DISTINCT codcli FROM data_detailed
        WHERE (p_supervisor_filter = '' OR superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR nome = ANY(p_sellers_filter))
    ),
    final_clients AS (
        SELECT c.codigo_cliente 
        FROM data_clients c
        JOIN sales_clients sc ON c.codigo_cliente = sc.codcli
        WHERE (p_rede_group_filter = '' OR 
              (p_rede_group_filter = 'sem_rede' AND (c.ramo IS NULL OR c.ramo = 'N/A')) OR
              (p_rede_group_filter = 'com_rede' AND (p_redes_filter IS NULL OR c.ramo = ANY(p_redes_filter))))
    ),
    
    -- 2. Estoque (lógica inalterada)
    stock_aggregated AS (
        SELECT product_code, SUM(stock_qty) as total_stock
        FROM data_stock
        WHERE (p_filial_filter IS NULL OR p_filial_filter = 'ambas' OR filial = p_filial_filter)
        GROUP BY product_code
    ),

    -- 3. Vendas Atuais e Históricas (sem agregação ainda)
    all_sales AS (
      SELECT produto, qtvenda_embalagem_master, dtped
      FROM data_detailed
      WHERE codcli IN (SELECT codigo_cliente FROM final_clients) AND (p_filial_filter = 'ambas' OR filial = p_filial_filter)
      UNION ALL
      SELECT produto, qtvenda_embalagem_master, dtped
      FROM data_history
      WHERE codcli IN (SELECT codigo_cliente FROM final_clients) AND (p_filial_filter = 'ambas' OR filial = p_filial_filter)
    ),

    -- 4. Encontra a data da primeira venda de cada produto
    product_first_sale AS (
      SELECT produto, MIN(dtped) as first_sale_date
      FROM all_sales
      GROUP BY produto
    ),

    -- 5. Série de dias úteis com ranking reverso (para buscar "N" dias para trás)
    working_days_ranked AS (
        SELECT
            d::date as work_date,
            row_number() OVER (ORDER BY d DESC) as rn
        FROM generate_series(first_ever_sale_date, last_sale_date, '1 day'::interval) d
        WHERE extract(isodow from d) < 6
    ),

    -- 6. Cruzamento final e Cálculos
    product_analysis AS (
        SELECT
            pd.code AS product_code,
            pd.descricao AS product_description,
            pd.fornecedor AS supplier_name,
            COALESCE(sa.total_stock, 0) as stock_qty,

            -- Lógica principal: Calcula o divisor de dias para cada produto
            (
                SELECT COUNT(*)
                FROM generate_series(pfs.first_sale_date, last_sale_date, '1 day') d
                WHERE extract(isodow from d) < 6
            ) as product_lifetime_days,

            pfs.first_sale_date

        FROM data_product_details pd
        LEFT JOIN stock_aggregated sa ON pd.code = sa.product_code
        LEFT JOIN product_first_sale pfs ON pd.code = pfs.produto
        WHERE
            (p_suppliers_filter IS NULL OR pd.codfor = ANY(p_suppliers_filter))
            AND (p_products_filter IS NULL OR pd.code = ANY(p_products_filter))
    ),
    
    -- 7. Define o número de dias e a data de início para o cálculo da média
    final_calc AS (
      SELECT
        pa.*,
        -- Aplica a regra de negócio para o divisor (quantos dias olhar para trás)
        GREATEST(
            LEAST(
                -- Se custom_days for 0 ou maior que o tempo de vida, usa o tempo de vida
                CASE WHEN p_custom_days <= 0 OR p_custom_days > pa.product_lifetime_days
                     THEN pa.product_lifetime_days
                     ELSE p_custom_days
                END,
                max_working_days -- Limita ao máximo de dias disponíveis
            ),
        1)::INTEGER as days_divisor
      FROM product_analysis pa
    ),

    -- 8. Calcula as vendas e médias com base no período correto
    sales_in_period AS (
      SELECT
        fc.product_code,
        fc.product_description,
        fc.supplier_name,
        fc.stock_qty,
        fc.days_divisor,

        -- Média mensal histórica (baseada em 3 meses, inalterada)
        (SELECT COALESCE(SUM(qtvenda_embalagem_master), 0) / 3.0
         FROM all_sales
         WHERE produto = fc.product_code AND dtped BETWEEN history_start_date AND history_end_date
        ) as history_avg_monthly_qty,

        -- Vendas no mês atual (inalterado)
        (SELECT COALESCE(SUM(qtvenda_embalagem_master), 0)
         FROM all_sales
         WHERE produto = fc.product_code AND dtped >= current_month_start
        ) as current_month_sales_qty,

        -- NOVA LÓGICA: Soma as vendas apenas no período de dias úteis determinado
        (SELECT COALESCE(SUM(s.qtvenda_embalagem_master), 0)
         FROM all_sales s
         WHERE s.produto = fc.product_code
           AND s.dtped >= (SELECT work_date FROM working_days_ranked WHERE rn = fc.days_divisor)
        ) as total_sales_for_avg

      FROM final_calc fc
      WHERE fc.product_code IS NOT NULL AND
            (fc.stock_qty > 0 OR fc.history_avg_monthly_qty > 0 OR fc.current_month_sales_qty > 0)
    ),

    categorized_products AS (
        SELECT *,
            CASE
                WHEN current_month_sales_qty > 0 AND history_avg_monthly_qty > 0 AND current_month_sales_qty >= history_avg_monthly_qty THEN 'growth'
                WHEN current_month_sales_qty > 0 AND history_avg_monthly_qty > 0 AND current_month_sales_qty < history_avg_monthly_qty THEN 'decline'
                WHEN current_month_sales_qty > 0 AND history_avg_monthly_qty = 0 THEN 'new'
                WHEN current_month_sales_qty = 0 AND history_avg_monthly_qty > 0 THEN 'lost'
                ELSE NULL
            END as category
        FROM sales_in_period
    )
    
    SELECT jsonb_build_object(
        'max_working_days', max_working_days,
        'working_days_used', CASE WHEN p_custom_days > 0 THEN p_custom_days ELSE max_working_days END,
        'stock_table', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'product_code', sip.product_code,
                'product_description', sip.product_description,
                'supplier_name', sip.supplier_name,
                'stock_qty', sip.stock_qty,
                'avg_monthly_qty', sip.history_avg_monthly_qty,
                'daily_avg_qty', sip.total_sales_for_avg / sip.days_divisor,
                'trend_days', CASE
                                WHEN (sip.total_sales_for_avg / sip.days_divisor) > 0 THEN (sip.stock_qty / (sip.total_sales_for_avg / sip.days_divisor))
                                ELSE 999
                              END
            ) ORDER BY (CASE WHEN (sip.total_sales_for_avg / sip.days_divisor) > 0 THEN (sip.stock_qty / (sip.total_sales_for_avg / sip.days_divisor)) ELSE 999 END) ASC), '[]'::jsonb)
            FROM sales_in_period sip
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
CREATE OR REPLACE FUNCTION get_weekly_view_data(
    p_supervisor TEXT DEFAULT NULL,
    p_fornecedor TEXT DEFAULT NULL
) RETURNS JSONB AS $$
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


-- 5. FUNÇÃO DE COBERTURA (RECRIADA)
-- ============================================================================
CREATE OR REPLACE FUNCTION get_coverage_view_data(
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
BEGIN
    -- Define datas
    SELECT date_trunc('month', MAX(dtped))::DATE INTO current_month_start FROM data_detailed;
    previous_month_start := current_month_start - INTERVAL '1 month';

    WITH
    -- 1. Dados de Produtos e Estoque (somado)
    stock_sum AS (
        SELECT product_code, SUM(stock_qty) as total_stock 
        FROM data_stock 
        WHERE (p_filial_filter = 'ambas' OR filial = p_filial_filter)
        GROUP BY product_code
    ),
    base_products AS (
        SELECT pd.code, pd.descricao, COALESCE(s.total_stock, 0) as stock_qty
        FROM data_product_details pd
        LEFT JOIN stock_sum s ON pd.code = s.product_code
        WHERE (p_suppliers_filter IS NULL OR pd.codfor = ANY(p_suppliers_filter))
          AND (p_products_filter IS NULL OR pd.code = ANY(p_products_filter))
    ),
    -- 2. Vendas Recentes (Mês Atual + Anterior) para cálculo de cobertura
    sales_data AS (
        SELECT 
            s.produto, 
            s.codcli, 
            CASE WHEN s.dtped >= current_month_start THEN 1 ELSE 0 END as is_current
        FROM (
            SELECT produto, codcli, dtped, superv, nome, filial, codfor, observacaofor FROM data_detailed
            UNION ALL
            SELECT produto, codcli, dtped, superv, nome, filial, codfor, observacaofor FROM data_history
        ) s
        JOIN data_clients c ON s.codcli = c.codigo_cliente
        WHERE s.dtped >= previous_month_start
          AND (p_supervisor_filter = '' OR s.superv = p_supervisor_filter)
          AND (p_sellers_filter IS NULL OR s.nome = ANY(p_sellers_filter))
          AND (p_filial_filter = 'ambas' OR s.filial = p_filial_filter)
          AND (p_suppliers_filter IS NULL OR s.codfor = ANY(p_suppliers_filter))
          AND (p_city_filter = '' OR c.cidade ILIKE p_city_filter)
    ),
    -- 3. Agregação Final
    final_agg AS (
        SELECT 
            bp.code,
            bp.descricao,
            bp.stock_qty,
            COUNT(DISTINCT CASE WHEN sd.is_current = 0 THEN sd.codcli END) as clients_prev,
            COUNT(DISTINCT CASE WHEN sd.is_current = 1 THEN sd.codcli END) as clients_curr
        FROM base_products bp
        LEFT JOIN sales_data sd ON bp.code = sd.produto
        GROUP BY 1, 2, 3
    )
    SELECT jsonb_build_object(
        'coverage_table', (SELECT COALESCE(jsonb_agg(fa ORDER BY clients_curr DESC), '[]'::jsonb) FROM final_agg fa),
        'active_clients_count', (SELECT COUNT(*) FROM data_clients WHERE bloqueio <> 'S')
    ) INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;
