-- =================================================================
-- SCRIPT SQL UNIFICADO V2.7 - PRIME (CORREÇÃO DE SEGURANÇA E RLS)
-- OBJETIVO: Adicionar a função de verificação `is_caller_approved` e
--           as políticas de RLS para corrigir o erro 401 (Não Autorizado).
-- =================================================================

-- ETAPA 1: APAGAR FUNÇÕES ANTIGAS (DROP)
-- Adicionado para permitir a alteração do tipo de retorno (RETURNS TABLE).

DROP FUNCTION IF EXISTS get_main_kpis(text,text,text[],text,text,text,text[],text,text[],text,text);
DROP FUNCTION IF EXISTS get_top_products(text,text,text,text[],text,text,text,text[],text,text[],text,text);
DROP FUNCTION IF EXISTS get_orders_count(text,text,text[],text,text,text,text[],text,text[],text,text);
DROP FUNCTION IF EXISTS get_city_analysis(text,text[],text,text[],text,text);
DROP FUNCTION IF EXISTS get_weekly_sales_and_rankings(text,text[]);

-- =================================================================
-- ETAPA 2: FUNÇÕES E POLÍTICAS DE SEGURANÇA (RLS)
-- =================================================================

-- 2.1: Função de Verificação de Acesso
-- Esta função verifica se o usuário que está fazendo a chamada tem o status 'aprovado'.
create or replace function public.is_caller_approved()
returns boolean
language plpgsql
security definer
set search_path = public
as $$
begin
  -- Retorna true se o usuário autenticado (auth.uid()) existir na tabela de perfis
  -- e tiver o status 'aprovado'.
  return exists (
    select 1
    from public.profiles
    where id = auth.uid() and status = 'aprovado'
  );
end;
$$;

-- 2.2: Habilitação do Row Level Security (RLS)
-- Ativa a aplicação de políticas de segurança para cada tabela.
alter table public.data_clients enable row level security;
alter table public.data_detailed enable row level security;
alter table public.data_history enable row level security;
alter table public.data_metadata enable row level security;
alter table public.data_orders enable row level security;
alter table public.data_product_details enable row level security;
alter table public.profiles enable row level security;

-- 2.3: Remoção de Políticas Antigas (para evitar duplicatas)
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_clients;
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_detailed;
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_history;
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_metadata;
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_orders;
DROP POLICY IF EXISTS "Permitir acesso de leitura para usuários aprovados" ON public.data_product_details;
DROP POLICY IF EXISTS "Permitir que usuários leiam seu próprio perfil" ON public.profiles;
DROP POLICY IF EXISTS "Permitir que usuários aprovados leiam todos os perfis" ON public.profiles;

-- 2.4: Criação das Políticas de Acesso (Policies)
-- Define as regras: apenas usuários aprovados podem ler (SELECT) os dados.
CREATE POLICY "Permitir acesso de leitura para usuários aprovados" ON public.data_clients FOR SELECT USING (public.is_caller_approved());
CREATE POLICY "Permitir acesso de leitura para usuários aprovados" ON public.data_detailed FOR SELECT USING (public.is_caller_approved());
CREATE POLICY "Permitir acesso de leitura para usuários aprovados" ON public.data_history FOR SELECT USING (public.is_caller_approved());
CREATE POLICY "Permitir acesso de leitura para usuários aprovados" ON public.data_metadata FOR SELECT USING (public.is_caller_approved());
CREATE POLICY "Permitir acesso de leitura para usuários aprovados" ON public.data_orders FOR SELECT USING (public.is_caller_approved());
CREATE POLICY "Permitir acesso de leitura para usuários aprovados" ON public.data_product_details FOR SELECT USING (public.is_caller_approved());

-- Políticas para a tabela de perfis:
-- 1. Um usuário sempre pode ler seu próprio perfil.
-- 2. Um usuário aprovado pode ler o perfil de outros (útil para admin).
CREATE POLICY "Permitir que usuários leiam seu próprio perfil" ON public.profiles FOR SELECT USING (auth.uid() = id);
CREATE POLICY "Permitir que usuários aprovados leiam todos os perfis" ON public.profiles FOR SELECT USING (public.is_caller_approved());


-- =================================================================
-- ETAPA 3: FUNÇÕES DE CÁLCULO (RPC) - (Funções Corrigidas)
-- =================================================================

-- 3.0: Função Auxiliar de Filtro de Cliente (BASE) - (Inalterado)
create or replace function get_filtered_client_base (
  p_supervisor TEXT default null,
  p_vendedor_nomes text[] default null,
  p_rede_group TEXT default null,
  p_redes text[] default null,
  p_cidade TEXT default null,
  p_codcli TEXT default null,
  p_filial TEXT default null
) RETURNS table (codigo_cliente TEXT) LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientLastBranch AS (
        SELECT DISTINCT ON (codcli) codcli, filial
        FROM (
            SELECT codcli, filial, dtped FROM public.data_detailed
            UNION ALL
            SELECT codcli, filial, dtped FROM public.data_history
        ) AS all_sales
        ORDER BY codcli, dtped DESC
    )
    SELECT c.codigo_cliente
    FROM public.data_clients AS c
    LEFT JOIN ClientLastBranch AS clb ON c.codigo_cliente = clb.codcli
    WHERE
        (p_rede_group IS NULL OR
         (p_rede_group = 'sem_rede' AND (c.ramo IS NULL OR c.ramo = 'N/A')) OR
         (p_rede_group = 'com_rede' AND (c.ramo IS NOT NULL AND c.ramo != 'N/A') AND
            (p_redes IS NULL OR c.ramo = ANY(p_redes)))
        )
    AND (p_supervisor IS NULL OR c.rca1 IN (
            SELECT DISTINCT codusur FROM data_detailed WHERE superv = p_supervisor
            UNION
            SELECT DISTINCT codusur FROM data_history WHERE superv = p_supervisor
        ))
    AND (p_vendedor_nomes IS NULL OR c.rca1 IN (
            SELECT DISTINCT codusur FROM data_detailed WHERE nome = ANY(p_vendedor_nomes)
            UNION
            SELECT DISTINCT codusur FROM data_history WHERE nome = ANY(p_vendedor_nomes)
        ))
    AND (p_cidade IS NULL OR c.cidade = p_cidade)
    AND (p_codcli IS NULL OR c.codigo_cliente = p_codcli)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR clb.filial = p_filial);
END;
$$;


-- 3.1: KPIs Principais (CORRIGIDO)
create or replace function get_main_kpis (
  p_pasta TEXT default null,
  p_supervisor TEXT default null,
  p_vendedor_nomes text[] default null,
  p_codcli TEXT default null,
  p_posicao TEXT default null,
  p_codfor TEXT default null,
  p_tipos_venda text[] default null,
  p_rede_group TEXT default null,
  p_redes text[] default null,
  p_cidade TEXT default null,
  p_filial TEXT default 'ambas'
) RETURNS table (
  total_faturamento NUMERIC,
  total_peso NUMERIC,
  total_skus BIGINT,
  total_pdvs_positivados BIGINT,
  base_clientes_filtro BIGINT
) LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
DECLARE
    v_base_clientes_count BIGINT;
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    SELECT COUNT(*)
    INTO v_base_clientes_count
    FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial);

    RETURN QUERY
    WITH VendasFiltradas AS (
        SELECT v.vlvenda, v.vlbonific, v.totpesoliq, v.codcli, v.produto
        FROM public.data_detailed AS v
        WHERE
            (p_pasta IS NULL OR v.observacaofor = p_pasta)
        AND (p_supervisor IS NULL OR v.superv = p_supervisor)
        AND (p_vendedor_nomes IS NULL OR v.nome = ANY(p_vendedor_nomes))
        AND (p_codcli IS NULL OR v.codcli = p_codcli)
        AND (p_posicao IS NULL OR v.posicao = p_posicao)
        AND (p_codfor IS NULL OR v.codfor = p_codfor)
        AND (p_tipos_venda IS NULL OR v.tipovenda = ANY(p_tipos_venda))
        AND (p_cidade IS NULL OR v.cidade = p_cidade)
        AND (p_filial = 'ambas' OR p_filial IS NULL OR v.filial = p_filial)
        AND ( (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR v.codcli IN (
            SELECT codigo_cliente FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
        ))
    )
    SELECT
        COALESCE(SUM(v.vlvenda), 0)::numeric AS total_faturamento,
        COALESCE(SUM(v.totpesoliq), 0)::numeric AS total_peso,
        COALESCE(COUNT(DISTINCT v.produto), 0)::bigint AS total_skus,
        COALESCE(COUNT(DISTINCT v.codcli), 0)::bigint AS total_pdvs_positivados,
        COALESCE(v_base_clientes_count, 0)::bigint AS base_clientes_filtro
    FROM VendasFiltradas AS v
    WHERE v.vlvenda > 0 OR v.vlbonific > 0;
END;
$$;


-- 3.2: Top Produtos (CORRIGIDO)
create or replace function get_top_products (
  p_metric TEXT,
  p_pasta TEXT default null,
  p_supervisor TEXT default null,
  p_vendedor_nomes text[] default null,
  p_codcli TEXT default null,
  p_posicao TEXT default null,
  p_codfor TEXT default null,
  p_tipos_venda text[] default null,
  p_rede_group TEXT default null,
  p_redes text[] default null,
  p_cidade TEXT default null,
  p_filial TEXT default 'ambas'
) RETURNS table (
  codigo_produto TEXT,
  descricao_produto TEXT,
  valor_metrica NUMERIC
) LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
    )
    SELECT
        v.produto::text AS codigo_produto,
        pd.descricao AS descricao_produto,
        SUM(
            CASE
                WHEN p_metric = 'faturamento' THEN v.vlvenda
                WHEN p_metric = 'peso' THEN v.totpesoliq
                ELSE 0::numeric
            END
        )::numeric AS valor_metrica
    FROM
        public.data_detailed AS v
    JOIN
        public.data_product_details AS pd ON v.produto = pd.code
    WHERE
        (p_pasta IS NULL OR v.observacaofor = p_pasta)
    AND (p_supervisor IS NULL OR v.superv = p_supervisor)
    AND (p_vendedor_nomes IS NULL OR v.nome = ANY(p_vendedor_nomes))
    AND (p_codcli IS NULL OR v.codcli = p_codcli)
    AND (p_posicao IS NULL OR v.posicao = p_posicao)
    AND (p_codfor IS NULL OR v.codfor = p_codfor)
    AND (p_tipos_venda IS NULL OR v.tipovenda = ANY(p_tipos_venda))
    AND (p_cidade IS NULL OR v.cidade = p_cidade)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR v.filial = p_filial)
    AND ( (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR v.codcli IN (SELECT codigo_cliente FROM ClientBase) )
    GROUP BY
        v.produto, pd.descricao
    ORDER BY
        valor_metrica DESC
    LIMIT 10;
END;
$$;


-- 3.3: Contagem de Pedidos (CORRIGIDO)
create or replace function get_orders_count (
  p_pasta TEXT default null,
  p_supervisor TEXT default null,
  p_vendedor_nomes text[] default null,
  p_codcli TEXT default null,
  p_posicao TEXT default null,
  p_codfor TEXT default null,
  p_tipos_venda text[] default null,
  p_rede_group TEXT default null,
  p_redes text[] default null,
  p_cidade TEXT default null,
  p_filial TEXT default 'ambas'
) RETURNS TABLE(total_count BIGINT) LANGUAGE plpgsql SECURITY DEFINER
set
  search_path = public as $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
    )
    SELECT COUNT(*)::bigint
    FROM public.data_orders AS o
    WHERE
        (p_pasta IS NULL OR (o.fornecedores_list::text[] @> ARRAY[p_pasta]))
    AND (p_supervisor IS NULL OR o.superv = p_supervisor)
    AND (p_vendedor_nomes IS NULL OR o.nome = ANY(p_vendedor_nomes))
    AND (p_codcli IS NULL OR o.codcli = p_codcli)
    AND (p_posicao IS NULL OR o.posicao = p_posicao)
    AND (p_codfor IS NULL OR (o.codfors_list::text[] @> ARRAY[p_codfor]))
    AND (p_tipos_venda IS NULL OR o.tipovenda = ANY(p_tipos_venda))
    AND (p_cidade IS NULL OR o.cidade = p_cidade)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR o.filial = p_filial)
    AND ( (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR o.codcli IN (SELECT codigo_cliente FROM ClientBase) );
END;
$$;


-- 3.4: Análise de Cidade (NOVA FUNÇÃO)
create or replace function get_city_analysis(
    p_supervisor text DEFAULT NULL,
    p_vendedor_nomes text[] DEFAULT NULL,
    p_rede_group text DEFAULT NULL,
    p_redes text[] DEFAULT NULL,
    p_cidade text DEFAULT NULL,
    p_codcli text DEFAULT NULL
)
RETURNS TABLE (
    tipo_analise text,
    group_name text,
    total_faturamento numeric,
    status_cliente text,
    codigo_cliente text,
    fantasia text,
    cidade text,
    bairro text,
    ultimacompra date,
    rca1 text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, NULL)
    ),
    SalesData AS (
        SELECT
            codcli,
            SUM(vlvenda) AS faturamento
        FROM public.data_detailed
        WHERE codcli IN (SELECT codigo_cliente FROM ClientBase)
        GROUP BY codcli
    )
    -- Gráfico: Top 10 Clientes na Cidade/Filtro
    SELECT
        'chart'::text AS tipo_analise,
        c.fantasia::text AS group_name,
        COALESCE(s.faturamento, 0)::numeric AS total_faturamento,
        NULL::text, NULL::text, NULL::text, NULL::text, NULL::text, NULL::date, NULL::text
    FROM public.data_clients c
    LEFT JOIN SalesData s ON c.codigo_cliente = s.codcli
    WHERE c.codigo_cliente IN (SELECT codigo_cliente FROM ClientBase)
    ORDER BY total_faturamento DESC
    LIMIT 10

    UNION ALL

    -- Lista de Clientes para as Tabelas
    SELECT
        'client_list'::text AS tipo_analise,
        c.fantasia::text AS group_name,
        COALESCE(s.faturamento, 0)::numeric AS total_faturamento,
        CASE
            WHEN c.dtinclusao >= (NOW() - INTERVAL '90 days') THEN 'novo'
            WHEN c.ultimacompra < (NOW() - INTERVAL '45 days') THEN 'inativo'
            ELSE 'ativo'
        END::text AS status_cliente,
        c.codigo_cliente::text,
        c.fantasia::text,
        c.cidade::text,
        c.bairro::text,
        c.ultimacompra::date,
        c.rca1::text
    FROM public.data_clients c
    LEFT JOIN SalesData s ON c.codigo_cliente = s.codcli
    WHERE c.codigo_cliente IN (SELECT codigo_cliente FROM ClientBase);
END;
$$;


-- 3.5: Análise Semanal (NOVA FUNÇÃO)
CREATE OR REPLACE FUNCTION public.get_weekly_sales_and_rankings(
    p_pasta text DEFAULT NULL,
    p_supervisores text[] DEFAULT NULL
)
RETURNS TABLE(
    tipo_dado text,
    group_name text,
    total_valor numeric,
    dia_semana text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET SEARCH_PATH = public AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    -- 1. Vendas da semana atual por dia
    WITH VendasDaSemana AS (
        SELECT
            v.nome,
            v.superv,
            v.vlvenda,
            -- Garante que o dia da semana seja calculado em UTC
            to_char(v.dtped AT TIME ZONE 'UTC', 'Day') AS dia_semana_nome,
            extract(isodow from v.dtped AT TIME ZONE 'UTC') AS dia_semana_num
        FROM public.data_detailed v
        WHERE
            v.dtped >= date_trunc('week', NOW() AT TIME ZONE 'UTC')
        AND (p_pasta IS NULL OR v.observacaofor = p_pasta)
        AND (p_supervisores IS NULL OR v.superv = ANY(p_supervisores))
    )
    SELECT
        'venda_semanal'::text,
        s.superv::text,
        SUM(s.vlvenda)::numeric,
        trim(s.dia_semana_nome)::text
    FROM VendasDaSemana s
    GROUP BY s.superv, s.dia_semana_nome, s.dia_semana_num
    ORDER BY s.dia_semana_num

    UNION ALL

    -- 2. Ranking de Positivação
    SELECT
        'rank_positivacao'::text,
        v.superv::text,
        COUNT(DISTINCT v.codcli)::numeric,
        NULL::text
    FROM public.data_detailed v
    WHERE
        v.dtped >= date_trunc('month', NOW() AT TIME ZONE 'UTC')
    AND (p_pasta IS NULL OR v.observacaofor = p_pasta)
    AND (p_supervisores IS NULL OR v.superv = ANY(p_supervisores))
    GROUP BY v.superv
    ORDER BY total_valor DESC
    LIMIT 5

    UNION ALL

    -- 3. Ranking Top Sellers (Faturamento)
    SELECT
        'rank_topsellers'::text,
        v.nome::text,
        SUM(v.vlvenda)::numeric,
        NULL::text
    FROM public.data_detailed v
    WHERE
        v.dtped >= date_trunc('month', NOW() AT TIME ZONE 'UTC')
    AND (p_pasta IS NULL OR v.observacaofor = p_pasta)
    AND (p_supervisores IS NULL OR v.superv = ANY(p_supervisores))
    GROUP BY v.nome
    ORDER BY total_valor DESC
    LIMIT 10

    UNION ALL

    -- 4. Ranking de Mix de Produto
    SELECT
        'rank_mix'::text,
        v.superv::text,
        (COUNT(DISTINCT v.produto)::decimal / COUNT(DISTINCT v.codcli))::numeric,
        NULL::text
    FROM public.data_detailed v
    WHERE
        v.dtped >= date_trunc('month', NOW() AT TIME ZONE 'UTC')
    AND (p_pasta IS NULL OR v.observacaofor = p_pasta)
    AND (p_supervisores IS NULL OR v.superv = ANY(p_supervisores))
    GROUP BY v.superv
    HAVING COUNT(DISTINCT v.codcli) > 0 -- Evita divisão por zero
    ORDER BY total_valor DESC
    LIMIT 5;
END;
$$;

-- ETAPA FINAL: Forçar o Supabase a recarregar o esquema
NOTIFY pgrst, 'reload schema';
