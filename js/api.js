// js/api.js

const api = {
    /**
     * Helper function to handle Supabase RPC calls and errors.
     * @param {SupabaseClient} supabaseClient - The Supabase client instance.
     * @param {string} rpcName - The name of the RPC function to call.
     * @param {object} params - The parameters for the RPC function.
     * @param {boolean} single - Whether to expect a single row as result.
     * @returns {Promise<any>} - The data from the RPC call.
     * @throws {Error} - Throws an error if the RPC call fails.
     */
    async _callRpc(supabaseClient, rpcName, params, single = false) {
        try {
            let query = supabaseClient.rpc(rpcName, params);
            if (single) {
                query = query.single();
            }

            const { data, error } = await query;

            if (error) {
                console.error(`Erro na chamada RPC '${rpcName}':`, error.message, 'Params:', params);
                throw new Error(`Falha ao buscar dados: ${error.message}`);
            }
            return data;
        } catch (e) {
            console.error(`Exceção na chamada RPC '${rpcName}':`, e.message);
            throw e; // Re-throw the exception to be handled by the caller
        }
    },

    // --- Filter Population Functions ---
    getDistinctSupervisors: (supabase) => api._callRpc(supabase, 'get_distinct_supervisors', {}),
    getDistinctVendedores: (supabase, supervisor) => api._callRpc(supabase, 'get_distinct_vendedores', { p_supervisor: supervisor }),
    getDistinctFornecedores: (supabase) => api._callRpc(supabase, 'get_distinct_fornecedores', {}),
    getDistinctTiposVenda: (supabase) => api._callRpc(supabase, 'get_distinct_tipos_venda', {}),
    getDistinctRedes: (supabase) => api._callRpc(supabase, 'get_distinct_redes', {}),

    // --- Main Dashboard Functions ---
    getMainKpis: (supabase, params) => api._callRpc(supabase, 'get_main_kpis', params, true),
    getSalesByGroup: (supabase, params) => api._callRpc(supabase, 'get_sales_by_group', params),
    getTopProducts: (supabase, params) => api._callRpc(supabase, 'get_top_products', params),

    // --- Orders View Functions ---
    getPaginatedOrders: (supabase, params) => api._callRpc(supabase, 'get_paginated_orders', params),
    getOrdersCount: (supabase, params) => api._callRpc(supabase, 'get_orders_count', params, true),

    // --- City View Functions ---
    getCityAnalysis: (supabase, params) => api._callRpc(supabase, 'get_city_analysis', params),

    // --- Weekly View Functions ---
    getWeeklySalesAndRankings: (supabase, params) => api._callRpc(supabase, 'get_weekly_sales_and_rankings', params),

    // --- Comparison View Functions ---
    getComparisonData: (supabase, params) => api._callRpc(supabase, 'get_comparison_data', params),

    // --- Stock View Functions ---
    getStockAnalysisData: (supabase, params) => api._callRpc(supabase, 'get_stock_analysis_data', params),

    // --- Innovations & Coverage View Functions ---
    getCoverageAnalysis: (supabase, params) => api._callRpc(supabase, 'get_coverage_analysis', params),
};
