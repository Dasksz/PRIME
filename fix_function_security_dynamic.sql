-- Fix Function Search Path Mutable Warnings (Dynamic)

-- This script dynamically finds and updates the search_path for specific functions
-- to resolve security warnings. It handles overloaded functions (functions with the same name
-- but different arguments) which simple SQL scripts might miss.

DO $$
DECLARE
    func_record record;
    target_functions text[] := ARRAY[
        'get_initial_dashboard_data',
        'get_comparison_data',
        'get_filtered_client_base',
        'get_city_view_data',
        'get_comparison_view_data',
        'get_orders_view_data',
        'get_main_charts_data',
        'get_detailed_orders_data',
        'get_innovations_data_v2',
        'handle_new_user',
        'get_weekly_view_data',
        'get_innovations_view_data',
        'get_detailed_orders',
        'get_coverage_view_data',
        'get_filtered_client_base_json',
        'get_stock_view_data'
    ];
BEGIN
    FOR func_record IN
        SELECT oid::regprocedure::text as func_signature, proname
        FROM pg_proc
        WHERE pronamespace = 'public'::regnamespace
          AND prokind = 'f' -- Only normal functions
          AND proname = ANY(target_functions)
    LOOP
        RAISE NOTICE 'Securing function: %', func_record.func_signature;
        EXECUTE format('ALTER FUNCTION %s SET search_path = public', func_record.func_signature);
    END LOOP;
END;
$$;
