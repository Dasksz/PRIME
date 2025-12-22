# Security Fixes

This document outlines the steps to resolve the security warnings reported in Supabase.

## 1. Function Search Path Mutable Warnings

The security report identified several functions in the `public` schema that do not have a `search_path` set. This can be a security risk.

Since the source code for these functions was not found in the repository, a SQL script has been created to fix them in the database.

### Instructions:

1.  Open the Supabase Dashboard for your project.
2.  Go to the **SQL Editor**.
3.  Open the file `fix_function_search_paths.sql` from this repository (or copy its content).
4.  Paste the content into the SQL Editor and click **Run**.

This script will iterate through the affected functions and set their `search_path` to `public`.

## 2. Leaked Password Protection Disabled

The security report also flagged that "Leaked Password Protection" is disabled.

### Instructions:

1.  Open the Supabase Dashboard for your project.
2.  Navigate to **Authentication** -> **Configuration** (or **Settings**).
3.  Look for the **Security** section or **Password Protection**.
4.  Enable **Leaked Password Protection**.
    *   *Note: This feature checks passwords against known data breaches (HaveIBeenPwned) to prevent users from using compromised passwords.*

## Missing Function Definitions

The following functions were flagged but their definitions are missing from the `codigo_sql_supabase.sql` file in this repository:

*   `get_initial_dashboard_data`
*   `get_comparison_data`
*   `get_filtered_client_base`
*   `get_city_view_data`
*   `get_comparison_view_data`
*   `get_orders_view_data`
*   `get_main_charts_data`
*   `get_detailed_orders_data`
*   `get_innovations_data_v2`
*   `get_weekly_view_data`
*   `get_innovations_view_data`
*   `get_detailed_orders`
*   `get_coverage_view_data`
*   `get_filtered_client_base_json`
*   `get_stock_view_data`

It is recommended to locate the source code for these functions and add them to your repository to ensure version control and reproducible deployments.
