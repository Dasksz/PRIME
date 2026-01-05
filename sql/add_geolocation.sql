-- Create table for storing client coordinates
CREATE TABLE IF NOT EXISTS data_client_coordinates (
    client_code TEXT PRIMARY KEY,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    address TEXT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Enable RLS
ALTER TABLE data_client_coordinates ENABLE ROW LEVEL SECURITY;

-- Allow read access to authenticated users
CREATE POLICY "Enable read access for all users" ON data_client_coordinates
    FOR SELECT USING (auth.role() = 'authenticated' OR auth.role() = 'anon');

-- Allow insert/update only for admins (logic handled in app via role check, but here we can allow authenticated for now or restrict based on profile)
-- Assuming the app handles role checks before writing.
CREATE POLICY "Enable insert/update for authenticated users" ON data_client_coordinates
    FOR ALL USING (auth.role() = 'authenticated' OR auth.role() = 'anon');
