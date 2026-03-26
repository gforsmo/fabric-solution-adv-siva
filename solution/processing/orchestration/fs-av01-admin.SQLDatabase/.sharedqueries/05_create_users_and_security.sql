-- ============================================
-- 05_create_users_and_security.sql
-- Create users, assign roles and schema permissions
--
-- CI/CD-safe: uses Entra ID groups only
-- No hardcoded personal users
--
-- Run order: After 04_create_triggers.sql
-- Can be run multiple times safely (idempotent)
-- ============================================

-- ============================================
-- Security group: sg-av-engineers
-- Entra ID group - CI/CD safe
-- ============================================
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'sg-av-engineers')
    CREATE USER [sg-av-engineers] FROM EXTERNAL PROVIDER
GO

IF EXISTS (SELECT * FROM sys.database_principals WHERE name = 'sg-av-engineers')
BEGIN
    IF NOT EXISTS (
        SELECT * FROM sys.database_role_members rm
        JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
        JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id
        WHERE r.name = 'db_datareader' AND m.name = 'sg-av-engineers'
    )
        ALTER ROLE [db_datareader] ADD MEMBER [sg-av-engineers]

    IF NOT EXISTS (
        SELECT * FROM sys.database_role_members rm
        JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
        JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id
        WHERE r.name = 'db_datawriter' AND m.name = 'sg-av-engineers'
    )
        ALTER ROLE [db_datawriter] ADD MEMBER [sg-av-engineers]

    GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::[metadata] TO [sg-av-engineers]
    GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::[instructions] TO [sg-av-engineers]
    GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::[log] TO [sg-av-engineers]
END
GO

-- ============================================
-- NOTE: Personal users should NOT be managed here.
-- Individual access is granted via Entra ID group
-- membership in sg-av-engineers.
--
-- To grant access to a developer:
--   Add them to sg-av-engineers in Entra ID/Azure AD
-- ============================================