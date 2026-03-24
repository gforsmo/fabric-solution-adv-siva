-- ============================================
-- 05_create_users_and_security.sql
-- Create users, assign roles and schema permissions
-- Run order: After 02_create_schemas_and_tables.sql
-- Can be run multiple times safely (idempotent)
-- ============================================

-- ============================================
-- Security group: sg-av-engineers
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
-- Individual user: geir.forsmo.atea@siva.no
-- Note: Already db owner - no additional permissions needed
-- ============================================
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'geir.forsmo.atea@siva.no')
    CREATE USER [geir.forsmo.atea@siva.no] FROM EXTERNAL PROVIDER
GO