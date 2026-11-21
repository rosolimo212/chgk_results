-- Grants for the already created database chgk.
-- Run as chgk_owner, connected to database chgk, after schema.sql.
-- Other databases are not touched.
--
-- chgk_owner owns the database and everything this script's user creates.
-- That role can grant, drop tables and change views inside chgk.
-- chgk_reader can only SELECT. The loader does not use it.

revoke all on database chgk from public;
grant connect, temp on database chgk to chgk_owner;
grant connect on database chgk to chgk_reader;

grant usage on schema chgk to chgk_reader;
grant select on all tables in schema chgk to chgk_reader;

alter default privileges for role chgk_owner in schema chgk
    grant select on tables to chgk_reader;
