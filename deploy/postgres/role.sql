-- Run as a PostgreSQL administrator before the first start of ratatoskr-claude, after Platform's
-- `deploy/postgres/01-database-and-roles.sql` has created the `ratatoskr` database:
--
--     sudo -u postgres psql -f deploy/postgres/role.sql
--
-- The service applies `schema.sql` itself at startup, as this role: the `create` privilege on the
-- database below is what lets it create its own `claude_archive` schema, and nothing else in the
-- cluster is granted to it. Set the password separately with `\password ratatoskr_claude` and
-- put it in `RATATOSKR__STORAGE__DATABASE_URL`; no credential belongs in this repository.
--
-- Re-runnable.

do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'ratatoskr_claude') then
        create role ratatoskr_claude login;
    end if;
end
$$;

alter role ratatoskr_claude nosuperuser nocreatedb nocreaterole noreplication nobypassrls;
grant connect, create on database ratatoskr to ratatoskr_claude;

\connect ratatoskr
revoke all on schema public from ratatoskr_claude;
