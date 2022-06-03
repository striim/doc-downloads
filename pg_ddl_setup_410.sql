-- STRIIM POSTGRESQL DDL CAPTURE SETUP
-- Execute this script through psql console.
-- CMD: psql <rolename> -h <hostname or hostIP> -d <DatabaseName> -f -a </path/to>pg_ddl_setup.sql 

CREATE SCHEMA IF NOT EXISTS striim;

CREATE TABLE IF NOT EXISTS striim.ddlcapturetable ( event text,
tag text, classid oid, objid oid, objsubid int,
object_type text, schema_name text, object_identity text, is_extension bool, query text,
username text default current_user, db_name text default current_database(),
client_addr inet default inet_client_addr(), creation_time timestamp default now()
);

GRANT USAGE ON SCHEMA striim TO PUBLIC;
GRANT SELECT, INSERT ON TABLE striim.ddlcapturetable TO PUBLIC;

create or replace function striim.ddl_capture_command() returns event_trigger as $$
declare v1 text;
r record;
begin

    select query into v1 from pg_stat_activity where pid=pg_backend_pid();
    if TG_EVENT='ddl_command_end' then
        SELECT * into r FROM pg_event_trigger_ddl_commands();
        if r.classid > 0 then
            insert into striim.ddlcapturetable(event, tag, classid, objid, objsubid, object_type, schema_name, object_identity, is_extension, query)
            values(TG_EVENT, TG_TAG, r.classid, r.objid, r.objsubid, r.object_type, r.schema_name, r.object_identity, r.in_extension, v1);
         end if;
    end if;
    if TG_EVENT='sql_drop' then
            SELECT * into r FROM pg_event_trigger_dropped_objects();
            insert into striim.ddlcapturetable(event, tag, classid, objid, objsubid, object_type, schema_name, object_identity, is_extension, query)
            values(TG_EVENT, TG_TAG, r.classid, r.objid, r.objsubid, r.object_type, r.schema_name, r.object_identity, 'f', v1);
    end if;
end;
$$ language plpgsql strict;

DROP event TRIGGER pg_get_ddl_command; 
DROP event TRIGGER pg_get_ddl_drop;


CREATE EVENT TRIGGER pg_get_ddl_command on ddl_command_end EXECUTE PROCEDURE striim.ddl_capture_command();
CREATE EVENT TRIGGER pg_get_ddl_drop on sql_drop EXECUTE PROCEDURE striim.ddl_capture_command();
