CREATE TABLE server (
    server_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    server_ip VARCHAR(50),
    server_port INTEGER,
    server_guid UUID,
    server_name VARCHAR(500),
    UNIQUE (server_guid),
    UNIQUE (server_ip, server_port, server_guid, server_name)
)