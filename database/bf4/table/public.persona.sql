CREATE TABLE persona (
    user_id BIGINT REFERENCES ea_user (user_id),
    persona_id BIGINT,
    persona_name VARCHAR(36),
    platform VARCHAR(20) REFERENCES platform (ea_namespace),
    initial_timestamp TIMESTAMPTZ,
    PRIMARY KEY (persona_id)
)