CREATE TABLE match (
    server_guid UUID NOT NULL REFERENCES server(server_guid),
    game_id BIGINT NOT NULL,
    game_mode VARCHAR(100) NOT NULL,
    max_players INT NOT NULL,
    round_time INT NOT NULL,
    current_map VARCHAR(500) NOT NULL,
    match_timestamp BIGINT NOT NULL,
    team_0_tickets INT NULL,
    team_0_max_tickets INT NULL,
    team_1_tickets INT NULL,
    team_1_max_tickets INT NULL,
    team_2_tickets INT NULL,
    team_2_max_tickets INT NULL,
    PRIMARY KEY (server_guid, game_id)
)