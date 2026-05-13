CREATE TABLE match_players (
    server_guid UUID NOT NULL,
    game_id BIGINT NOT NULL,
    team_id INT NOT NULL,
    faction_id INT NOT NULL,
    persona_id BIGINT NOT NULL,
    player_name VARCHAR(500) NOT NULL,
    player_tag VARCHAR(100),
    player_score INT NOT NULL,
    player_kills INT NOT NULL,
    player_deaths INT NOT NULL,
    player_squad INT,
    player_role INT,
    PRIMARY KEY (server_guid, game_id, persona_id),
    FOREIGN KEY (server_guid, game_id) REFERENCES match(server_guid, game_id)
)