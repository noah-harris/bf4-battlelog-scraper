CREATE TABLE persona_info (
    user_id BIGINT,
    persona_id BIGINT UNIQUE,
    rank INT,
    skill INT,
    kills INT,
    deaths INT,
    kd_ratio FLOAT,
    kills_per_minute FLOAT,
    num_wins INT,
    num_rounds INT,
    num_losses INT,
    score INT,
    score_per_minute FLOAT,
    time_played INT,
    accuracy FLOAT
)

