import sqlalchemy

from requesting import _get_json, _get_text
import re
import json
from db import get_conn
from datetime import datetime, timezone
import pandas as pd
import colorlog

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    "%(log_color)s%(levelname)-8s%(reset)s %(cyan)s%(funcName)s%(reset)s — %(message)s",
    log_colors={
        "DEBUG":    "white",
        "INFO":     "green",
        "WARNING":  "yellow",
        "ERROR":    "red",
        "CRITICAL": "bold_red",
    }
))
logger = colorlog.getLogger("pipeline")
logger.addHandler(handler)
logger.setLevel(colorlog.DEBUG)


def download_ea_text_content() -> None:
    def get_ea_text_content() -> dict[str, str]:
        js_text = _get_text("https://cdn.battlelog.com/bl-cdn/cdnprefix/develop-8950182/public/dynamic/bf4/en_US.js")
        return dict(re.findall(r"t\['([^']+)'\]\s*=\s*\"((?:[^\"\\]|\\.)*)\"", js_text))

    def upsert_ea_text(text_map: dict[str, str]) -> None:
        if not text_map:
            return
        rows = [{"ea_text_id": k, "ea_text": v} for k, v in text_map.items()]
        with get_conn() as conn:
            conn.execute(sqlalchemy.text("""
                INSERT INTO ea_cdn_text (ea_text_id, ea_text)
                VALUES (:ea_text_id, :ea_text)
                ON CONFLICT (ea_text_id) DO UPDATE SET ea_text = EXCLUDED.ea_text
            """), rows)

    logger.info("Downloading EA text content...")
    text_map = get_ea_text_content()
    logger.debug(f"Fetched {len(text_map)} EA text keys from CDN")
    upsert_ea_text(text_map)
    logger.info(f"Upserted {len(text_map)} EA text entries into the database")


def download_servers() -> None:
    def get_current_unique_servers() -> list[dict]:
        servers: list[dict] = _get_json("https://battlelog.battlefield.com/bf4/servers/pc/?filtered=1&expand=1&settings=&useLocation=1&useAdvanced=-1&gameexpansions=-1&slots=16&slots=1&slots=2&slots=4&q=&serverTypes=1&serverTypes=2&gameexpansions=-1&gameexpansions=-1&gameexpansions=-1&gameexpansions=-1&gameexpansions=-1&mapRotation=-1&modeRotation=-1&password=-1&regions=1&osls=-1&vvsa=-1&vffi=-1&vaba=-1&vkca=-1&v3ca=-1&v3sp=-1&vmsp=-1&vrhe=-1&vhud=-1&vmin=-1&vnta=-1&vbdm-min=1&vbdm-max=300&vprt-min=1&vprt-max=300&vshe-min=1&vshe-max=300&vtkk-min=1&vtkk-max=99&vnit-min=30&vnit-max=86400&vtkc-min=1&vtkc-max=99&vvsd-min=0&vvsd-max=500&vgmc-min=0&vgmc-max=500").get('globalContext', {}).get('servers', {})
        return [{
            'server_ip': server['ip'],
            'server_port': server['port'],
            'server_guid': server['guid'],
            'server_name': server['name'],
        } for server in servers]

    def upsert_servers(servers: list[dict]) -> None:
        if not servers:
            return
        with get_conn() as conn:
            conn.execute(sqlalchemy.text("""
                INSERT INTO server (server_ip, server_port, server_guid, server_name)
                VALUES (:server_ip, :server_port, :server_guid, :server_name)
                ON CONFLICT (server_ip, server_port, server_guid, server_name) DO NOTHING
            """), servers)

    logger.info("Fetching current server list...")
    servers = get_current_unique_servers()
    logger.debug(f"Found {len(servers)} active servers in browser")
    upsert_servers(servers)
    logger.info(f"Upserted {len(servers)} servers into the database")

    return servers


def download_server_snapshot(server_guid) -> None:
    def get_server_snapshot(server_guid: str) -> dict:
        server_json = _get_json(f"https://keeper.battlelog.com/snapshot/{server_guid}")

        GAME_MODE_TO_SCORE_FIELD = {
            "ConquestLarge0": "conquest",
            "ConquestSmall0": "conquest",
            "ConquestLarge": "conquest",
            "ConquestSmall": "conquest",
            "RushLarge0": "rush",
            "SquadDeathMatch0": "squaddeathmatch",
            "TeamDeathMatch0": "teamdeathmatch",
            "Domination0": "domination",
            "Elimination0": "elimination",
            "Obliteration": "obliteration",
            "AirSuperiority0": "airsuperiority",
            "CaptureTheFlag0": "capturetheflag",
            "CarrierAssaultSmall0": "carrierassault",
            "CarrierAssaultLarge0": "carrierassault",
            "Chainlink0": "chainlink",
        }
        team_score_key = GAME_MODE_TO_SCORE_FIELD.get(server_json.get('snapshot', {}).get('gameMode'))
        server_data = {
            'server_guid': server_guid,
            'game_id': server_json.get('snapshot', {}).get('gameId'),
            'game_mode': server_json.get('snapshot', {}).get('gameMode'),
            'max_players': server_json.get('snapshot', {}).get('maxPlayers'),
            'round_time': server_json.get('snapshot', {}).get('roundTime'),
            'current_map': server_json.get('snapshot', {}).get('currentMap'),
            'match_timestamp': server_json.get('lastUpdated'),
            'team_0_tickets': server_json.get('snapshot', {}).get(team_score_key, {}).get('0', {}).get('tickets', 0),
            'team_0_max_tickets': server_json.get('snapshot', {}).get(team_score_key, {}).get('0', {}).get('ticketsMax', 0),
            'team_1_tickets': server_json.get('snapshot', {}).get(team_score_key, {}).get('1', {}).get('tickets', 0),
            'team_1_max_tickets': server_json.get('snapshot', {}).get(team_score_key, {}).get('1', {}).get('ticketsMax', 0),
            'team_2_tickets': server_json.get('snapshot', {}).get(team_score_key, {}).get('2', {}).get('tickets', 0),
            'team_2_max_tickets': server_json.get('snapshot', {}).get(team_score_key, {}).get('2', {}).get('ticketsMax', 0),
        }
        player_data = []
        team_info: dict = server_json.get('snapshot', {}).get('teamInfo', [])
        for team_id, team in team_info.items():
            faction_id = team.get('faction')
            for persona_id, player_info in team.get('players', {}).items():
                player_data.append({
                    'server_guid': server_guid,
                    'game_id': server_json.get('snapshot', {}).get('gameId'),
                    'team_id': int(team_id),
                    'faction_id': faction_id,
                    'persona_id': int(persona_id),
                    'player_name': player_info.get('name'),
                    'player_tag': player_info.get('tag'),
                    'player_score': player_info.get('score'),
                    'player_kills': player_info.get('kills'),
                    'player_deaths': player_info.get('deaths'),
                    'player_squad': player_info.get('squad'),
                    'player_role': player_info.get('role'),
                })
        return server_data, player_data

    def upsert_match(server_data: dict) -> None:
        if server_data.get('game_mode') is None:
            server_data['game_mode'] = ''
        with get_conn() as conn:
            conn.execute(sqlalchemy.text("""
                INSERT INTO match (
                    server_guid, game_id, game_mode, max_players, round_time, current_map, match_timestamp,
                    team_0_tickets, team_0_max_tickets, team_1_tickets, team_1_max_tickets,
                    team_2_tickets, team_2_max_tickets
                ) VALUES (
                    :server_guid, :game_id, :game_mode, :max_players, :round_time,
                    :current_map, :match_timestamp,
                    :team_0_tickets, :team_0_max_tickets, :team_1_tickets, :team_1_max_tickets,
                    :team_2_tickets, :team_2_max_tickets
                )
                ON CONFLICT (server_guid, game_id) DO UPDATE SET
                    round_time = EXCLUDED.round_time,
                    match_timestamp = EXCLUDED.match_timestamp,
                    team_0_tickets = EXCLUDED.team_0_tickets,
                    team_1_tickets = EXCLUDED.team_1_tickets,
                    team_2_tickets = EXCLUDED.team_2_tickets
            """), server_data)

    def upsert_match_players(players: list[dict]) -> None:
        if not players:
            return
        with get_conn() as conn:
            conn.execute(sqlalchemy.text("""
                INSERT INTO match_players (
                    server_guid, game_id, team_id, faction_id, persona_id, player_name, player_tag,
                    player_score, player_kills, player_deaths, player_squad, player_role
                ) VALUES (
                    :server_guid, :game_id, :team_id, :faction_id, :persona_id, :player_name, :player_tag,
                    :player_score, :player_kills, :player_deaths, :player_squad, :player_role
                )
                ON CONFLICT (server_guid, game_id, persona_id) DO UPDATE SET
                    player_score = EXCLUDED.player_score,
                    player_kills = EXCLUDED.player_kills,
                    player_deaths = EXCLUDED.player_deaths,
                    player_squad = EXCLUDED.player_squad,
                    player_role = EXCLUDED.player_role
            """), players)

        for p in players:
            download_user_information(p['persona_id'])

    logger.info(f"Fetching snapshot for server {server_guid}...")
    server_data, player_data = get_server_snapshot(server_guid)
    logger.debug(f"Snapshot: game_id={server_data.get('game_id')} mode={server_data.get('game_mode')} map={server_data.get('current_map')} players={len(player_data)}")
    upsert_match(server_data)
    logger.debug(f"Upserted match record for game_id={server_data.get('game_id')}")
    upsert_match_players(player_data)
    logger.info(f"Snapshot done — {len(player_data)} players upserted for server {server_guid}")


def download_user_information(persona_id: int):
    logger.info(f"Downloading user information for persona_id {persona_id}...")

    def get_persona_info(persona_id: int):
        data: dict = _get_json(f"https://battlelog.battlefield.com/bf4/warsawoverviewpopulate/{persona_id}/1/").get('data')
        persona_stats: dict = data.get('overviewStats')
        return {
            "user_id": data.get('currentUserId'),
            "persona_id": persona_id,
            "rank": persona_stats.get('rank', 0),
            "skill": persona_stats.get('skill', 0),
            "kills": persona_stats.get('kills', 0),
            "deaths": persona_stats.get('deaths', 0),
            "kd_ratio": persona_stats.get('kdRatio', 0),
            "kills_per_minute": persona_stats.get('killsPerMinute', 0),
            "num_wins": persona_stats.get('numWins', 0),
            "num_rounds": persona_stats.get('numRounds', 0),
            "num_losses": persona_stats.get('numLosses', 0),
            "score": persona_stats.get('score', 0),
            "score_per_minute": persona_stats.get('scorePerMinute', 0),
            "time_played": persona_stats.get('timePlayed', 0),
            "accuracy": persona_stats.get('accuracy', 0),
        }

    def get_personas(user_id: int) -> list[dict]:
        data = _get_json(f"https://battlelog.battlefield.com/bf4/user/overviewBoxStats/{user_id}/")
        soldiers = data.get("data", {}).get("soldiersBox", []) or []
        personas = [soldier['persona'] for soldier in soldiers]
        return [
            {
                "user_id": p["userId"],
                "persona_id": p["personaId"],
                "persona_name": p["personaName"],
                "platform": p["namespace"],
            }
            for p in personas
        ]

    def get_weapon_stats(persona_id: int) -> pd.DataFrame:
        data: dict = _get_json(f"https://battlelog.battlefield.com/bf4/warsawWeaponsPopulateStats/{persona_id}/1/stats/").get("data")
        return pd.DataFrame(data.get("mainWeaponStats") or [])

    def upsert_user_id(user_id: int):
        with get_conn() as conn:
            conn.execute(sqlalchemy.text("""
                INSERT INTO ea_user (user_id) VALUES (:user_id)
                ON CONFLICT (user_id) DO NOTHING
            """), {"user_id": user_id})

    def upsert_personas(personas: list[dict]) -> None:
        if not personas:
            return
        now = datetime.now(timezone.utc)
        with get_conn() as conn:
            conn.execute(sqlalchemy.text("""
                INSERT INTO ea_user (user_id) VALUES (:user_id)
                ON CONFLICT (user_id) DO NOTHING
            """), [{"user_id": p['user_id']} for p in personas])
            conn.execute(sqlalchemy.text("""
                INSERT INTO persona (user_id, persona_id, persona_name, platform, initial_timestamp)
                VALUES (:user_id, :persona_id, :persona_name, :platform, :initial_timestamp)
                ON CONFLICT (persona_id) DO NOTHING
            """), [{**p, 'initial_timestamp': now} for p in personas])

    def upsert_persona_info(persona_info: dict):
        with get_conn() as conn:
            conn.execute(sqlalchemy.text("""
                INSERT INTO persona_info (
                    user_id, persona_id, rank, skill, kills, deaths, kd_ratio, kills_per_minute,
                    num_wins, num_rounds, num_losses, score, score_per_minute, time_played, accuracy
                ) VALUES (
                    :user_id, :persona_id, :rank, :skill, :kills, :deaths, :kd_ratio, :kills_per_minute,
                    :num_wins, :num_rounds, :num_losses, :score, :score_per_minute, :time_played, :accuracy
                )
                ON CONFLICT (persona_id) DO UPDATE SET
                    rank = EXCLUDED.rank,
                    skill = EXCLUDED.skill,
                    kills = EXCLUDED.kills,
                    deaths = EXCLUDED.deaths,
                    kd_ratio = EXCLUDED.kd_ratio,
                    kills_per_minute = EXCLUDED.kills_per_minute,
                    num_wins = EXCLUDED.num_wins,
                    num_rounds = EXCLUDED.num_rounds,
                    num_losses = EXCLUDED.num_losses,
                    score = EXCLUDED.score,
                    score_per_minute = EXCLUDED.score_per_minute,
                    time_played = EXCLUDED.time_played,
                    accuracy = EXCLUDED.accuracy
            """), persona_info)

    def upsert_weapon_stats(persona_id: int, weapon_stats: pd.DataFrame):
        if weapon_stats.empty:
            return
        col_map = {
            'serviceStars':         'service_stars',
            'serviceStarsProgress': 'service_stars_progress',
            'categorySID':          'category_sid',
            'unlockImageConfig':    'unlock_image_config',
            'shotsFired':           'shots_fired',
            'timeEquippedDelta':    'time_equipped_delta',
            'imageConfig':          'image_config',
            'startedWith':          'started_with',
            'duplicateOf':          'duplicate_of',
            'timeEquipped':         'time_equipped',
            'killsPerMinuteDelta':  'kills_per_minute_delta',
            'shotsHit':             'shots_hit',
            'killsDelta':           'kills_delta',
        }
        df = weapon_stats.rename(columns=col_map)
        df['persona_id'] = persona_id
        df = df.astype(object).where(pd.notnull(df), None)
        if 'unlocks' in df.columns:
            df['unlocks'] = df['unlocks'].apply(lambda x: json.dumps(x) if x is not None else None)

        all_cols = [
            'persona_id', 'guid', 'slug', 'name', 'category', 'category_sid', 'code', 'type',
            'image_config', 'unlock_image_config', 'service_stars', 'service_stars_progress',
            'kills', 'kills_delta', 'kills_per_minute_delta', 'deaths', 'headshots',
            'shots_fired', 'shots_hit', 'accuracy', 'score', 'time_equipped', 'time_equipped_delta',
            'unlocked', 'started_with', 'kit', 'duplicate_of', 'weapon', 'unlocks',
        ]
        cols = [c for c in all_cols if c in df.columns]
        update_cols = [c for c in cols if c not in ('persona_id', 'guid')]

        cols_str = ', '.join(cols)
        params_str = ', '.join(f':{c}' for c in cols)
        update_str = ', '.join(f'{c} = EXCLUDED.{c}' for c in update_cols)
        rows = df[cols].to_dict('records')

        with get_conn() as conn:
            conn.execute(sqlalchemy.text(f"""
                INSERT INTO persona_weapon_stats ({cols_str})
                VALUES ({params_str})
                ON CONFLICT (persona_id, guid) DO UPDATE SET {update_str}
            """), rows)

    persona_info = get_persona_info(persona_id)
    user_id = persona_info['user_id']
    logger.debug(f"persona_id={persona_id} user_id={user_id} rank={persona_info.get('rank')} kills={persona_info.get('kills')} kd={persona_info.get('kd_ratio')}")

    personas = get_personas(user_id)
    logger.debug(f"Found {len(personas)} persona(s) on user_id={user_id}")

    weapon_stats = get_weapon_stats(persona_id)
    logger.debug(f"Fetched {len(weapon_stats)} weapon stat rows for persona_id={persona_id}")

    upsert_user_id(user_id)
    upsert_personas(personas)
    logger.debug(f"Upserted {len(personas)} persona(s) for user_id={user_id}")

    upsert_persona_info(persona_info)
    logger.debug(f"Upserted persona_info for persona_id={persona_id}")

    upsert_weapon_stats(persona_id, weapon_stats)
    logger.debug(f"Upserted weapon stats for persona_id={persona_id}")
