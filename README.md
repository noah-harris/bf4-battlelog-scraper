# bf4-battlelog-scraper
Python scraper for extracting player stats and match data from Battlefield 4's Battlelog.

## Definitions
- **Persona** — a player account. `persona_id` is an integer.
- **Server** — a game server. `server_guid` is a 36-character UUID found in the `data-guid` HTML attribute.
- A server that is offline still exists; it simply is not currently hosting a match.

---

## Endpoints

### CDN / Static Data

| Function | URL | Returns |
|---|---|---|
| `get_localization()` | `https://cdn.battlelog.com/bl-cdn/cdnprefix/develop-8950182/public/dynamic/bf4/en_US.js` | JS (text) — UI strings and labels |
| `get_warsaw_items()` | `https://cdn.battlelog.com/bl-cdn/cdnprefix/develop-8950182/public/gamedatawarsaw/warsaw.items.js` | JS (text) — weapons, gadgets, vehicle definitions |

### Servers

| Function | URL | Returns |
|---|---|---|
| `get_server_list()` | `https://battlelog.battlefield.com/bf4/servers/` | HTML — full server listing; parse with `results_table_class` |
| `get_server_page(server_guid)` | `https://battlelog.battlefield.com/bf4/servers/show/pc/{server_guid}/` | HTML — individual server detail page |
| `get_server_snapshot(server_guid)` | `https://keeper.battlelog.com/snapshot/{server_guid}` | JSON — live state: current players, map, scores, round timer |

### Persona (Player) Stats



| Function | URL | Returns |
|---|---|---|
| `get_weapon_stats(persona_id)` | `https://battlelog.battlefield.com/bf4/warsawWeaponsPopulateStats/{persona_id}/1/stats/` | JSON — per-weapon kill/accuracy stats |
| `get_index_stats(persona_id)` | `https://battlelog.battlefield.com/bf4/indexstats/{persona_id}/1` | JSON — general index stats |
| `get_overview(persona_id)` | `https://battlelog.battlefield.com/bf4/warsawoverviewpopulate/{persona_id}/1/` | JSON — Warsaw overview stats |
| `get_player_stats(persona_id)` | `https://battlelog.battlefield.com/bf4/indexstats/{persona_id}/1/?stats=1` | JSON — full player stats (timestamp busts cache) |

---

## Rate Limiting

All requests share a single `requests.Session` and pass through `_throttle()` before each call.

The default delay is **1 second ± 0.5 s** of uniform random noise, so actual inter-request gaps fall in the range `[0.5 s, 1.5 s]`.

```python
# Adjust at runtime
set_rate(base=1.5, jitter=0.3)   # 1.2 s – 1.8 s
set_rate(base=2.0, jitter=1.0)   # 1.0 s – 3.0 s
```

The jitter reduces the fingerprint of a fixed-interval scraper, which can help avoid simple rate-limit detection based on request regularity.

## Misc

