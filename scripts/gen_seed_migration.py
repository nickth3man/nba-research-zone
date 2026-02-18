"""Generate migration 0005 to seed franchise, season, and team tables."""

from pathlib import Path

teams = [
    (1610612737, "Hawks", "Atlanta", "ATL", "East", "Southeast"),
    (1610612738, "Celtics", "Boston", "BOS", "East", "Atlantic"),
    (1610612739, "Cavaliers", "Cleveland", "CLE", "East", "Central"),
    (1610612740, "Pelicans", "New Orleans", "NOP", "West", "Southwest"),
    (1610612741, "Bulls", "Chicago", "CHI", "East", "Central"),
    (1610612742, "Mavericks", "Dallas", "DAL", "West", "Southwest"),
    (1610612743, "Nuggets", "Denver", "DEN", "West", "Northwest"),
    (1610612744, "Warriors", "San Francisco", "GSW", "West", "Pacific"),
    (1610612745, "Rockets", "Houston", "HOU", "West", "Southwest"),
    (1610612746, "Clippers", "Los Angeles", "LAC", "West", "Pacific"),
    (1610612747, "Lakers", "Los Angeles", "LAL", "West", "Pacific"),
    (1610612748, "Heat", "Miami", "MIA", "East", "Southeast"),
    (1610612749, "Bucks", "Milwaukee", "MIL", "East", "Central"),
    (1610612750, "Timberwolves", "Minnesota", "MIN", "West", "Northwest"),
    (1610612751, "Nets", "Brooklyn", "BKN", "East", "Atlantic"),
    (1610612752, "Knicks", "New York", "NYK", "East", "Atlantic"),
    (1610612753, "Magic", "Orlando", "ORL", "East", "Southeast"),
    (1610612754, "Pacers", "Indiana", "IND", "East", "Central"),
    (1610612755, "76ers", "Philadelphia", "PHI", "East", "Atlantic"),
    (1610612756, "Suns", "Phoenix", "PHX", "West", "Pacific"),
    (1610612757, "Trail Blazers", "Portland", "POR", "West", "Northwest"),
    (1610612758, "Kings", "Sacramento", "SAC", "West", "Pacific"),
    (1610612759, "Spurs", "San Antonio", "SAS", "West", "Southwest"),
    (1610612760, "Thunder", "Oklahoma City", "OKC", "West", "Northwest"),
    (1610612761, "Raptors", "Toronto", "TOR", "East", "Atlantic"),
    (1610612762, "Jazz", "Utah", "UTA", "West", "Northwest"),
    (1610612763, "Grizzlies", "Memphis", "MEM", "West", "Southwest"),
    (1610612764, "Wizards", "Washington", "WAS", "East", "Southeast"),
    (1610612765, "Pistons", "Detroit", "DET", "East", "Central"),
    (1610612766, "Hornets", "Charlotte", "CHA", "East", "Southeast"),
]

franchise_meta = {
    1610612737: 1946,
    1610612738: 1946,
    1610612739: 1970,
    1610612740: 2002,
    1610612741: 1966,
    1610612742: 1980,
    1610612743: 1967,
    1610612744: 1946,
    1610612745: 1967,
    1610612746: 1970,
    1610612747: 1947,
    1610612748: 1988,
    1610612749: 1968,
    1610612750: 1989,
    1610612751: 1967,
    1610612752: 1946,
    1610612753: 1989,
    1610612754: 1967,
    1610612755: 1946,
    1610612756: 1968,
    1610612757: 1970,
    1610612758: 1945,
    1610612759: 1967,
    1610612760: 1967,
    1610612761: 1995,
    1610612762: 1974,
    1610612763: 1995,
    1610612764: 1961,
    1610612765: 1941,
    1610612766: 1988,
}

seasons = [
    (2013, "2013-14", 82),
    (2014, "2014-15", 82),
    (2015, "2015-16", 82),
    (2016, "2016-17", 82),
    (2017, "2017-18", 82),
    (2018, "2018-19", 82),
    (2019, "2019-20", 72),
    (2020, "2020-21", 72),
    (2021, "2021-22", 82),
    (2022, "2022-23", 82),
    (2023, "2023-24", 82),
    (2024, "2024-25", 82),
]

lines = []
lines.append("-- Seed NBA franchises, seasons (2013-2024), and team records")
lines.append("-- Populates franchise/season/team so FK constraints are satisfied")
lines.append("")

# Franchises
lines.append("-- FRANCHISES")
lines.append("INSERT OR IGNORE INTO franchise")
lines.append(
    "    (franchise_id, nba_franchise_id, current_team_name, current_city, abbreviation, conference, division, founded_year, league_id)"
)
lines.append("VALUES")
frows = []
for tid, name, city, abbr, conf, div in teams:
    yr = franchise_meta[tid]
    frows.append(
        f"    ({tid}, {tid}, '{name}', '{city}', '{abbr}', '{conf}', '{div}', {yr}, 'NBA')"
    )
lines.append(",\n".join(frows) + ";")
lines.append("")

# Seasons
lines.append("-- SEASONS")
lines.append(
    "INSERT OR IGNORE INTO season (season_id, league_id, season_label, games_per_team) VALUES"
)
srows = []
for sid, label, gpt in seasons:
    srows.append(f"    ({sid}, 'NBA', '{label}', {gpt})")
lines.append(",\n".join(srows) + ";")
lines.append("")

# Teams
lines.append("-- TEAM rows (one per franchise per season)")
lines.append("INSERT OR IGNORE INTO team")
lines.append(
    "    (team_id, franchise_id, season_id, team_name, city, abbreviation, conference, division)"
)
lines.append("VALUES")
trows = []
for sid, _label, _ in seasons:
    for tid, name, city, abbr, conf, div in teams:
        trows.append(f"    ({tid}, {tid}, {sid}, '{name}', '{city}', '{abbr}', '{conf}', '{div}')")
lines.append(",\n".join(trows) + ";")

content = "\n".join(lines) + "\n"
out = "migrations/0005_seed_teams_and_seasons.sql"
with Path(out).open("w", encoding="utf-8") as f:
    f.write(content)
print(f"Written {out}: {len(trows)} team rows, {len(frows)} franchises, {len(srows)} seasons")
