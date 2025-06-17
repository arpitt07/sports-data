#!/usr/bin/env python3
import requests
import sys
import zipfile
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
from pybaseball import statcast

# ——— Configuration ———
DAILY_API_URL = "https://zuriteapi.com/homers/api/dailyhomeruns?days=1&format=json"
MLB_STATS_API = (
    "https://statsapi.mlb.com/api/v1/stats"
    "?stats=season"
    "&sportIds=1"
    "&season=2025"
    "&group=hitting"
    "&gameType=R"
    "&playerPool=all"
    "&sortStat=homeRuns"
    "&order=desc"
    "&limit=100"
)
OUTPUT_XLSX = Path(r"C:\Users\arpit\Documents\mlb_homer_data3.xlsx")

# ——— Step 1: Fetch yesterday's homers ———
def fetch_daily():
    yesterday = date.today() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")
    resp = requests.get(DAILY_API_URL)
    resp.raise_for_status()
    runs = resp.json()
    df = pd.DataFrame([
        {
            "Date":      date_str,
            "Batter":    rec.get("batter_name"),
            "Exit_Vel":  rec.get("hit_speed"),
            "Distance":  rec.get("hit_distance"),
            "Pitch":     rec.get("pitch_name"),
            "Pitcher":   rec.get("pitcher_name"),
        }
        for rec in runs
    ])
    return df, date_str

# ——— Step 2: Fetch top 50 season HR leaders ———
def fetch_top50():
    resp = requests.get(MLB_STATS_API)
    resp.raise_for_status()
    splits = resp.json().get("stats", [])[0].get("splits", [])
    data = [
        {
            "Batter":    s["player"]["fullName"],
            "Player_ID": s["player"]["id"],
            "HRs":       s["stat"]["homeRuns"],
            "Games":     s["stat"]["gamesPlayed"],
            "AVG":       s["stat"]["avg"],
            "SLG":       s["stat"]["slg"],
            "HR%":       round(s["stat"]["homeRuns"] / (s["stat"].get("atBats") or 1), 4),
            "AB":        s["stat"].get("atBats", 0),
        }
        for s in splits
    ]
    return pd.DataFrame(data)

# ——— Step 2b: Fetch top 50 pitchers by strikeouts ———
def fetch_top50_pitchers():
    MLB_PITCHER_STATS_API = (
        "https://statsapi.mlb.com/api/v1/stats"
        "?stats=season"
        "&sportIds=1"
        "&season=2025"
        "&group=pitching"
        "&gameType=R"
        "&playerPool=all"
        "&sortStat=strikeOuts"
        "&order=desc"
        "&limit=50"
    )
    resp = requests.get(MLB_PITCHER_STATS_API)
    resp.raise_for_status()
    splits = resp.json().get("stats", [])[0].get("splits", [])
    data = [
        {
            "Pitcher":   s["player"]["fullName"],
            "Player_ID": s["player"]["id"],
            "SO":        s["stat"]["strikeOuts"],
            "ERA":       s["stat"]["era"],
            "Games":     s["stat"]["gamesPlayed"],
            "IP":        s["stat"]["inningsPitched"],
            "Wins":      s["stat"]["wins"],
            "Losses":    s["stat"]["losses"],
        }
        for s in splits
    ]
    return pd.DataFrame(data)

# ——— Compute barrel flag per MLB definition ———
def compute_barrel(df_sc):
    # Fill missing values with zero to avoid NA in comparisons
    speed = df_sc["launch_speed"].fillna(0)
    angle = df_sc["launch_angle"].fillna(0)
    # Barrel if:
    # speed >= 98 & angle between 26–30, or
    # speed >= 105 & angle between 19–26, or
    # speed >= 109 & angle between 13–19
    cond1 = (speed >= 98)  & angle.between(26, 30)
    cond2 = (speed >= 105) & angle.between(19, 26)
    cond3 = (speed >= 109) & angle.between(13, 19)
    return (cond1 | cond2 | cond3).astype(int)

# ——— Step 3: Enrich top50 with Statcast metrics ———
def enrich_top50(df_top50):
    yesterday = date.today() - timedelta(days=1)
    start_date = date(2025, 3, 28)
    end_date = yesterday
    # Fetch statcast for the season
    sc = statcast(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    # Keep only necessary cols and rename
    sc = sc[["batter","launch_speed","launch_angle"]].rename(columns={"batter":"Player_ID"})
    # Compute barrel flag
    sc["barrel_flag"] = compute_barrel(sc)
    # Aggregate by Player_ID
    barrel_map = sc.groupby("Player_ID")["barrel_flag"].mean()
    ev_map     = sc.groupby("Player_ID")["launch_speed"].mean()

    df = df_top50.copy()
    df["Barrel%"]      = df["Player_ID"].map(barrel_map)
    df["Avg_Exit_Vel"] = df["Player_ID"].map(ev_map)
    return df

# ——— Step 4: Write sheets ———
def write_sheets(df_daily, df_top50_enriched, date_str, output_xlsx=None, df_pitchers=None, df_matchups=None):
    if output_xlsx is None:
        output_xlsx = OUTPUT_XLSX
    else:
        output_xlsx = Path(output_xlsx)

    if output_xlsx.exists() and not zipfile.is_zipfile(output_xlsx):
        output_xlsx.unlink()

    mode = 'a' if output_xlsx.exists() else 'w'
    sheet_daily = f"{date_str}_HR_Hitters"
    sheet_top50 = f"{date_str}_Top_HR_Batters"
    sheet_pitchers = f"{date_str}_Top_Pitchers"
    sheet_matchups = f"{date_str}_Matchups"

    if mode == 'a':
        writer = pd.ExcelWriter(output_xlsx, engine='openpyxl', mode=mode, if_sheet_exists='replace')
    else:
        writer = pd.ExcelWriter(output_xlsx, engine='openpyxl', mode=mode)

    with writer:
        df_daily.to_excel(writer, sheet_name=sheet_daily, index=False)
        df_top50_enriched.to_excel(writer, sheet_name=sheet_top50, index=False)
        if df_pitchers is not None:
            df_pitchers.to_excel(writer, sheet_name=sheet_pitchers, index=False)
        if df_matchups is not None and not df_matchups.empty:
            df_matchups.to_excel(writer, sheet_name=sheet_matchups, index=False)
    print(f"✅ Wrote sheets '{sheet_daily}', '{sheet_top50}'"
          f"{', ' + sheet_pitchers if df_pitchers is not None else ''}"
          f"{', and ' + sheet_matchups if df_matchups is not None else ''} to {output_xlsx!r}")

def fetch_today_matchups():
    today = date.today()
    schedule_url = (
        f"https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&date={today.strftime('%Y-%m-%d')}&hydrate=team,linescore,probablePitcher"
    )
    resp = requests.get(schedule_url)
    resp.raise_for_status()
    games = resp.json().get("dates", [])[0].get("games", [])
    matchups = []
    for g in games:
        home = g["teams"]["home"]["team"]["name"]
        away = g["teams"]["away"]["team"]["name"]
        home_pitcher = g["teams"]["home"].get("probablePitcher", {})
        away_pitcher = g["teams"]["away"].get("probablePitcher", {})
        matchup = {
            "Home_Team": home,
            "Away_Team": away,
            "Home_Starter": home_pitcher.get("fullName", ""),
            "Home_Starter_ID": home_pitcher.get("id", ""),
            "Away_Starter": away_pitcher.get("fullName", ""),
            "Away_Starter_ID": away_pitcher.get("id", ""),
        }
        matchups.append(matchup)
    df = pd.DataFrame(matchups)
    # Optionally, add stats for each starter (ERA, SO, etc.)
    if not df.empty:
        pitcher_stats = fetch_top50_pitchers()
        stats_map = pitcher_stats.set_index("Player_ID").to_dict(orient="index")
        for side in ["Home", "Away"]:
            df[f"{side}_ERA"] = df[f"{side}_Starter_ID"].map(lambda pid: stats_map.get(pid, {}).get("ERA", ""))
            df[f"{side}_SO"] = df[f"{side}_Starter_ID"].map(lambda pid: stats_map.get(pid, {}).get("SO", ""))
    return df

if __name__ == "__main__":
    daily_df, date_str = fetch_daily()
    top50_df = fetch_top50()
    enriched = enrich_top50(top50_df)
    pitchers_df = fetch_top50_pitchers()
    matchups_df = fetch_today_matchups()
    output_xlsx = sys.argv[1] if len(sys.argv) > 1 else None
    write_sheets(daily_df, enriched, date_str, output_xlsx, df_pitchers=pitchers_df, df_matchups=matchups_df)