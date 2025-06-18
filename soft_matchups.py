#!/usr/bin/env python3
"""Find today's soft pitching matchups.

This utility fetches today's MLB schedule and ranks games by the ERA of
probable starting pitchers. A higher ERA indicates a potentially easier
matchup for opposing hitters.
"""
import requests
import pandas as pd
from datetime import date


def fetch_schedule():
    """Return today's games from the MLB stats API."""
    today = date.today()
    url = (
        "https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&date={today.strftime('%Y-%m-%d')}"
        "&hydrate=team,probablePitcher"
    )
    r = requests.get(url)
    r.raise_for_status()
    dates = r.json().get("dates", [])
    return dates[0].get("games", []) if dates else []


def fetch_pitcher_era(pid):
    """Fetch ERA for the given pitcher id. Returns None if unavailable."""
    if pid is None:
        return None
    url = (
        f"https://statsapi.mlb.com/api/v1/people/{pid}"
        "?hydrate=stats(group=pitching,type=season,season=2025,gameType=R)"
    )
    r = requests.get(url)
    r.raise_for_status()
    people = r.json().get("people", [])
    if not people:
        return None
    stats = people[0].get("stats", [])
    if not stats:
        return None
    splits = stats[0].get("splits", [])
    if not splits:
        return None
    era = splits[0].get("stat", {}).get("era")
    try:
        return float(era)
    except (TypeError, ValueError):
        return None


def main():
    games = fetch_schedule()
    rows = []
    for g in games:
        home_team = g["teams"]["home"]["team"]["name"]
        away_team = g["teams"]["away"]["team"]["name"]
        home_pitcher = g["teams"]["home"].get("probablePitcher", {})
        away_pitcher = g["teams"]["away"].get("probablePitcher", {})

        home_era = fetch_pitcher_era(home_pitcher.get("id"))
        away_era = fetch_pitcher_era(away_pitcher.get("id"))

        softness = None
        eras = [e for e in [home_era, away_era] if e is not None]
        if eras:
            softness = max(eras)

        rows.append(
            {
                "Away": away_team,
                "Home": home_team,
                "Away_Starter": away_pitcher.get("fullName", ""),
                "Away_ERA": away_era,
                "Home_Starter": home_pitcher.get("fullName", ""),
                "Home_ERA": home_era,
                "Softness": softness,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        print("No games found for today.")
        return

    df.sort_values("Softness", ascending=False, inplace=True)
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
