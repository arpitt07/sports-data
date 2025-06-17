#!/usr/bin/env python3
import requests
import pandas as pd
from pathlib import Path
from datetime import date, timedelta
import sys

# ─── CONFIG ─────────────────────────────────────────────────────────────────
START_DATE = date(2025, 3, 28)      # Opening Day
END_DATE   = date(2025, 6, 11)      # Final Day
WORKBOOK   = Path(r"C:\Users\arpit\Documents\mlb_full_data.xlsx")

# Top-75 season snapshot
TOP75_API = (
    "https://statsapi.mlb.com/api/v1/stats"
    "?stats=season&sportIds=1&season=2025"
    "&group=hitting&gameType=R&playerPool=all"
    "&sortStat=homeRuns&order=desc&limit=75"
)

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def fetch_top75_and_enrich():
    """Fetch top-75 HR leaders & enrich with Barrel% & Avg_Exit_Vel from Statcast."""
    # 1) Fetch the top-75 season snapshot
    r = requests.get(TOP75_API)
    r.raise_for_status()
    splits = r.json().get("stats", [])[0].get("splits", [])
    base = []
    for s in splits:
        pl, st = s["player"], s["stat"]
        ab = st.get("atBats", 0) or 0
        hr_pct = round(st.get("homeRuns", 0) / ab, 4) if ab > 0 else None
        base.append({
            "Player_ID": pl["id"],
            "Batter":    pl["fullName"],
            "HRs":       st.get("homeRuns"),
            "Games":     st.get("gamesPlayed"),
            "AVG":       st.get("avg"),
            "SLG":       st.get("slg"),
            "HR%":       hr_pct,
            "AB":        ab,
        })
    df = pd.DataFrame(base)

    # 2) Statcast enrichment: pass ISO-format strings instead of date objects
    from pybaseball import statcast
    sc = statcast(
        START_DATE.strftime("%Y-%m-%d"),
        END_DATE.strftime("%Y-%m-%d"),
    )

    # compute barrel flag by MLB definition
    sp = sc["launch_speed"].fillna(0)
    ag = sc["launch_angle"].fillna(0)
    cond1 = (sp >= 98)  & ag.between(26, 30)
    cond2 = (sp >= 105) & ag.between(19, 26)
    cond3 = (sp >= 109) & ag.between(13, 19)
    sc["barrel_flag"] = (cond1 | cond2 | cond3).astype(int)

    # aggregate by the numeric batter ID
    barrel_map = sc.groupby("batter")["barrel_flag"].mean().rename("Barrel%")
    ev_map     = sc.groupby("batter")["launch_speed"].mean().rename("Avg_Exit_Vel")

    df["Barrel%"]      = df["Player_ID"].map(barrel_map)
    df["Avg_Exit_Vel"] = df["Player_ID"].map(ev_map)

    return df


def fetch_daily_homers_for(dstr):
    """Fetch all HR events on date dstr (YYYY-MM-DD) via schedule & live feed."""
    sched = requests.get(
        f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={dstr}"
    ).json().get("dates", [])
    if not sched:
        return pd.DataFrame()

    gamePks = [g["gamePk"] for g in sched[0]["games"]]
    rows = []

    for pk in gamePks:
        feed = requests.get(
            f"https://statsapi.mlb.com/api/v1.1/game/{pk}/feed/live"
        ).json()
        for play in feed["liveData"]["plays"]["allPlays"]:
            # We still filter on the top‐level eventType
            if play["result"]["eventType"] != "home_run":
                continue

            bat = play["matchup"]["batter"]
            pit = play["matchup"]["pitcher"]
            pitch = play["matchup"].get("pitchType")

            # Find the actual home_run playEvent
            exit_vel = None
            distance = None
            for ev in play.get("playEvents", []):
                details = ev.get("details", {}).get("type", {})
                if details.get("description") == "home_run":
                    hd = ev.get("hitData", {}) or {}
                    exit_vel = hd.get("launchSpeed")
                    distance = hd.get("totalDistance")
                    break

            rows.append({
                "Date":     dstr,
                "Batter":   bat.get("fullName"),
                "Exit_Vel": exit_vel,
                "Distance": distance,
                "Pitch":    pitch,
                "Pitcher":  pit.get("fullName"),
            })

    return pd.DataFrame(rows)



def write_sheet(df, name, mode):
    kw = {"index":False}
    if mode=="w":
        with pd.ExcelWriter(WORKBOOK, engine="openpyxl", mode="w") as ew:
            df.to_excel(ew, sheet_name=name, **kw)
    else:
        with pd.ExcelWriter(WORKBOOK, engine="openpyxl",
                            mode="a", if_sheet_exists="replace") as ew:
            df.to_excel(ew, sheet_name=name, **kw)

# ─── MAIN ───────────────────────────────────────────────────────────────────

def main():
    # 1) Top-75 sheet
    top75 = fetch_top75_and_enrich()
    # create fresh workbook
    if WORKBOOK.exists(): WORKBOOK.unlink()
    write_sheet(top75, "data_Top_HR_Batters", mode="w")
    print("✅ Wrote sheet data_Top_HR_Batters")

    # 2) one sheet per day
    today = START_DATE
    while today <= END_DATE:
        dstr = today.strftime("%Y-%m-%d")
        df = fetch_daily_homers_for(dstr)
        name = f"{dstr}_HR_Hitters"
        if df.empty:
            print(f"— no homers on {dstr}, skipping")
        else:
            write_sheet(df, name, mode="a")
            print(f"✅ Wrote sheet {name} ({len(df)} rows)")
        today += timedelta(days=1)

if __name__=="__main__":
    main()
