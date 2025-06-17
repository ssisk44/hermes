import ast
import json
import sqlite3
import pandas as pd
import requests
import os
from dotenv import load_dotenv

load_dotenv()


# TODO: parse triple inset json
# def brkdwn_inset_json(
#         df_obj,
#         col_nm
# ):
#     # Convert string to actual Python dict
#     df_obj[col_nm] = df_obj[col_nm].apply(ast.literal_eval)
#
#     # Normalize the top-level dictionary
#     df_expanded = pd.json_normalize(df_obj[col_nm])
#
#     return df_expanded


def api_get_schedule(
        year=2025,
        game_type="REG"
):
    url = f"https://api.sportradar.com/mlb/trial/v8/en/games/{year}/{game_type}/schedule.json"

    headers = {
        "accept": "application/json",
        "x-api-key": os.getenv("SPORTS_RADAR_API_KEY")
    }

    # parse primary columns
    response = requests.get(url, headers=headers)
    parsed_content = json.loads(response.text)
    df = pd.DataFrame(parsed_content["games"])

    # Remove irrelevant columns
    # TODO: Remove rescheduled games if partially played? for now keep all and drop ,rescheduled,parent_id
    df = df.drop(
        [
            'status', 'coverage', 'attendance', 'duration',
            'entry_mode', 'reference', 'broadcasts', 'venue',
            'home', 'away', 'rescheduled', 'parent_id'
        ],
        axis=1
    )

    df = df.sort_values(by='scheduled')

    # TODO: parse triple inset json
    # df = brkdwn_inset_json(
    #     df_obj=df,
    #     col_nm="venue"
    # )

    # Connect to SQLite database (creates the file if it doesn't exist)
    conn = sqlite3.connect('C:/Users/samue/PycharmProjects/hermes/MLB/db/mlb.db')

    # Write DataFrame to a SQL table
    df.to_sql('schedule', conn, if_exists='replace', index=False)

    # Close the connection
    conn.close()

api_get_schedule()