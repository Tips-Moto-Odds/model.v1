import pandas as pd
import requests
import mariadb
import os
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestClassifier
from bs4 import BeautifulSoup

# model imports
from constants import url_list

# preconfigure
load_dotenv()


def train_model():
    # team_codes = {team: code for code, team in enumerate(team_names.astype('category').cat.categories)}

    for n in range(len(team_names)):
        team_codes[team_names[n]] = n

    for df in df_list:
        df['home_code'] = df['HomeTeam'].apply(lambda team_name: team_codes.get(team_name))
        df['away_code'] = df['AwayTeam'].apply(lambda team_name: team_codes.get(team_name))
        df['target'] = df["FTR"].map({"H": 1, "D": 0, "A": -1})
        df['hour'] = df['Time'].str.replace(':.+', '', regex=True).astype(int)

    df = pd.concat(df_list)

    rf = RandomForestClassifier(n_estimators=100, min_samples_split=5, random_state=1)

    predictors = ['home_code', 'away_code', 'hour']

    rf.fit(df[predictors], df['target'])

    return rf


n = 0

for url_name, url in url_list.items():
    file_name = f'./data/E{n}.csv'
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_name, 'wb') as file:
            file.write(response.content)
        n += 1
    else:
        print(f'Failed to download file {n + 1}. Status code: {response.status_code}')

df0 = pd.read_csv('data/E0.csv')
df1 = pd.read_csv('data/E1.csv')
df2 = pd.read_csv('data/E2.csv')
df3 = pd.read_csv('data/E3.csv')
df4 = pd.read_csv('data/E4.csv')
df5 = pd.read_csv('data/E5.csv')
df6 = pd.read_csv('data/E6.csv')
df7 = pd.read_csv('data/E7.csv')
df8 = pd.read_csv('data/E8.csv')
df9 = pd.read_csv('data/E9.csv')
df10 = pd.read_csv('data/E10.csv')
df11 = pd.read_csv('data/E11.csv')
df12 = pd.read_csv('data/E12.csv')

keep_columns = ['Time', 'HomeTeam', 'AwayTeam', 'FTR']

df0 = df0[keep_columns]
df1 = df1[keep_columns]
df2 = df2[keep_columns]
df3 = df3[keep_columns]
df4 = df4[keep_columns]
df5 = df5[keep_columns]
df6 = df6[keep_columns]
df7 = df7[keep_columns]
df8 = df8[keep_columns]
df9 = df9[keep_columns]
df10 = df10[keep_columns]
df11 = df11[keep_columns]
df12 = df12[keep_columns]

df_list = [df0, df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12]
team_names = []
team_codes = {}

for df in df_list:
    unique_teams = df['HomeTeam'].unique()
    for team in unique_teams:
        team_names.append(team)

rf = train_model()

# Scrape upcoming matches
# TODO: uncomment for production
# url_list = [
#     "https://www.skysports.com/premier-league-fixtures",
#     "https://www.skysports.com/league-1-fixtures",
#     "https://www.skysports.com/league-2-fixtures",
#     "https://www.skysports.com/la-liga-fixtures",
#     "https://www.skysports.com/bundesliga-fixtures",
#     "https://www.skysports.com/serie-a-fixtures",
#     "https://www.skysports.com/ligue-1-fixtures",
#     "https://www.skysports.com/carabao-cup-fixtures"
# ]

url_list = [
    "https://www.skysports.com/premier-league-results/2021-22",
    # "https://www.skysports.com/league-1-fixtures/2022-23",
    # "https://www.skysports.com/league-2-fixtures/2022-23",
    # "https://www.skysports.com/la-liga-fixtures/2022-23",
    # "https://www.skysports.com/bundesliga-fixtures/2022-23",
    # "https://www.skysports.com/serie-a-fixtures/2022-23",
    # "https://www.skysports.com/ligue-1-fixtures/2022-23",
    # "https://www.skysports.com/carabao-cup-fixtures/2022-23"
]

match_data = []

# scraping data from the websites. Add match objects to list
for url in url_list:
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    match_containers = soup.find_all("div", class_="fixres__item")

    for container in match_containers:
        # Find the participant elements (one for each team)
        participant_elements = container.find_all("span", class_="matches__participant")
        # Extract data for each participant (assuming order is home then away)
        home_team = participant_elements[0].find("span", class_="swap-text__target").text.strip()
        away_team = participant_elements[1].find("span", class_="swap-text__target").text.strip()

        match_info = {
            "time": container.find("span", class_="matches__date").text.strip(),
            "home_team": home_team,
            "away_team": away_team,
        }

        match_data.append(match_info)

# append match odds to each match object
for match in match_data:

    if match['home_team'] not in team_names or match['away_team'] not in team_names:
        team_names.append(match['home_team'])
        team_names.append(match['away_team'])
        rf = train_model()

    this_match = pd.DataFrame({
        'home_code': [team_codes.get(match['home_team'])],
        'away_code': [team_codes.get(match['away_team'])],
        'hour': match['time'][:2]
    })

    prediction = rf.predict(this_match)

    probabilities = rf.predict_proba(this_match)

    loss, draw, win = 1 / probabilities[0][0], 1 / probabilities[0][1], 1 / probabilities[0][2]

    match['FTR'], match['H'], match['D'], match['A'] = prediction[0], win, draw, loss

cursor = None
conn = None

# Send match odds to server
try:
    conn = mariadb.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USERNAME'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_DATABASE'),
        port=3306
    )
    cursor = conn.cursor()

except mariadb.Error as err:
    print(f"Failed to connect to MariaDB: {err}")

# TODO: Copy this code to final notebooks
for match in match_data:
    home_team = match['home_team']
    away_team = match['away_team']
    FTR = int(match['FTR'])
    home = round(float(match['H']), 2)
    draw = round(float(match['D']), 2)
    away = round(float(match['A']), 2)

    sql = """INSERT INTO tips (home_teams, away_teams, home_odds, away_odds, draw_odds, predictions)
             VALUES (%s, %s, %s, %s, %s, %s)"""

    cursor.execute(sql, (home_team, away_team, home, away, draw, FTR))

# Commit the transaction if necessary
conn.commit()

conn.close()
