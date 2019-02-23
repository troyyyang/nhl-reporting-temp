from datetime import datetime
import pandas as pd
import requests
import re
from collections import OrderedDict
from sqlalchemy import create_engine



base = 'https://statsapi.web.nhl.com'

# cleaning utility
def clean_column(column):
    string = re.sub('__c', '', column)
    string = re.sub('_', '', string)
    string = re.sub('(\W[A-Z])', lambda m: m.group(0).lower(), string)
    string = re.sub('([A-Z]+)', r'_\1',string).lower()
    if string[0] == '_':
        string = string[1:]
    return(string)


# returns list of games that have occured in the NHL for this season to date
def get_game_list():

    games = []
    today = datetime.today().strftime('%Y-%m-%d')
    sched_url = '/api/v1/schedule?startDate=2018-10-03&endDate=' + today
    url = base + sched_url
    req = requests.get(url = url)
    data = req.json()
    for dt in data['dates']:
        for game in dt['games']:
            games.append(game['link'])

    return(games)

def get_player_list():

    teams =[1,10,12,13,14,15,16,17,18,19,2,20,21,22,23,24,25,26,28,29,3,30,4,5,52,53,54,6,7,8,9]
    players = []
    for team in teams:
        url = 'https://statsapi.web.nhl.com/api/v1/teams/' +str(team) +'/roster'
        js = requests.get(url = url)
        data = js.json()
        for player in data['roster']:
            players.append(player['person']['id'])

    return(players)


def get_game_data(games):

    all_game_data = []

    for game in games:
        url = base + game
        js = requests.get(url = url)
        data = js.json()
        game_data = {}
        game_data.update({'game_id': data['gamePk']})
        game_data.update(data['gameData']['datetime'])
        away = {k: data['gameData']['teams']['away'][k] for k in ('name', 'link')}
        away.update(data['liveData']['boxscore']['teams']['away']['teamStats']['teamSkaterStats'])
        away = {'away_team_'+ k: v for k, v in away.items()}
        game_data.update(away)
        home = {k: data['gameData']['teams']['home'][k] for k in ('name', 'link')}
        home.update(data['liveData']['boxscore']['teams']['home']['teamStats']['teamSkaterStats'])
        home = {'home_team_'+ k: v for k, v in home.items()}
        game_data.update(home)
        decisions = data['liveData']['decisions']
        for k,v in decisions.items():
            dec = k
            player = {k: v[k] for k in('fullName', 'link')}
            player = {dec+'_'+k: v for k,v in player.items()}
            game_data.update(player)
        officials = data['liveData']['boxscore']['officials']
        i = 0
        for official in officials:
            i+=1
            o = official['official']
            o = {k: o[k] for k in('fullName', 'link')}
            o = {'official_'+str(i)+'_'+k: v for k,v in o.items()}
            game_data.update(o)
        game_data = OrderedDict(game_data)
        all_game_data.append(game_data)
    return(all_game_data)


def get_player_data(all_player_list):


    all_player_data = []

    for player in all_player_list:
        player_url = '/api/v1/people/' + str(player) + '/stats?stats=gameLog&season=20182019'
        url = base + player_url
        js = requests.get(url = url)
        data = js.json()
        logs = data['stats'][0]['splits']
        player_info = requests.get(url = base + '/api/v1/people/' + str(player)).json()
        name = player_info['people'][0]['fullName']
        position = player_info['people'][0]['primaryPosition']['abbreviation']

        for game in logs:
            player_log = {}
            player_log.update({'playerName': name})

            player_log.update({'playerId':player})
            player_log.update({'team':game['team']['name']})
            player_log.update({'gameId':game['game']['gamePk']})
            player_log.update({'gameDate':game['date']})
            player_log.update(game['stat'])
            player_log = OrderedDict(player_log)
            all_player_data.append(player_log)

    return(all_player_data)

def refresh_game_data(all_game_data):


    engine = create_engine('postgresql://postgres:password@localhost:5432/nhl')
    table_name = 'games'

    games = pd.DataFrame(all_game_data)

    games = games[['game_id', 'dateTime','home_team_name','home_team_goals', 'away_team_name','away_team_goals', 'away_team_link',  'away_team_pim', 'away_team_shots', 'away_team_powerPlayPercentage', 'away_team_powerPlayGoals', 'away_team_powerPlayOpportunities', 'away_team_faceOffWinPercentage', 'away_team_blocked', 'away_team_takeaways', 'away_team_giveaways', 'away_team_hits',  'home_team_link',  'home_team_pim', 'home_team_shots', 'home_team_powerPlayPercentage', 'home_team_powerPlayGoals', 'home_team_powerPlayOpportunities', 'home_team_faceOffWinPercentage', 'home_team_blocked', 'home_team_takeaways', 'home_team_giveaways', 'home_team_hits', 'endDateTime', 'winner_fullName', 'winner_link', 'loser_fullName', 'loser_link', 'firstStar_fullName', 'firstStar_link', 'secondStar_fullName', 'secondStar_link', 'thirdStar_fullName', 'thirdStar_link', 'official_1_fullName', 'official_1_link', 'official_2_fullName', 'official_2_link', 'official_3_fullName', 'official_3_link', 'official_4_fullName', 'official_4_link']]
    games = games.rename(columns=clean_column)
    games.to_sql(table_name, engine, index = False, if_exists = 'replace')

def time_to_dec(time):
    if pd.isnull(time):
        return
    (m,s) = time.split(':')
    dec = int(m) + float(int(s)/60)
    return(dec)

def refresh_player_data(all_player_data):


    engine = create_engine('postgresql://postgres:password@localhost:5432/nhl')
    table_name = 'players'

    players = pd.DataFrame(all_player_data)

    #players = players[['playerName', 'playerId', 'team', 'gameId', 'gameDate', 'timeOnIce', 'assists', 'goals', 'pim', 'shots', 'games', 'hits', 'powerPlayGoals', 'powerPlayPoints', 'powerPlayTimeOnIce', 'evenTimeOnIce', 'penaltyMinutes', 'faceOffPct', 'shotPct', 'gameWinningGoals', 'overTimeGoals', 'shortHandedGoals', 'shortHandedPoints', 'shortHandedTimeOnIce', 'blocked', 'plusMinus', 'points', 'shifts']]

    toi = [col for col in players.columns if ('TimeOnIce' in col or 'timeOnIce' in col)]
    players[toi] = players[toi].applymap(time_to_dec)
    players = players.rename(columns=clean_column)
    players.to_sql(table_name, engine, index = False, if_exists = 'replace')


#refresh_game_data(get_game_data(get_game_list()))
a = get_player_list()
b = get_player_data(a)
refresh_player_data(b)
