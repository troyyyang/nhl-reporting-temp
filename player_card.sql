select player_name, player_id, team,sum(games) as gp, avg(time_on_ice) as avg_toi, sum(time_on_ice) as toi, sum(assists) as assists,
avg(assists) as assistspg, sum(goals) as goals, avg(goals) as goalspg,
sum(points) as points, avg(points) as pointspg, sum(hits) as hits, avg(hits) as hitspg, sum(pim) as pims, avg(pim) as pimpg,
sum(shots) as shots, avg(shots) as shotspg, sum(power_play_goals) as ppg, avg(power_play_goals) as ppgpg, sum(power_play_points) as ppp,
avg(power_play_points) as ppppg, sum(power_play_time_on_ice) as pptoi, avg(power_play_time_on_ice) as pptoipg, sum(even_time_on_ice) as etoi,
avg(even_time_on_ice) as etoipg, sum(goals)/sum(nullif(shots,0)) as shot_pct, sum(game_winning_goals) as gwg, sum(over_time_goals) as otg, sum(short_handed_goals) as shg,
sum(short_handed_points) as shp, sum(short_handed_time_on_ice) shtoi, sum(blocked) as blocks, avg(blocked) as blockspg, sum(plus_minus) as plus_minus,
 sum(shifts) as shifts, avg(face_off_pct) as average_faceoff_pct, sum(shutouts) as shutouts, sum(saves) as saves, avg(saves) as savespg,
sum(power_play_saves) as pp_saves, sum(short_handed_saves) as sh_saves, sum(even_saves) as e_saves, sum(power_play_shots) as pp_shots, sum(even_shots) as e_shots,
sum(saves)/sum(nullif(shots_against,0)) as save_pct,
sum(goals_against) as goals_against, avg(goals_against) as gaa, sum(games_started) as goalie_games_started
from players group by 1,2,3; 
