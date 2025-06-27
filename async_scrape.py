import asyncio
import aiohttp
import requests
import re
import pandas as pd
import numpy as np
import json
import os
from itertools import chain
from collections import defaultdict
import time
import glob
from concurrent.futures import ThreadPoolExecutor
import warnings
warnings.filterwarnings('ignore')

# Load master record
record = pd.read_csv('master_record.csv')
record = record[['GAME_ID','year']]
record.drop_duplicates(inplace=True)

def get_existing_games():
    """Get list of already scraped games from existing CSV files"""
    existing_games = set()
    
    if os.path.exists('pbp_data'):
        pbp_files = glob.glob('pbp_data/*.csv')
        for file_path in pbp_files:
            filename = os.path.basename(file_path)
            if '_' in filename:
                parts = filename.replace('.csv', '').split('_')
                if len(parts) >= 2:
                    game_id = '_'.join(parts[1:])
                    existing_games.add(game_id)
    
    print(f"Found {len(existing_games)} already scraped games")
    return existing_games

def get_player_name(play):
    """Extract player name from play data"""
    if 'playerName' in play and play['playerName']:
        return play['playerName']
    elif 'playerNameI' in play and play['playerNameI']:
        return play['playerNameI']
    return ''

def is_field_goal(play):
    """Check if play is a field goal attempt"""
    return play.get('actionType') in ['2pt', '3pt']

async def fetch_json_async(session, url, game_id, data_type="PBP"):
    """Async function to fetch JSON data"""
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"HTTP {response.status} for {data_type} game {game_id}")
                return None
    except asyncio.TimeoutError:
        print(f"Timeout fetching {data_type} data for game {game_id}")
        return None
    except Exception as e:
        print(f"Error fetching {data_type} data for game {game_id}: {e}")
        return None

async def get_game_data_async(session, game_id):
    """Fetch both PBP and boxscore data concurrently"""
    pbp_url = f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_00{game_id}.json"
    boxscore_url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_00{game_id}.json"
    
    # Fetch both URLs concurrently
    pbp_task = fetch_json_async(session, pbp_url, game_id, "PBP")
    boxscore_task = fetch_json_async(session, boxscore_url, game_id, "Boxscore")
    
    pbp_data, boxscore_data = await asyncio.gather(pbp_task, boxscore_task)
    
    return pbp_data, boxscore_data

def parse_iso_clock(clock_str):
    """Convert ISO 8601 duration format (PTXMYS) to minutes and seconds"""
    if not clock_str or pd.isna(clock_str):
        return 0, 0
    match = re.match(r'PT(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?', str(clock_str))
    if match:
        minutes = int(match.group(1)) if match.group(1) else 0
        seconds = float(match.group(2)) if match.group(2) else 0.0
        return minutes, int(seconds)
    return 0, 0

def extract_starters_from_boxscore(boxscore_data):
    """Extract starting lineups from boxscore data"""
    try:
        home_starters = []
        away_starters = []
        
        home_players = boxscore_data['game']['homeTeam']['players']
        away_players = boxscore_data['game']['awayTeam']['players']
        
        for player in home_players:
            if player.get('starter') == '1':
                home_starters.append(str(player['personId']))
        
        for player in away_players:
            if player.get('starter') == '1':
                away_starters.append(str(player['personId']))
        
        all_starters = home_starters + away_starters
        return '|'.join(all_starters)
    
    except Exception as e:
        print(f"Error extracting starters from boxscore: {e}")
        return ""

def process_pbp_data(pbp_data, boxscore_data, game_id):
    """Process play-by-play data - optimized version"""
    if not pbp_data or 'game' not in pbp_data or 'actions' not in pbp_data['game']:
        return [], {}
    
    # Extract starters
    starters_on = extract_starters_from_boxscore(boxscore_data) if boxscore_data else ""
    
    # Create DataFrame from actions
    pbp_df = pd.DataFrame(pbp_data['game']['actions'])
    
    if pbp_df.empty:
        return [], {}
    
    # Filter for relevant events
    events = ['2pt', 'rebound', '3pt', 'turnover', 'steal', 'foul', 'freethrow', 'timeout', 'substitution', 'block']
    pbp_df = pbp_df[pbp_df.actionType.isin(events)]
    pbp_df = pbp_df.replace({np.nan: None})

    if pbp_df.empty:
        return [], {}

    # Vectorized clock parsing
    clock_data = pbp_df['clock'].apply(lambda x: pd.Series(parse_iso_clock(x)))
    pbp_df[['clock_minutes', 'clock_seconds']] = clock_data
    pbp_df['clock_display'] = pbp_df.apply(
        lambda row: f"{int(row['clock_minutes']):02}:{int(row['clock_seconds']):02}", axis=1
    )
    pbp_df['game_clock'] = pbp_df.apply(
        lambda row: f"Q{row['period']} {row['clock_display']}", axis=1
    )
    pbp_df['minutes_left_in_game'] = (
        (4 - (pbp_df['period'] - 1)) * 12 - 
        (12 - pbp_df['clock_minutes']) - 
        (pbp_df['clock_seconds'] / 60)
    )

    # Compute next action fields
    pbp_df['next_actionType'] = pbp_df['actionType'].shift(-1)
    pbp_df['next_shotResult'] = pbp_df['shotResult'].shift(-1)

    processed_data = []
    prev_action_type = None
    prev_shot_result = None
    team_lineups = defaultdict(set)

    players_on_list = starters_on.split('|') if starters_on else []

    # Process each play
    for idx, play in pbp_df.iterrows():
        # Previous and next action logic (unchanged)
        previous_action = None
        if prev_action_type:
            if prev_action_type in ['2pt', '3pt']:
                if prev_shot_result == 'Made':
                    previous_action = f"{prev_action_type} made"
                elif prev_shot_result == 'Missed':
                    previous_action = f"{prev_action_type} missed"
                else:
                    previous_action = prev_action_type
            else:
                previous_action = prev_action_type

        next_action = None
        next_action_type = play.get('next_actionType')
        next_shot_result = play.get('next_shotResult')
        if next_action_type:
            if next_action_type in ['2pt', '3pt']:
                if next_shot_result == 'Made':
                    next_action = f"{next_action_type} made"
                elif next_shot_result == 'Missed':
                    next_action = f"{next_action_type} missed"
                else:
                    next_action = next_action_type
            else:
                next_action = next_action_type

        # Handle substitutions
        if play.get('actionType') == 'substitution':
            person_id = str(play.get('personId'))
            sub_type = play.get('subType')
            if sub_type == 'out' and person_id in players_on_list:
                players_on_list.remove(person_id)
            elif sub_type == 'in' and person_id not in players_on_list:
                players_on_list.append(person_id)

        person_id = play.get('personId', None)
        assist_id = play.get('assistPersonId', None)
        formatted_players_on = '|'.join(sorted(players_on_list))
        
        team_id = play.get('teamId')
        if team_id and formatted_players_on:
            team_lineups[team_id].add(formatted_players_on)

        play_dict = {
            'period': play.get('period', 0),
            'clock': play.get('clock', ''),
            'clock_display': play.get('clock_display', ''),
            'game_clock': play.get('game_clock', ''),
            'minutes_left_in_game': play.get('minutes_left_in_game', 0),
            'actionNumber': play.get('actionNumber', ''),
            'actionType': play.get('actionType', ''),
            'description': play.get('description', ''),
            'qualifier': play.get('qualifiers', []),
            'playerName': get_player_name(play),
            'scoreHome': play.get('scoreHome', 0),
            'scoreAway': play.get('scoreAway', 0),
            'shotResult': play.get('shotResult', ''),
            'isFieldGoal': is_field_goal(play),
            'assisted': assist_id is not None,
            'person_id': person_id,
            'assister_id': assist_id,
            'previous_action': previous_action,
            'next_action': next_action,
            'foulDrawnPersonId': play.get('foulDrawnPersonId', ''),
            'stealPersonId': play.get('stealPersonId', ''),
            'blockPersonId': play.get('blockPersonId', ''),
            'players_on': formatted_players_on,
            'teamId': team_id,
            'game_id': game_id
        }

        processed_data.append(play_dict)
        
        prev_action_type = play.get('actionType', '')
        prev_shot_result = play.get('shotResult', '')

    return processed_data, team_lineups

def verify_existing_file(game_id, year):
    """Verify that an existing file contains valid data"""
    filename = f"pbp_data/{year}_{game_id}.csv"
    
    try:
        if not os.path.exists(filename):
            return False
            
        df = pd.read_csv(filename)
        
        if df.empty:
            return False
            
        required_columns = ['actionType', 'game_id', 'period']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return False
            
        if 'game_id' in df.columns and not str(df['game_id'].iloc[0]) == str(game_id):
            return False
            
        return True
        
    except Exception as e:
        return False

async def process_game_batch(session, game_batch, all_team_lineups):
    """Process a batch of games concurrently"""
    batch_results = []
    
    # Fetch all game data concurrently
    tasks = [get_game_data_async(session, game_id) for game_id, year in game_batch]
    game_data_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Process each game's data
    for i, ((game_id, year), game_data_result) in enumerate(zip(game_batch, game_data_results)):
        try:
            if isinstance(game_data_result, Exception):
                print(f"Failed to fetch data for game {game_id}: {game_data_result}")
                batch_results.append((game_id, False, str(game_data_result)))
                continue
                
            pbp_data, boxscore_data = game_data_result
            
            if not pbp_data:
                batch_results.append((game_id, False, "No PBP data"))
                continue
            
            # Process the data
            processed_data, team_lineups = process_pbp_data(pbp_data, boxscore_data, game_id)
            
            if not processed_data:
                batch_results.append((game_id, False, "No processed data"))
                continue
            
            # Save to CSV
            output_df = pd.DataFrame(processed_data)
            filename = f"pbp_data/{year}_{game_id}.csv"
            output_df.to_csv(filename, index=False)
            
            # Track team lineups
            for team_id, lineups in team_lineups.items():
                for lineup in lineups:
                    all_team_lineups[team_id][year].add((lineup, game_id))
            
            batch_results.append((game_id, True, "Success"))
            
        except Exception as e:
            batch_results.append((game_id, False, str(e)))
    
    return batch_results

async def main_async():
    """Main async function"""
    # Filter games since 2020-21 season
    games_to_scrape = record[record['year'] >= 2021].copy()
    print(f"Total games in master record: {len(games_to_scrape)}")
    
    # Create directories
    os.makedirs('pbp_data', exist_ok=True)
    os.makedirs('team_lineups', exist_ok=True)
    
    # Get existing games and filter
    existing_games = get_existing_games()
    
    games_to_process = []
    skipped_games = 0
    
    for idx, game_row in games_to_scrape.iterrows():
        game_id = str(game_row['GAME_ID'])
        year = game_row['year']
        
        if game_id in existing_games:
            if verify_existing_file(game_id, year):
                skipped_games += 1
                continue
            else:
                games_to_process.append((game_id, year))
        else:
            games_to_process.append((game_id, year))
    
    print(f"Skipping {skipped_games} already processed games")
    print(f"Games to process: {len(games_to_process)}")
    
    if len(games_to_process) == 0:
        print("No new games to process!")
        return
    
    # Track results
    all_team_lineups = defaultdict(lambda: defaultdict(set))
    failed_games = []
    successful_games = 0
    
    # Process games in batches
    BATCH_SIZE = 20  # Process 20 games concurrently
    MAX_CONCURRENT = 10  # Limit concurrent connections
    
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, limit_per_host=MAX_CONCURRENT)
    timeout = aiohttp.ClientTimeout(total=60, connect=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for i in range(0, len(games_to_process), BATCH_SIZE):
            batch = games_to_process[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(games_to_process) + BATCH_SIZE - 1) // BATCH_SIZE
            
            print(f"Processing batch {batch_num}/{total_batches} ({len(batch)} games)")
            
            try:
                batch_results = await process_game_batch(session, batch, all_team_lineups)
                
                # Count results
                for game_id, success, message in batch_results:
                    if success:
                        successful_games += 1
                        print(f"✓ {game_id}")
                    else:
                        failed_games.append(game_id)
                        print(f"✗ {game_id}: {message}")
                
                # Small delay between batches to be respectful
                if i + BATCH_SIZE < len(games_to_process):
                    await asyncio.sleep(1)
                    
            except Exception as e:
                print(f"Error processing batch {batch_num}: {e}")
                for game_id, year in batch:
                    failed_games.append(game_id)
    
    # Create team lineup index files (unchanged)
    print("\nCreating team lineup indexes...")
    for team_id in all_team_lineups:
        for year in all_team_lineups[team_id]:
            lineup_data = []
            lineup_games = defaultdict(list)
            
            for lineup, game_id in all_team_lineups[team_id][year]:
                lineup_games[lineup].append(game_id)
            
            for lineup, games in lineup_games.items():
                lineup_data.append({
                    'team_id': team_id,
                    'year': year,
                    'players_on': lineup,
                    'games': '|'.join(games),
                    'game_count': len(games)
                })
            
            if lineup_data:
                lineup_df = pd.DataFrame(lineup_data)
                lineup_filename = f"team_lineups/team_{team_id}_year_{year}_lineups.csv"
                lineup_df.to_csv(lineup_filename, index=False)
    
    print(f"\nScraping complete!")
    print(f"Successfully processed: {successful_games} new games")
    print(f"Skipped existing games: {skipped_games}")
    print(f"Failed games: {len(failed_games)}")
    
    if failed_games:
        print("Failed game IDs:")
        for game_id in failed_games[:10]:
            print(f"  {game_id}")
        
        failed_df = pd.DataFrame({'failed_game_ids': failed_games})
        failed_df.to_csv('failed_games.csv', index=False)

def main():
    """Entry point that runs the async main function"""
    asyncio.run(main_async())

if __name__ == "__main__":
    main()