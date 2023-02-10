#!/usr/bin/env python3
import os
import time
import re
import pandas
from pandas import DataFrame
from pandas import Series

import pygsheets

from oauth2client.service_account import ServiceAccountCredentials

import slack_sdk
from slack_sdk import WebClient

# instantiate Slack client
sc = WebClient(os.environ.get('SLACK_BOT_TOKEN'))

# Get google sheet credentials
spreadsheetKey = os.environ.get('GSHEET_KEY') 
jsonKey = os.environ.get('GSHEET_JSON_KEY') 
wksName = os.environ.get('GSHEET_WKS_NAME') 


# starterbot's user ID in Slack: value is assigned after the bot starts up
starterbot_id = None

def write_to_gsheet(service_file_path, spreadsheet_id, sheet_name, data_df):
    """
    this function takes data_df and writes it under spreadsheet_id
    and sheet_name using your credentials under service_file_path
    """
    gc = pygsheets.authorize(service_file=service_file_path)
    sh = gc.open_by_key(spreadsheet_id)
    try:
        sh.add_worksheet(sheet_name)
    except:
        pass
    wks_write = sh.worksheet_by_title(sheet_name)
    wks_write.clear('A1',None,'*')
    wks_write.set_dataframe(data_df, (0,0), copy_index=True )

def CreateUserMentionList( users, msgText ):
    '''
    Function to parse message for user mentions
    '''
    results = re.findall( "\<@(.{11})\>", msgText )

    # Remove duplicates
    results = list( set( results ) )
    return results

def UpdateGrindTotalsDataFrame( users, userId, grindTotals, timeStamp=0,
                                mentions=0, posts=0):
    '''
    Function to update the grindTotal dataFrame
    '''
    if users[ userId ] in grindTotals.index.values.tolist():
        msgTimeStamp = grindTotals.at[ users[ userId ], 'TimeStamp' ]
        grindTotals.at[ users[ userId ], 'Total' ] += 1
        grindTotals.at[ users[ userId ], 'Posts' ] += posts
        grindTotals.at[ users[ userId ], 'Mentions' ] += mentions

        # Only update timestamp if there is one
        if timeStamp:
            grindTotals.at[ users[ userId ], 'TimeStamp' ] = float( timeStamp )
    else:
        newRow = DataFrame({'Highest React': [ 0 ],
                        'Mentions': [ mentions ],
                        'Posts': [ posts ],
                        'Total': [ 1 ],
                        'TimeStamp': float( timeStamp ) },
                        index=[ users[ userId ] ] )
        grindTotals = pandas.concat( [ grindTotals, newRow ] )
    return grindTotals

def MessageAlreadyCounted( users, message, grindTotals ):
    '''
    Function to check if message is already counted. If it has been counted
    return true
    '''
    userId = message[ 'user' ]
    if users[ userId ] in grindTotals.index.values.tolist():
        dataFrameTimeStamp = grindTotals.at[ users[ userId ], 'TimeStamp' ]
        if float( message[ 'ts'] ) <= dataFrameTimeStamp:
           return True
    return False 


if __name__ == "__main__":
    # Get dict of channels
    channelsApi = sc.api_call("conversations.list")
    channels = channelsApi.data['channels']

    # Get channel id by iterating through list of channels 
    grindChannelId = None
    for channel in channels:
        if channel[ 'name' ] == 'grind-22':
            grindChannelId = channel['id']

    # Get all messages sent into grind channel
    grindHistory = sc.conversations_history(channel=grindChannelId, limit=200 )
    grindLength = len( grindHistory[ 'messages' ] )

    # Get user data to make ids to names
    usersList = sc.api_call("users.list")
    users = {}
    for user in usersList['members']:
        users[user['id']] = user['profile']['first_name'] + ' ' + user['profile']['last_name']

    # Now create DataFrame of grind posts. Get results from csv if file exists,
    # otherwise create file
    if os.path.isfile('Total_Grind.csv'):
        grindTotals = pandas.read_csv( "Total_Grind.csv", index_col=0 )
    else:
        grindTotals = DataFrame()

    # Need to iterate from oldest to newest so we can compare timestamps
    # correctly
    for message in reversed( grindHistory['messages'] ):
        # Check if message is already counted
        if MessageAlreadyCounted( users, message, grindTotals ):
            continue

        # First deal with message poster
        grindTotals = UpdateGrindTotalsDataFrame( users, message['user'],
                                                  grindTotals,
                                                  timeStamp=message[ 'ts' ],
                                                  mentions=0, posts=1 )

        # Deal with mentions
        mentions = CreateUserMentionList( users, message[ 'text' ] )
        for mention in mentions:
            grindTotals = UpdateGrindTotalsDataFrame( users, mention, grindTotals,
                                                      mentions=1, posts=0 )

    # Now update csv
    grindTotals.to_csv( "Total_Grind.csv" )
    grindTotals = grindTotals.sort_values( 'Total', ascending=False )

    # Publish to google sheets
    write_to_gsheet( jsonKey, spreadsheetKey, wksName,
                     grindTotals )

    print( grindTotals )
