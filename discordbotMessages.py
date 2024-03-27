import discord
import pandas as pd 
from datetime import datetime
import os
from dotenv import load_dotenv

# load environment variables
load_dotenv()

# Fetch token 
TOKEN = os.getenv('DISCORD_TOKEN')

intents = discord.Intents.all()
intents.messages = True  # allow the bot to receive message events

client = discord.Client(intents=intents)

data = pd.DataFrame(columns=['id', 'content', 'time', 'channel', 'author', 'reaction_count', 'attachments'])
processed_messages = set()  # set to store processed message IDs

@client.event
async def on_message(message):
    global data  
    if message.author == client.user:
        return
    elif message.content.startswith('_'):

        cmd = message.content.split()[0].replace("_","")
        if len(message.content.split()) > 1:
            parameters = message.content.split()[1:]

        if cmd == 'scan':
            channel = message.channel
            if isinstance(channel, discord.DMChannel):  # check if it's a DM channel
                channel_name = "Direct Message"
            elif isinstance(channel, discord.TextChannel):  # check if it's a guild text channel
                channel_name = channel.name
            else:
                channel_name = "Unknown"
                return

            # set the date after which to retrieve messages
            target_date = datetime(2024, 1, 1, 0, 0, 0)  # March 1, 2024

            async for msg in channel.history(limit=40, after=target_date):  # listen for messages in the channel after the target date

                if msg.id not in processed_messages:  # Check if message ID is already processed
                    processed_messages.add(msg.id)
                    if msg.author != client.user:
                        # count reactions
                        reaction_count = sum(reaction.count for reaction in msg.reactions)
                        
                        # extract attachments
                        attachment_info = [{'filename': attachment.filename, 'url': attachment.url} for attachment in msg.attachments]
                        
                        new_data = pd.DataFrame({
                            'content': [msg.content],
                            'time': [msg.created_at],
                            'channel': [channel_name],
                            'author': [msg.author.name],
                            'reaction_count': [reaction_count],
                            'attachments': [attachment_info],
                        })
                        data = pd.concat([data, new_data], ignore_index=True)

            file_location = "data.csv"
            data.to_csv(file_location)

client.run(TOKEN)
