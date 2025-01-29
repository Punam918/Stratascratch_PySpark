from pyspark.sql import functions as F

# Assuming you have a Spark DataFrame named twitch_sessions
# Filter users who are both viewers and streamers
viewers = twitch_sessions.filter(twitch_sessions.session_type == 'viewer').select('user_id').distinct()
streamers = twitch_sessions.filter(twitch_sessions.session_type == 'streamer').select('user_id').distinct()

# Join the viewers and streamers DataFrames on user_id to find users who are both
both_viewer_and_streamer = viewers.join(streamers, on='user_id', how='inner')

# Show the result
both_viewer_and_streamer.show()
