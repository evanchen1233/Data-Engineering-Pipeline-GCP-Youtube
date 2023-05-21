import pandas as pd
from google.cloud import bigquery

def run_youtube_etl():
    
    # load files from cloud strage
    url = "https://storage.googleapis.com/youtube_data_engineering/USvideos.csv"
    df = pd.read_csv(url)

    url2 = "https://storage.googleapis.com/youtube_data_engineering/US_category_id.json"
    df2 = pd.read_json(url2)

    # object to string/datetime
    df['trending_date'] = pd.to_datetime(df['trending_date'], format='%y.%d.%m')
    df['publish_time'] = pd.to_datetime(df['publish_time']).dt.tz_convert(None)
    df['video_id'] = df['video_id'].astype("string")
    df['description'] = df['description'].astype("string")
    df['title'] = df['title'].astype("string")
    df['channel_title'] = df['channel_title'].astype("string")
    df['tags'] = df['tags'].astype("string")
    df['thumbnail_link'] = df['thumbnail_link'].astype("string")

    # Add fact_id as SURROGATE Key
    df = df.drop_duplicates().reset_index(drop=True)
    df['fact_id'] = df.index
    # rename category_id
    df = df.rename(columns={"category_id": "category_no"}, errors="raise")
    # add column tags_count
    df['tag_count'] = df['tags'].astype('string').apply(lambda x: len(x.split('|')))

    # datetime_dim
    datetime_dim = df[['trending_date','publish_time']].drop_duplicates().reset_index(drop=True)

    datetime_dim['trending_date'] = datetime_dim['trending_date']
    datetime_dim['trending_day'] = datetime_dim['trending_date'].dt.day
    datetime_dim['trending_month'] = datetime_dim['trending_date'].dt.month
    datetime_dim['trending_year'] = datetime_dim['trending_date'].dt.year
    datetime_dim['trending_weekday'] = datetime_dim['trending_date'].dt.weekday
    datetime_dim['trending_weekdayname'] = datetime_dim['trending_date'].dt.day_name().astype("string")

    datetime_dim['publish_time'] = datetime_dim['publish_time']
    datetime_dim['publish_hour'] = datetime_dim['publish_time'].dt.hour
    datetime_dim['publish_day'] = datetime_dim['publish_time'].dt.day
    datetime_dim['publish_month'] = datetime_dim['publish_time'].dt.month
    datetime_dim['publish_year'] = datetime_dim['publish_time'].dt.year
    datetime_dim['publish_weekday'] = datetime_dim['publish_time'].dt.weekday
    datetime_dim['publish_weekdayname'] = datetime_dim['publish_time'].dt.day_name().astype("string")

    datetime_dim['datetime_id'] = datetime_dim.index
    # order columns 
    datetime_dim = datetime_dim[['datetime_id', 'trending_date', 'trending_day', 'trending_month', 'trending_year', 'trending_weekday', 'trending_weekdayname',
                                 'publish_time', 'publish_hour', 'publish_day', 'publish_month', 'publish_year', 'publish_weekday','publish_weekdayname']]

    # category_dim, match category id and category name with the json file
    category_dict = {}
    for item in df2['items']:
        category_dict[item['id']] = item['snippet']['title']

    category_dim = df[['category_no']].drop_duplicates().reset_index(drop=True)
    category_dim['category_id'] = category_dim.index
    category_dim['category_title'] = category_dim['category_no'].astype('string').map(category_dict)
    category_dim = category_dim[['category_id', 'category_no', 'category_title']]

    # title_dim
    title_dim = df[['title']].drop_duplicates().reset_index(drop=True)
    title_dim['title_id'] = title_dim.index
    title_dim = title_dim[['title_id','title']]

    # channel_dim
    channel_dim = df[['channel_title']].drop_duplicates().reset_index(drop=True)
    channel_dim['channel_id'] = channel_dim.index
    channel_dim = channel_dim[['channel_id','channel_title']]

    # tags_dim
    tags_dim = df[['tags']].drop_duplicates().reset_index(drop=True)
    tags_dim['tags_id'] = tags_dim.index
    tags_dim = tags_dim[['tags_id','tags']]

    # videoDesc_dim
    videoDesc_dim = df[['description']].drop_duplicates().reset_index(drop=True)
    videoDesc_dim['videoDesc_id'] = videoDesc_dim.index
    videoDesc_dim = videoDesc_dim[['videoDesc_id','description']]

    # settings_dim
    settings_dim = df[['comments_disabled', 'ratings_disabled', 'video_error_or_removed']].drop_duplicates().reset_index(drop=True)
    settings_dim['settings_id'] = settings_dim.index

    settings_dim = settings_dim[['settings_id','comments_disabled', 'ratings_disabled', 'video_error_or_removed']]

    # thumbnail_link_dim
    thumbnail_link_dim = df[['thumbnail_link']].drop_duplicates().reset_index(drop=True)
    thumbnail_link_dim['thumbnail_link_id'] = thumbnail_link_dim.index
    thumbnail_link_dim = thumbnail_link_dim[['thumbnail_link_id','thumbnail_link']]

    # fact_table
    fact_table = df.merge(datetime_dim, how = "left") \
                 .merge(category_dim,  how = "left") \
                 .merge(title_dim, how = "left") \
                 .merge(channel_dim, how = "left") \
                 .merge(tags_dim, how = "left") \
                 .merge(videoDesc_dim, how = "left")\
                 .merge(settings_dim, how = "left") \
                 .merge(thumbnail_link_dim, how = "left") \
                 [['fact_id','video_id','datetime_id','category_id','title_id','channel_id','tags_id','videoDesc_id',
                  'settings_id','thumbnail_link_id', 'tag_count', 'views', 'likes', 'dislikes', 'comment_count']]

    fact_table = fact_table.rename(columns={"views": "view_count", "likes": "like_count", "dislikes": "dislike_count"})
    


