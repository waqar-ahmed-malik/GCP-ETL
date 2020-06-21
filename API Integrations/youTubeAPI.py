import requests
from googleAPI import get_access_token
from datetime import date
from datetime import timedelta


def get_lifetime_channel_community(client_id: str, client_secret: str, refresh_token: str, channel_id: str) -> int:
    access_token = get_access_token(client_id, client_secret, refresh_token)
    headers = {
        'Authorization': 'Bearer {}'.format(access_token)
    }
    endpoint = 'https://www.googleapis.com/youtube/v3/channels'
    params = {
        'id': channel_id,
        'part': 'statistics'
    }
    response = requests.get(endpoint, headers=headers, params=params).json()
    return response['items'][0]['statistics']['subscriberCount']


def get_lifetime_video_data(client_id: str, client_secret: str, refresh_token: str, channel_id: str, start_date: str, end_date: str) -> list:
    data = list()
    access_token = get_access_token(client_id, client_secret, refresh_token)
    endpoint = 'https://www.googleapis.com/youtube/v3/search/'
    params = {
        'channelId': channel_id,
        'maxResults': 50,
        'type': 'video',
        'part': 'id',
        'order': 'date',
        'publishedAfter': '{}T00:00:00Z'.format(start_date),
        'publishedBefore': '{}T00:00:00Z'.format(end_date)
    }
    headers = {
        'Authorization': 'Bearer {}'.format(access_token)
    }

    response = requests.get(endpoint, headers=headers, params=params).json()
    while endpoint is not None:
        if response.get('items'):
            data.extend(response.get('items'))
        else:
            endpoint = None
        if response.get('nextPageToken'):
            endpoint = response['nextPageToken']
            response = requests.get(endpoint).json()
        else:
            endpoint = None
    return data


def get_videos_list(client_id: str, client_secret: str, refresh_token: str, channel_id: str, start_date: str, end_date: str) -> list:
    data = list()
    access_token = get_access_token(client_id, client_secret, refresh_token)
    endpoint = 'https://www.googleapis.com/youtube/v3/search/'
    params = {
        'channelId': channel_id,
        'maxResults': 50,
        'type': 'video',
        'part': 'id',
        'order': 'date',
        'publishedAfter': '{}T00:00:00Z'.format(start_date),
        'publishedBefore': '{}T00:00:00Z'.format(end_date)
    }
    headers = {
        'Authorization': 'Bearer {}'.format(access_token)
    }

    response = requests.get(endpoint, headers=headers, params=params).json()

    while endpoint is not None:
        if response.get('items'):
            data.extend([video['id']['videoId']
                         for video in response['items']])
        else:
            endpoint = None
        if response.get('nextPageToken'):
            endpoint = response['nextPageToken']
            response = requests.get(endpoint).json()
        else:
            endpoint = None
    return data


def get_available_channel_metrics(access_token: str, channel_id: str) -> str:
    headers = {
        'Authorization': 'Bearer {}'.format(access_token)
    }
    endpoint = "https://youtubeanalytics.googleapis.com/v2/reports"
    params = {
        'startDate': date.today() - timedelta(days=1),
        'endDate': date.today(),
        'ids': 'channel=={}'.format(channel_id),
        'metrics': 'cpm'
    }
    if requests.get(endpoint, headers=headers, params=params).status_code == 200:
        return "views,redViews,comments,likes,dislikes,videosAddedToPlaylists,videosRemovedFromPlaylists,shares,estimatedMinutesWatched,estimatedRedMinutesWatched,averageViewDuration,averageViewPercentage,annotationClickThroughRate,annotationCloseRate,annotationImpressions,annotationClickableImpressions,annotationClosableImpressions,annotationClicks,cardClickRate,cardTeaserClickRate,cardImpressions,cardTeaserImpressions,cardClicks,subscribersGained,subscribersLost,estimatedRevenue,estimatedAdRevenue,grossRevenue,estimatedRedPartnerRevenue,monetizedPlaybacks,playbackBasedCpm,adImpressions,cpm"
    else:
        return "views,redViews,comments,likes,dislikes,videosAddedToPlaylists,videosRemovedFromPlaylists,shares,estimatedMinutesWatched,estimatedRedMinutesWatched,averageViewDuration,averageViewPercentage,annotationClickThroughRate,annotationCloseRate,annotationImpressions,annotationClickableImpressions,annotationClosableImpressions,annotationClicks,cardClickRate,cardTeaserClickRate,cardImpressions,cardTeaserImpressions,cardClicks,subscribersGained,subscribersLost"


def get_daily_channel_insights(client_id: str, client_secret: str, refresh_token: str, channel_id: str, start_date: str, end_date: str) -> list:
    data = list()
    access_token = get_access_token(client_id, client_secret, refresh_token)
    metrics = get_available_channel_metrics(access_token, channel_id)

    headers = {
        'Authorization': 'Bearer {}'.format(access_token)
    }
    endpoint = "https://youtubeanalytics.googleapis.com/v2/reports"
    params = {
        'startDate': start_date,
        'endDate': end_date,
        'ids': 'channel=={}'.format(channel_id),
        'metrics': metrics
    }

    response = requests.get(endpoint, headers=headers, params=params).json()
    return response['rows']


def get_daily_channel_demographics(client_id: str, client_secret: str, refresh_token: str, channel_id: str, data_date: str) -> list:
    data = list()
    access_token = get_access_token(client_id, client_secret, refresh_token)

    headers = {
        'Authorization': 'Bearer {}'.format(access_token)
    }
    params = {
        'dimensions': 'ageGroup,gender',
        'startDate': data_date,
        'endDate': data_date,
        'ids': 'channel=={}'.format(channel_id),
        'metrics': 'viewerPercentage'
    }

    response = requests.get(endpoint, headers=headers, params=params).json()
    return response['rows']


def get_daily_channel_country_insights(client_id: str, client_secret: str, refresh_token: str, channel_id: str, data_date: str) -> list:
    data = list()
    access_token = get_access_token(client_id, client_secret, refresh_token)
    metrics = get_available_channel_metrics(access_token, channel_id)

    headers = {
        'Authorization': 'Bearer {}'.format(access_token)
    }
    endpoint = "https://youtubeanalytics.googleapis.com/v2/reports"
    params = {
        'dimensions': 'country',
        'startDate': data_date,
        'endDate': data_date,
        'ids': 'channel=={}'.format(channel_id),
        'metrics': metrics
    }

    response = requests.get(endpoint, headers=headers, params=params).json()
    return response['rows']


def get_daily_video_insights(client_id: str, client_secret: str, refresh_token: str, channel_id: str, start_date: str, end_date: str, video_id: str) -> list:
    data = list()
    access_token = get_access_token(client_id, client_secret, refresh_token)
    metrics = get_available_channel_metrics(access_token, channel_id)

    headers = {
        'Authorization': 'Bearer {}'.format(access_token)
    }
    endpoint = "https://youtubeanalytics.googleapis.com/v2/reports"
    params = {
        'dimensions': 'day,video',
        'startDate': start_date,
        'endDate': end_date,
        'ids': 'channel=={}'.format(channel_id),
        'metrics': metrics,
        'filters': 'video=={}'.format(video_id)
    }

    response = requests.get(endpoint, headers=headers, params=params).json()
    return response['rows']


def get_daily_video_trafficinsights(client_id: str, client_secret: str, refresh_token: str, channel_id: str, data_date: str, video_id: str) -> list:
    data = list()
    access_token = get_access_token(client_id, client_secret, refresh_token)
    metrics = get_available_channel_metrics(access_token, channel_id)

    headers = {
        'Authorization': 'Bearer {}'.format(access_token)
    }
    endpoint = "https://youtubeanalytics.googleapis.com/v2/reports"
    params = {
        'dimensions': 'video,insightTrafficSourceType',
        'startDate': data_date,
        'endDate': data_date,
        'ids': 'channel=={}'.format(channel_id),
        'metrics': 'views,estimatedMinutesWatched',
        'filters': 'video=={}'.format(video_id)
    }

    response = requests.get(endpoint, headers=headers, params=params).json()
    return response['rows']
