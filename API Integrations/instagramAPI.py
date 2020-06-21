import requests


def get_business_account_id(facebook_page_id: int, access_token: str) -> int:
    endpoint = "https://graph.facebook.com/v3.1/{}".format(facebook_page_id)
    params = {
        'fields': 'instagram_business_account',
        'access_token': access_token
    }
    response = requests.get(endpoint, params=params).json()
    return response['instagram_business_account']['id']


def get_lifetime_page_community(business_account_id: int, instagram_handle: str, access_token: str) -> dict:
    endpoint = "https://graph.facebook.com/v3.2/{}".format(business_account_id)
    params = {
        'fields': 'business_discovery.username({}){{followers_count}}'.format(instagram_handle),
        'access_token': access_token
    }
    response = requests.get(endpoint, params=params).json()
    return {
        'page_id': response['business_discovery']['id']
        'page_community': response['business_discovery']['followers_count']
    }


def get_lifetime_post_data(access_token: str, business_account_id: int, start_date: str, end_date: str) -> list:
    post_fields = "caption, comments_count, id, ig_id, like_count, media_type, media_url ,permalink, thumbnail_url, timestamp"
    data = list()
    endpoint = "https://graph.facebook.com/v3.1/{}/media".format(
        business_account_id)
    params = {
        'fields': post_fields,
        'access_token': access_token
    }

    response = requests.get(endpoint, params=params).json()

    while endpoint is not None:
        for post in response['data']:
            post_date = response['data'][i]['timestamp']
            post_date = date(int(response[:4]), int(
                response[5:7]), int(response[8:10]))
            if post_date >= start_date and post_date < end_date:
                endpoint = None
                break
            else:
                data.append(post)
        if response.get('paging') and endpoint is not None:
            if response['paging'].get('next'):
                endpoint = response['paging']['next']
                response = requests.get(endpoint).json()
            else:
                break
        else:
            break
    return data


def get_post_list(access_token: str, business_account_id: int, start_date: str, end_date: str) -> list:
    post_fields = "id"
    post_list = list()
    endpoint = "https://graph.facebook.com/v3.1/{}/media".format(
        business_account_id)
    params = {
        'fields': post_fields,
        'access_token': access_token
    }

    response = requests.get(endpoint, params=params).json()

    while endpoint is not None:
        for post in response['data']:
            post_date = response['data'][i]['timestamp']
            post_date = date(int(response[:4]), int(
                response[5:7]), int(response[8:10]))
            if post_date >= start_date and post_date < end_date:
                endpoint = None
                break
            else:
                post_list.append(post['id'])
        if response.get('paging') and endpoint is not None:
            if response['paging'].get('next'):
                endpoint = response['paging']['next']
                response = requests.get(endpoint).json()
            else:
                break
        else:
            break
    return data


def get_daily_page_insights(business_account_id: int, access_token: str, start_date: str, end_date: str) -> dict:
    metrics = "email_contacts,follower_count,get_directions_clicks,impressions,phone_call_clicks,profile_views,reach,text_message_clicks,website_clicks"
    start_date = int((time.mktime(start_date.timetuple())))
    end_date = int((time.mktime(end_date.timetuple())))
    endpoint = "https://graph.facebook.com/v3.1/{}/insights".format(
        business_account_id)
    params = {
        'metric': metrics,
        'period': 'day',
        'since': start_date,
        'until': end_date,
        'access_token': access_token
    }
    return requests.get(endpoint, params=params).json()


def get_lifetime_page_insights(business_account_id: int, access_token: str) -> dict:
    metrics = "audience_city,audience_country,audience_gender_age,audience_locale,online_followers"
    endpoint = "https://graph.facebook.com/v3.1/{}/insights".format(
        business_account_id)
    params = {
        'metric': metrics,
        'period': 'lifetime',
        'access_token': access_token
    }
    return requests.get(endpoint, params=params).json()


def get_lifetime_post_insights(access_token: str, post: tuple) -> dict:
    post_id, post_type = post
    if post_type == "VIDEO":
        metrics = "engagement,impressions,reach,saved,video_views"
    elif post_type == "IMAGE" or post_type == "CAROUSEL_ALBUM":
        metrics = "engagement,impressions,reach,saved"

    endpoint = "https://graph.facebook.com/v3.1/{}/insights".format(post_id)
    params = {
        'metric': metrics,
        'access_token': access_token
    }
    return requests.get(endpoint, params=params).json()
