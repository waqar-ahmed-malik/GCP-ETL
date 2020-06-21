import requests


def get_post_list(access_token: str, page_id: int, start_date: str, end_date: str) -> list:
    post_list = list()
    endpoint = "https://graph.facebook.com/v3.1/{}/posts".format(page_id)
    params = {
        'since': start_date,
        'until': end_date,
        'limit': 100,
        'access_token': access_token
    }

    response = requests.get(endpoint, params=params).json()
    post_list.extend([post.get('id') for post in response.get('data')])

    while endpoint is not None:
        if response.get('paging'):
            if response['paging'].get('next'):
                endpoint = response['paging']['next']
                response = requests.get(endpoint).json()
                post_list.extend([post.get('id')
                                  for post in response.get('data')])
            else:
                endpoint = None
        else:
            endpoint = None
    return post_list


def get_lifetime_page_community(access_token: str, page_id: int) -> int:
    endpoint = 'https://graph.facebook.com/v3.1/{}'.format(page_id)
    params = {
        'fields': 'name,fan_count'
        'access_token': access_token
    }
    response = requests.get(endpoint, params=params).json()
    return {
        'page_title': response['name']
        'page_community': response['fan_count']
    }


def get_lifetime_post_data(access_token: str, page_id: int, start_date: str, end_date: str) -> list:
    post_fields = "created_time, full_picture, message, permalink_url, type, comments.limit(0).summary(true), shares,reactions.limit(0).summary(true), name"
    data = list()
    endpoint = "https://graph.facebook.com/v3.1/{}/posts".format(page_id)
    params = {
        'since': start_date,
        'until': end_date,
        'limit': 100,
        'access_token': access_token,
        'fields': post_fields
    }

    response = requests.get(endpoint, params=params).json()
    data.extend(response.get('data'))

    while endpoint is not None:
        if response.get('paging'):
            if response['paging'].get('next'):
                endpoint = response['paging']['next']
                response = requests.get(endpoint).json()
                data.extend(response.get('data'))
            else:
                endpoint = None
        else:
            endpoint = None
    return data


def get_lifetime_post_insights(access_token: str, page_id: int, post_list: list) -> dict:
    post_metrics = "post_video_length,post_activity_unique,post_video_views_unique,post_impressions_nonviral,post_video_views_10s_paid,post_video_views_paid_unique,post_impressions_nonviral_unique,post_negative_feedback_by_type_unique,post_engaged_users,post_impressions_organic,post_reactions_wow_total,post_impressions_fan_paid,post_reactions_love_total,post_video_avg_time_watched,post_video_complete_views_paid,post_video_complete_views_30s_paid,post_video_views_10s_clicked_to_play,post_video_views_by_distribution_type,post_clicks_by_type,post_video_views_10s,post_video_views_organic,post_reactions_by_type_total,post_impressions_by_story_type_unique,post_video_complete_views_paid_unique,post_video_retention_graph_clicked_to_play,post_reactions_haha_total,post_reactions_like_total,post_clicks_by_type_unique,post_reactions_sorry_total,post_activity_by_action_type,post_video_views_10s_organic,post_video_view_time_by_region_id,post_video_complete_views_organic_unique,post_video_view_time_by_distribution_type,post_engaged_fan,post_impressions_paid,post_impressions_unique,post_video_complete_views_organic,post_video_view_time_by_country_id,post_video_complete_views_30s_unique,post_video_view_time_by_age_bucket_and_gender,post_negative_feedback,post_reactions_anger_total,post_video_view_time_organic,post_impressions_viral_unique,post_video_views_organic_unique,post_impressions_fan_paid_unique,post_video_complete_views_30s_autoplayed,post_video_complete_views_30s_clicked_to_play,post_activity,post_impressions_viral,post_video_retention_graph,post_impressions_fan_unique,post_video_views_10s_autoplayed,post_video_views_clicked_to_play,post_activity_by_action_type_unique,post_video_views,post_video_views_10s_unique,post_negative_feedback_unique,post_negative_feedback_by_type,post_impressions_organic_unique,post_clicks,post_video_view_time,post_impressions_paid_unique,post_video_views_10s_sound_on,post_video_complete_views_30s_organic,post_video_retention_graph_autoplayed,post_impressions,post_clicks_unique,post_impressions_fan,post_video_views_paid,post_video_views_sound_on,post_video_views_autoplayed,post_impressions_by_story_type"
    endpoint = "https://graph.facebook.com/v3.1/{}/insights".format(page_id)
    params = {
        'ids': ','.join(post_list),
        'metric': post_metrics,
        'period': 'lifetime',
        'access_token': access_token
    }
    return requests.get(endpoint, params=params).json()


def get_daily_page_insights(access_token: str, page_id: int, start_date: str, end_date: str) -> dict:
    page_metrics = "page_views_logged_in_total,page_actions_post_reactions_like_total,page_actions_post_reactions_love_total,page_actions_post_reactions_anger_total,page_views_by_profile_tab_total,page_fan_removes,page_fan_removes_unique,page_get_directions_clicks_logged_in_unique,page_website_clicks_by_site_logged_in_unique,page_views_by_site_logged_in_unique,page_content_activity,page_fan_adds,page_fan_adds_unique,page_places_checkin_mobile,page_places_checkin_mobile_unique,page_views_by_profile_tab_logged_in_unique,page_fans,page_views_by_internal_referer_logged_in_unique,page_total_actions,page_posts_impressions_viral,page_video_view_time,page_video_views_10s,page_video_complete_views_30s,page_video_complete_views_30s_organic,page_positive_feedback_by_type,page_video_views_10s_repeat,page_positive_feedback_by_type_unique,page_impressions_by_story_type,page_video_complete_views_30s_click_to_play,page_posts_impressions_organic,page_posts_impressions_frequency_distribution,page_video_views_10s_unique,page_video_complete_views_30s_unique,page_posts_impressions_organic_unique,page_fans_country,page_video_views_by_paid_non_paid,page_impressions_frequency_distribution,page_posts_impressions_viral_unique,page_posts_impressions,page_posts_impressions_nonviral,page_video_views_10s_paid,page_posts_impressions_unique,page_impressions_nonviral,page_impressions_paid_unique,page_fans_locale,page_video_complete_views_30s_autoplayed,page_impressions_viral_frequency_distribution,page_impressions_viral_unique,page_consumptions,page_consumptions_by_consumption_type,page_consumptions_unique,page_posts_served_impressions_organic_unique,page_consumptions_by_consumption_type_unique,page_impressions,page_impressions_unique,page_impressions_by_locale_unique,page_views_logout,page_content_activity_by_country_unique,page_tab_views_login_top,page_tab_views_login_top_unique,page_fans_online_per_day,page_fans_by_unlike_source,page_fans_by_like_source_unique,page_views_total,page_cta_clicks_logged_in_total,page_actions_post_reactions_total,page_actions_post_reactions_wow_total,page_actions_post_reactions_haha_total,page_actions_post_reactions_sorry_total,page_places_checkin_total,page_places_checkin_total_unique,page_cta_clicks_logged_in_unique,page_website_clicks_logged_in_unique,page_views_logged_in_unique,page_cta_clicks_by_site_logged_in_unique,page_call_phone_clicks_logged_in_unique,page_content_activity_by_action_type,page_views_by_age_gender_logged_in_unique,page_video_views_organic,page_content_activity_by_action_type_unique,page_video_repeat_views,page_video_views_click_to_play,page_negative_feedback_unique,page_negative_feedback,page_impressions_by_story_type_unique,page_video_views,page_video_views_paid,page_video_views_10s_organic,page_video_views_autoplayed,page_negative_feedback_by_type_unique,page_negative_feedback_by_type,page_video_complete_views_30s_repeat_views,page_video_views_10s_click_to_play,page_fan_adds_by_paid_non_paid_unique,page_post_engagements,page_impressions_viral,page_engaged_users,page_video_views_unique,page_fans_city,page_posts_impressions_paid,page_posts_impressions_paid_unique,page_video_complete_views_30s_paid,page_impressions_paid,page_video_views_10s_autoplayed,page_fans_gender_age,page_posts_impressions_nonviral_unique,page_impressions_nonviral_unique,page_impressions_organic,page_impressions_organic_unique,page_impressions_by_city_unique,page_impressions_by_country_unique,page_impressions_by_age_gender_unique,page_tab_views_logout_top,page_content_activity_by_city_unique,page_views_external_referrals,page_content_activity_by_locale_unique,page_content_activity_by_age_gender_unique,page_fans_online,page_fans_by_like_source,page_fans_by_unlike_source_unique"
    endpoint = "https://graph.facebook.com/v3.1/{}/insights".format(page_id)
    params = {
        'metric': page_metrics,
        'since': start_date,
        'until': end_date,
        'period': 'day',
        'access_token': access_token
    }
    return requests.get(endpoint, params=params).json()
