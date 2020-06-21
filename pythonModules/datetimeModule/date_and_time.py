import datetime
import pytz


"""
pytz is recommended by python for timezone handling. 
"""
dt_now = datetime.datetime.now()        # possible to pass time zone
dt_today = datetime.datetime.today()    # current local datetime        tzinfo = None
dt_utcnow = datetime.datetime.utcnow()  # utctime zone                  tzinfo = None

"""All three above are naive as there is no timezone info can be fetched."""

# print(dt_now)
# print(dt_today)
# print(dt_utcnow)

# print(dt_now.date())
# print(dt_now.time())
dt_next_week = dt_now + datetime.timedelta(days=7)

timedelta = dt_next_week - dt_now
# print(timedelta.days)

dt = datetime.datetime(2019, 1, 24, 12, 30, 45, 100000, tzinfo=pytz.UTC)
dt_now = datetime.datetime.now(tz=pytz.UTC)        # possible to pass time zone So, its the best.
dt_utcnow = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)

# print(dt)
# print(dt_now)
# print(dt_utcnow)

# ALL THREE NOW ARE TIMEZONE AWARE

dt_local_with_utc_offset = datetime.datetime.utcnow().astimezone(pytz.timezone('Asia/Calcutta'))
# print(dt_local_with_utc_offset)

# for tz in pytz.all_timezones:
#     print(tz)

# make naive to timezone_aware

tz = pytz.timezone('Asia/Calcutta')
dt_tz_aware = tz.localize(datetime.datetime.now())
# print(dt_tz_aware)

# Use internation format

dt_international_format = dt_now.isoformat()

# format datetime

dt_formatted = dt_now.strftime('%B %d, %Y')
print(dt_formatted)

# parse datetime
datetime_string = 'July 28, 2019'
dt = datetime.datetime.strptime(datetime_string, '%B %d, %Y')
print(dt)