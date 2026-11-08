"""Constants for the Meridian Energy integration."""

from datetime import timedelta

DOMAIN = "meridian_energy"
NAME = "Meridian Energy"

# The config entry version the setup flow creates and the oldest that loads;
# an entry below it is removed and added again (async_migrate_entry).
CONFIG_ENTRY_VERSION = 4

CONF_REFRESH_TOKEN = "refresh_token"  # noqa: S105 - config-entry field name
CONF_FIREBASE_USER_ID = "firebase_user_id"

AUTH_EMAIL_URL = "https://auth.meridianenergy.nz/cf/email-connector"
AUTH_OTP_URL = "https://auth.meridianenergy.nz/cf/email-otp-authenticator"
FIREBASE_API_KEY = "AIzaSyCYCKXQhGmo7haJxAAyO_7mIPrV7jtxsK8"
FIREBASE_CUSTOM_TOKEN_URL = (
    "https://identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken"  # noqa: S105 - public endpoint
)
FIREBASE_REFRESH_URL = "https://securetoken.googleapis.com/v1/token"
GRAPHQL_URL = "https://api.meridianenergy.nz/v1/graphql/"
REDIRECT_URL = "https://app.meridianenergy.nz?srcPlatformOS=web"

BRAND = "meridian"
CLIENT_PLATFORM = "web"
REQUEST_TIMEOUT_SECONDS = 30
# How much of a request's time limit making its connection may take: a free
# connection, the name lookup, TCP and TLS. aiohttp reports this limit
# running out as a connection timeout. Without it, a connection never made
# used up the request's whole limit and read as Meridian not answering.
CONNECT_TIMEOUT_SECONDS = 20
# Above the gateway's apparent limit of about 60 s, so a slow answer is told
# apart from the gateway's 504, and below Cloudflare's own 100 s limit.
MEASUREMENTS_TIMEOUT_SECONDS = 75

UPDATE_INTERVAL = timedelta(hours=1)
TOPOLOGY_CACHE_INTERVAL = timedelta(days=1)
BILLING_CACHE_INTERVAL = timedelta(days=1)
# Every measurements request asks for one window of whole New Zealand dates,
# both ends included, at most this many dates long.
WINDOW_DAYS = 7
# A window may take this many pages; a longer one is an answer this can't read.
MAX_WINDOW_PAGES = 8
# The recent request set: the last 21 dates when the daily set has not
# succeeded for a day (always so after a start), else the last 3.
DAILY_DATES = 21
RECENT_DATES = 3
DAILY_INTERVAL = timedelta(days=1)
# How far back the fill completes each unit's history, in New Zealand dates.
HISTORY_DAYS = 90
# The most fill windows a unit sends in one sync.
FILL_WINDOWS = 13
# A fill window that failed waits this long before it is asked for again,
# doubling for each failure in a row up to the longest.
FILL_COOLDOWN = timedelta(hours=1)
MAX_FILL_COOLDOWN = timedelta(days=1)
# At least this long between the starts of two measurements requests.
MEASUREMENTS_PACING_SECONDS = 1.0
# The wait before one resend after a passing failure, chosen at random.
TRANSIENT_RESEND_DELAY_SECONDS = (2.0, 5.0)
# Statuses of a gateway or service that failed in a way a resend may not meet.
TRANSIENT_STATUSES = frozenset({408, 502, 503, 504, 520, 521, 522, 523, 524})

FIRST_SYNC_RETRY_SECONDS = 600
# Until an entry's first sync has succeeded, a failed one is tried again after
# these waits in turn, then hourly, as the update interval does.
FIRST_SYNC_RETRY_SCHEDULE = (FIRST_SYNC_RETRY_SECONDS, 1800, 3600)
# Failed updates in a row before the repair notice that updates are failing.
FAILED_UPDATES_BEFORE_ISSUE = 3
DEFAULT_RETRY_AFTER_SECONDS = 3600
MIN_RETRY_AFTER_SECONDS = 60
MAX_RETRY_AFTER_SECONDS = 86400

STAT_CONSUMPTION = "consumption"
STAT_CONSUMPTION_COST = "consumption_cost"
STAT_GENERATION = "generation"
STAT_GENERATION_CREDIT = "generation_credit"

READING_CONSUMPTION = "CONSUMPTION"
READING_GENERATION = "GENERATION"
READING_FREQUENCY_HOUR = "HOUR_INTERVAL"
READING_QUALITY_COMBINED = "COMBINED"
AGGREGATE_CHANNEL_ID = "aggregate"

# The cost types Meridian's own app adds up for an hour's cost, each with its
# sign, so a credit lowers it.
COST_STATISTIC_TYPES = frozenset(
    {
        "CONSUMPTION_COST",
        "STANDING_CHARGE_COST",
        "CAPACITY_CHARGE_COST",
        "DEMAND_CHARGE_COST",
        "DISTANCE_CHARGE_COST",
        "ADDITIONAL_CHARGE_COST",
        "ADDITIONAL_CREDIT_COST",
    }
)
GENERATION_CREDIT_TYPES = frozenset({"GENERATION_VALUE"})
