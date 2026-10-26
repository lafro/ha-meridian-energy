"""Constants for the Meridian Energy integration."""

from datetime import timedelta

DOMAIN = "meridian_energy"
NAME = "Meridian Energy"

CONF_REFRESH_TOKEN = "refresh_token"  # noqa: S105 - config-entry field name
CONF_FIREBASE_USER_ID = "firebase_user_id"
CONF_SELECTED_ACCOUNTS = "selected_accounts"
CONF_AUTO_ADD_ACCOUNTS = "auto_add_accounts"
CONF_STATISTICS_STATE_VERSION = "statistics_state_version"
STATISTICS_STATE_VERSION = 1

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
INITIAL_BACKFILL = timedelta(days=90)
REVISION_OVERLAP = timedelta(days=14)
TIP_WINDOW = timedelta(hours=24)
TARGETED_RECONCILIATION_MINIMUM = timedelta(hours=48)
TARGETED_RECONCILIATION_INTERVAL = timedelta(days=1)
FULL_RECONCILIATION_INTERVAL = timedelta(days=7)
TOPOLOGY_CACHE_INTERVAL = timedelta(days=1)
BILLING_CACHE_INTERVAL = timedelta(days=1)
RECONCILIATION_SAFETY_MARGIN = timedelta(hours=6)
# How far before the newest stored usage hour a walk the frontier rule moves
# back may reach for the hours whose cost Meridian published later: a cost
# statistic that stopped for good must not make every such walk 90 days long.
COST_LAG_MARGIN = timedelta(hours=48)
PAGE_SIZE = 744
MIN_PAGE_SIZE = 24
# Every measurements request asks for one window of whole New Zealand dates,
# both ends inclusive, at most this many days long, newest first.
WINDOW_DAYS = 7
# Days each window repeats of the newer one next to it. Held in reserve at 0:
# set to 1 only if a live check shows Meridian's window edges are shifted, and
# a repeated hour then counts once when its readings are identical.
WINDOW_OVERLAP_DAYS = 0
# A window may take this many pages of PAGE_SIZE rows, or as many smaller pages
# as hold the same rows (windows.page_limit).
MAX_WINDOW_PAGES = 16
# Consecutive windows with no readings that mark the start of the history.
EMPTY_WINDOWS_AT_HISTORY_START = 2
# At least this long between the starts of two measurements requests in a
# sync, so no sync sends more than 60 a minute.
MEASUREMENTS_PACING_SECONDS = 1.0
# A new request starts only while this much of a sync's time budget remains,
# enough for a measurements request to time out (the first always starts).
REQUEST_START_RESERVE_SECONDS = 80
# After a gateway timeout no measurements request goes to the same property
# for at least this long (or for as long as the failed request took): the
# service behind the gateway may still be working on it.
GATEWAY_COOLDOWN_SECONDS = 75
# The wait before one resend after another passing failure, chosen at random.
TRANSIENT_RESEND_DELAY_SECONDS = (2.0, 5.0)
# Statuses of a service that answered with an error a resend may not meet.
TRANSIENT_STATUSES = frozenset({408, 502, 503, 520, 521, 522, 523})

FIRST_SYNC_RETRY_SECONDS = 600
# Until an entry's first sync has succeeded, a failed one is tried again after
# these waits in turn, then hourly, as the update interval does.
FIRST_SYNC_RETRY_SCHEDULE = (FIRST_SYNC_RETRY_SECONDS, 1800, 3600)
# An hourly update's time budget: no measurements request after its first
# starts once less than REQUEST_START_RESERVE_SECONDS of it remain.
POLL_BUDGET_SECONDS = 240
# How many 30 s waits an entry's first sync makes for the recorder to save
# the work queued before it, such as the setup import, before it reads any
# statistics. When the recorder is still behind, the sync tries again after
# FIRST_SYNC_RETRY_SECONDS.
FIRST_SYNC_RECORDER_WAITS = 10
SETUP_IMPORT_RETRY_DELAY_SECONDS = 120
# A backstop for one setup import attempt, including the wait for the
# recorder to save it; real failures end long before.
SETUP_IMPORT_TIMEOUT_SECONDS = 300
# The setup import's own budget, as an hourly update's: no request after its
# first starts once less than REQUEST_START_RESERVE_SECONDS of it remain, so
# the last one can end before the backstop, and what came back is imported.
# Setup then creates the entry, whose updates import the rest.
SETUP_IMPORT_BUDGET_SECONDS = POLL_BUDGET_SECONDS
# An hourly update that walks back further than 14 days for a direction, as
# a first import or a catch-up after a long outage, has this budget instead:
# it imports what it fetched all the same, but a search for the newest
# reading of a supply that ended must finish to show it ended, or it would
# start again at every update. Its newest windows still go first.
DEEP_WALK_BUDGET_SECONDS = 900
SETUP_IMPORT_MAX_ATTEMPTS = 5
# Where a finished setup leaves its new entry the accounts it found and when
# its import ran, in hass.data[DOMAIN], keyed by the entry's unique ID.
SETUP_HANDOFF = "setup_handoff"
# Where each entry keeps, across its reloads, the oldest hour of each unit's
# deep replay that was not confirmed, in hass.data[DOMAIN], keyed by the
# entry's ID. A restart of Home Assistant forgets it.
UNCONFIRMED_REPLAYS = "unconfirmed_replays"
# Where each entry keeps, across its reloads, the dates of each unit's lane
# whose coverage import was not confirmed, with when each is tried again, in
# hass.data[DOMAIN], keyed by the entry's ID. A restart forgets them too.
UNSAVED_COVERAGE = "unsaved_coverage"
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

COST_STATISTIC_TYPES = frozenset({"CONSUMPTION_COST", "STANDING_CHARGE_COST"})
GENERATION_CREDIT_TYPES = frozenset({"GENERATION_VALUE"})
