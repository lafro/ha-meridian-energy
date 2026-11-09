"""GraphQL documents and privacy-safe response error helpers."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

# The codes that say the session has ended. KT-CT-1112, a missing
# Authorization header, is not one: it says nothing of the session, so it is
# a refused request.
AUTH_GRAPHQL_CODES = frozenset({"KT-CT-1111", "KT-CT-1120", "KT-CT-1124", "KT-CT-1143"})
_ERROR_CODE_PATTERN = re.compile(r"[A-Za-z0-9_.-]{1,64}")
# The form of Kraken's own codes: only these are ever shown.
_KRAKEN_CODE_PATTERN = re.compile(r"KT-[A-Z]{2}-\d{4}")
# What an error without a stable code is named, a failure inside the service say.
UNKNOWN_ERROR_CODE = "UNKNOWN"


def graphql_error_code(error: dict[str, Any]) -> str:
    """Return only the stable upstream error code, never its message."""
    extensions = error.get("extensions")
    if not isinstance(extensions, dict):
        return UNKNOWN_ERROR_CODE
    code = extensions.get("errorCode")
    if not isinstance(code, str) or _ERROR_CODE_PATTERN.fullmatch(code) is None:
        return UNKNOWN_ERROR_CODE
    return code


def shown_codes(codes: Iterable[str]) -> list[str]:
    """
    Return codes as logs, messages and diagnostics show them, once each.

    Only a code in Kraken's own form is shown; any other, which could carry
    an identifier, reads "an error code". Codes without one are left out.
    """
    return list(
        dict.fromkeys(
            code if _KRAKEN_CODE_PATTERN.fullmatch(code) else "an error code"
            for code in codes
            if code != UNKNOWN_ERROR_CODE
        )
    )


def is_service_failure(codes: Iterable[str]) -> bool:
    """
    Return whether GraphQL errors are all without a code.

    Such an error, as for a failure inside Meridian's service, says nothing
    about the request: it counts as Meridian not answering, not as a
    refusal. An error without a code is recorded as UNKNOWN_ERROR_CODE, so
    codes is never empty.
    """
    return set(codes) <= {UNKNOWN_ERROR_CODE}


ACCOUNTS_QUERY = """
query accountsList($allowedBrandCodes: [BrandChoices]) {
  viewer {
    accounts(allowedBrandCodes: $allowedBrandCodes) {
      number
      status
      ... on AccountType { id }
    }
  }
}
"""

ACCOUNT_QUERY = """
query account($accountNumber: String!, $activeFrom: DateTime) {
  account(accountNumber: $accountNumber) {
    number
    status
    properties(activeFrom: $activeFrom) {
      id
      address
      meterPoints {
        id
        marketIdentifier
        registers { identifier activeFrom activeTo isFeedIn }
      }
    }
  }
}
"""

# Kraken's in-band limits, which arrive in an HTTP 200 body: a rate limit, and
# a query whose complexity or node count is over the limit.
RATE_LIMIT_CODE = "KT-CT-1199"
QUERY_LIMIT_CODES = frozenset({"KT-CT-1188", "KT-CT-1189"})


def web_app_url(graphql_url: str, name: str) -> str:
    """Return the URL the web app sends an operation to: ``?opName=<name>``."""
    return f"{graphql_url}?opName={name}"


def measurements_operation(graphql_url: str) -> tuple[str, str]:
    """Return the measurements query's web-app name and the URL it is sent to."""
    name = "GetMeasurements"
    return name, web_app_url(graphql_url, name)


def measurements_query(operation_name: str) -> str:
    """
    Return the measurements query document, named as it is sent.

    Both bounds and the page size are non-null variables, so a request that
    left one out would fail validation instead of asking for the whole
    history. A continuation page adds ``before``.
    """
    return f"""
fragment MeasurementFields on MeasurementConnection {{
  pageInfo {{ hasNextPage hasPreviousPage startCursor endCursor }}
  edges {{
    node {{
      source
      value
      unit
      readAt
      ... on IntervalMeasurementType {{ startAt endAt }}
      metaData {{
        utilityFilters {{
          ... on ElectricityFiltersOutput {{
            readingFrequencyType
            readingDirection
            registerId
            deviceId
            marketSupplyPointId
            readingQuality
          }}
        }}
        statistics {{ type costInclTax {{ estimatedAmount }} }}
      }}
    }}
  }}
}}
query {operation_name}(
  $accountNumber: String!
  $propertyId: ID!
  $before: String
  $last: Int!
  $startOn: Date!
  $endOn: Date!
  $readingFrequencyType: ReadingFrequencyType!
  $readingDirectionType: ReadingDirectionType
  $readingQualityType: ReadingQualityType
) {{
  account(accountNumber: $accountNumber) {{
    id
    property(id: $propertyId) {{
      id
      measurements(
        before: $before
        last: $last
        startOn: $startOn
        endOn: $endOn
        timezone: "Pacific/Auckland"
        utilityFilters: [{{ electricityFilters: {{
          readingDirection: $readingDirectionType
          readingQuality: $readingQualityType
          readingFrequencyType: $readingFrequencyType
        }}}}]
      ) {{ ... on MeasurementConnection {{ ...MeasurementFields }} }}
    }}
  }}
}}
"""


BILLING_PERIODS_QUERY = """
query billingPeriods($accountNumber: String!) {
  account(accountNumber: $accountNumber) {
    billingOptions {
      periodLength
      periodLengthMultiplier
      isFixed
      currentBillingPeriodStartDate
      currentBillingPeriodEndDate
      nextBillingDate
      periodStartDay
    }
  }
}
"""


# The web app's balance query, trimmed to what the sensors show: no bill
# link, address or identifiers.
BALANCE_NAME = "GetAccountBalance"
BALANCE_QUERY = """
query GetAccountBalance($accountNumber: String!) {
  account(accountNumber: $accountNumber) {
    payments(first: 20) { edges { node { amount paymentDate status } } }
    bills(first: 1) { edges { node { fromDate toDate issuedDate } } }
    balance(includeAllLedgers: true)
  }
}
"""

# The web app's plan query, trimmed to the current agreement's rates.
PRICES_NAME = "GetAccountPropertyAgreements"
PRICES_QUERY = """
query GetAccountPropertyAgreements($accountNumber: String!, $propertyId: ID!) {
  account(accountNumber: $accountNumber) {
    property(id: $propertyId) {
      meterPoints {
        activeAgreement {
          displayName
          rates {
            hasDiscount
            formattedRateIncludingTax
            formattedRateIncludingTaxIncludingDiscount
            bandCategory
            touBucketName
          }
        }
      }
    }
  }
}
"""
