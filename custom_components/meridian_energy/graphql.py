"""GraphQL documents and privacy-safe response error helpers."""

from __future__ import annotations

import re
from typing import Any

AUTH_GRAPHQL_CODES = frozenset(
    {"KT-CT-1111", "KT-CT-1112", "KT-CT-1120", "KT-CT-1124", "KT-CT-1143"}
)
_ERROR_CODE_PATTERN = re.compile(r"[A-Za-z0-9_.-]{1,64}")


def graphql_error_code(error: dict[str, Any]) -> str:
    """Return only the stable upstream error code, never its message."""
    extensions = error.get("extensions")
    if not isinstance(extensions, dict):
        return "UNKNOWN"
    code = extensions.get("errorCode")
    if not isinstance(code, str) or _ERROR_CODE_PATTERN.fullmatch(code) is None:
        return "UNKNOWN"
    return code


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

MEASUREMENTS_QUERY = """
fragment MeasurementFields on MeasurementConnection {
  pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
  edges {
    node {
      source
      value
      unit
      readAt
      ... on IntervalMeasurementType { startAt endAt }
      metaData {
        utilityFilters {
          ... on ElectricityFiltersOutput {
            readingFrequencyType
            readingDirection
            registerId
            deviceId
            marketSupplyPointId
            readingQuality
          }
        }
        statistics { type costInclTax { estimatedAmount } }
      }
    }
  }
}
query measurements(
  $accountNumber: String!
  $propertyId: ID!
  $before: String
  $last: Int
  $endOn: Date
  $readingFrequencyType: ReadingFrequencyType!
  $readingDirectionType: ReadingDirectionType
  $readingQualityType: ReadingQualityType
) {
  account(accountNumber: $accountNumber) {
    id
    property(id: $propertyId) {
      id
      measurements(
        before: $before
        last: $last
        endOn: $endOn
        timezone: "Pacific/Auckland"
        utilityFilters: [{ electricityFilters: {
          readingDirection: $readingDirectionType
          readingQuality: $readingQualityType
          readingFrequencyType: $readingFrequencyType
        }}]
      ) { ... on MeasurementConnection { ...MeasurementFields } }
    }
  }
}
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
