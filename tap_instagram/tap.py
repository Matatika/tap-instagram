"""Instagram tap class."""

from typing import Dict, List

import requests
from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_instagram.streams import (
    MediaChildrenStream,
    MediaInsightsStream,
    MediaStream,
    MediaCommentsStream,
    DefaultUserInsightsDailyStream,
    UserInsightsLifetimeDefaultStream,
    UserInsightsLifetimeTotalValueStream,
    UserInsightsCustomStream,
    UsersStream,
)

STREAM_TYPES = [
    #MediaChildrenStream,
    MediaInsightsStream,
    MediaStream,
    MediaCommentsStream,
    DefaultUserInsightsDailyStream,
    #UserInsightsLifetimeDefaultStream,
    UserInsightsLifetimeTotalValueStream,
    UsersStream,
]

BASE_URL = "https://graph.facebook.com/{ig_user_id}"

session = requests.Session()


class TapInstagram(Tap):
    """Instagram tap class."""

    name = "tap-instagram"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=True,
            description="A user access token",
        ),
        th.Property(
            "api_version",
            th.StringType,
            description="API version to use for requests (e.g., v17.0)",
        ),
        th.Property(
            "ig_user_ids",
            th.ArrayType(th.StringType),
            required=True,
            description="User IDs of the Instagram accounts to replicate",
        ),
        th.Property(
            "media_insights_lookback_days",
            th.IntegerType,
            default=60,
            description="The tap fetches media insights for Media objects posted in the last `insights_lookback_days` "
            "days - defaults to 14 days if not provided",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "metrics_log_level",
            th.StringType,
            description="A user access token",
        ),
        th.Property(
            "custom_user_insight_reports",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "name",
                        th.StringType,
                        description="Unique name for the custom report (used as stream name)",
                        required=True,
                    ),
                    th.Property(
                        "metrics",
                        th.ArrayType(th.StringType),
                        description="List of metrics to include in the report",
                        required=True,
                    ),
                    th.Property(
                        "metric_type",
                        th.StringType,
                        description="Designates if you want the responses aggregated by time period or as a simple total",
                        required=True,
                    ),
                    th.Property(
                        "time_period",
                        th.StringType,
                        description="Time period for the report (e.g., day, week, month, lifetime, custom)",
                        default="day",
                    ),
                    th.Property(
                        "timeframe",
                        th.StringType,
                        description="Optional timeframe (e.g. last_7_days, last_30_days)",
                    ),
                    th.Property(
                        "start_date",
                        th.StringType,
                        description="Start date for custom period (YYYY-MM-DD)",
                    ),
                    th.Property(
                        "end_date",
                        th.StringType,
                        description="End date for custom period (YYYY-MM-DD)",
                    ),
                    th.Property(
                        "breakdown",
                        th.ArrayType(th.StringType),
                        description="Designates how to break down result set into subsets",
                        default=[],
                    ),
                )
            ),
            description="List of custom Instagram insights report definitions.",
            default=[],
        ),
    ).to_dict()

    @property
    def access_tokens(self) -> Dict[str, str]:
        return {
            int(user_id): self._exchange_token(user_id)
            for user_id in self.config.get("ig_user_ids")
        }

    def _exchange_token(self, user_id: str):
        url = BASE_URL.format(ig_user_id=user_id)
        data = {
            "fields": "access_token,name",
            "access_token": self.config.get("access_token"),
        }
        self.logger.info(f"Exchanging access token for user: {user_id}")
        response = session.get(url=url, params=data)
        response.raise_for_status()
        self.logger.info(f"Successfully exchanged token for user: {user_id}")
        return response.json().get("access_token")

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams: List[Stream] = []

        for stream_class in STREAM_TYPES:
            streams.append(stream_class(tap=self))

        for report_cfg in self.config.get("custom_user_insight_reports", []):
            streams.append(UserInsightsCustomStream(self, report_cfg))

        return streams
