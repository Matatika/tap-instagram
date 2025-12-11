"""Stream type classes for tap-instagram."""

from datetime import datetime, timedelta
import json
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pendulum
import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath
import urllib

from tap_instagram.client import InstagramStream

# SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class UsersStream(InstagramStream):
    """Define custom stream."""

    name = "users"
    path = "/{user_id}"
    replication_key = None
    fields = [
        "id",
        "ig_id",
        "name",
        "username",
        "biography",
        "followers_count",
        "follows_count",
        "media_count",
        "is_published",
        "profile_picture_url",
        "has_profile_pic",
        "website",
    ]
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("ig_id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("biography", th.StringType),
        th.Property("username", th.StringType),
        th.Property("followers_count", th.IntegerType),
        th.Property("media_count", th.IntegerType),
        th.Property("follows_count", th.IntegerType),
        th.Property("is_published", th.BooleanType),
        th.Property("has_profile_pic", th.BooleanType),
        th.Property("profile_picture_url", th.StringType),
        th.Property("website", th.StringType),
    ).to_dict()

    @property
    def partitions(self) -> Optional[List[dict]]:
        return [{"user_id": user_id} for user_id in self.config["ig_user_ids"]]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["fields"] = ",".join(self.fields)
        return params

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {"user_id": record["id"]}

class BaseMediaStream(InstagramStream):
    """Shared logic for Media & Stories."""

    records_jsonpath = "$.data[*]"
    inject_context_fields: list[str] = []

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        if hasattr(self, "fields"):
            params["fields"] = ",".join(self.fields)
        if getattr(self, "replication_key", None):
            params["since"] = self.get_starting_timestamp(context)
        return params

    def get_records(self, context):
        for row in super().get_records(context):
            for f in self.inject_context_fields:
                row[f] = context.get(f)
            yield row

    def parse_response(self, response):
        for row in extract_jsonpath(self.records_jsonpath, response.json()):
            ts = row.get("timestamp")
            if ts:
                row["timestamp"] = pendulum.parse(ts).format("YYYY-MM-DD HH:mm:ss")

            row["is_story"] = (row.get("media_product_type") == "STORY")

            yield row

class MediaStream(BaseMediaStream):
    """Media history stream."""

    name = "media"
    path = "/{user_id}/media"  # user_id is populated using child context keys from UsersStream
    parent_stream_type = UsersStream
    primary_keys = ["id","timestamp"]
    replication_key = "timestamp"
    records_jsonpath = "$.data[*]"
    inject_context_fields: list[str] = []
    fields = [
        "id",
        "ig_id",
        "caption",
        "comments_count",
        "is_comment_enabled",
        "is_story",
        "like_count",
        "media_product_type",
        "media_type",
        "media_url",
        "owner",
        "permalink",
        "shortcode",
        "thumbnail_url",
        "timestamp",
        "username",
    ]
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
            description="Media ID.",
        ),
        th.Property(
            "ig_id",
            th.StringType,
            description="Instagram media ID.",
        ),
        th.Property(
            "caption",
            th.StringType,
            description="Caption. Excludes album children. @ symbol excluded unless the app user can perform "
            "admin-equivalent tasks on the Facebook Page connected to the Instagram account used to "
            "create the caption.",
        ),
        th.Property(
            "carousel_album_id",
            th.StringType,
            description="ID of the parent carousel album if the media is part of a carousel album.",
        ),
        th.Property(
            "comments_count",
            th.IntegerType,
            description="Count of comments on the media. Excludes comments on album child media and the media's "
            "caption. Includes replies on comments.",
        ),
        th.Property(
            "is_comment_enabled",
            th.BooleanType,
            description="Indicates if comments are enabled or disabled. Excludes album children.",
        ),
        th.Property(
            "is_story",
            th.BooleanType,
            description="Indicates whether the media is a story.",
        ),
        th.Property(
            "like_count",
            th.IntegerType,
            description="Count of likes on the media. Excludes likes on album child media and likes on promoted posts "
            "created from the media. Includes replies on comments.",
        ),
        th.Property(
            "media_product_type",
            th.StringType,
            description="Surface where the media is published. Can be AD, FEED, IGTV, or STORY.",
        ),
        th.Property(
            "media_type",
            th.StringType,
            description="Media type. Can be CAROUSEL_ALBUM, IMAGE, or VIDEO.",
        ),
        th.Property(
            "media_url",
            th.StringType,
            description="Media URL. Will be omitted from responses if the media contains copyrighted material, "
            "or has been flagged for a copyright violation.",
        ),
        th.Property(
            "owner",
            th.ObjectType(
                th.Property(
                    "id",
                    th.StringType,
                    description="ID of Instagram user who created the media.",
                ),
                th.Property(
                    "username",
                    th.StringType,
                    description="Username of Instagram user who created the media.",
                ),
            ),
            description="ID of Instagram user who created the media. Only returned if the app user making the query "
            "also created the media, otherwise username field will be returned instead.",
        ),
        th.Property(
            "permalink",
            th.StringType,
            description="Permanent URL to the media.",
        ),
        th.Property(
            "shortcode",
            th.StringType,
            description="Shortcode to the media.",
        ),
        th.Property(
            "thumbnail_url",
            th.StringType,
            description="Media thumbnail URL. Only available on VIDEO media.",
        ),
        th.Property(
            "timestamp",
            th.DateTimeType,
            description="ISO 8601 formatted creation date in UTC (default is UTC ±00:00)",
        ),
        th.Property(
            "username",
            th.StringType,
            description="Username of user who created the media.",
        ),
    ).to_dict()

    def get_child_context(self, record, context):
        ctx = {
            "user_id": context["user_id"],
            "media_id": record["id"],
            "media_type": record.get("media_type"),
            "media_product_type": record.get("media_product_type"),
        }
        if record.get("media_type") == "CAROUSEL_ALBUM":
            ctx["carousel_album_id"] = record["id"]
        return ctx


class MediaChildrenStream(MediaStream):
    """Define custom stream."""

    name = "media_children"
    parent_stream_type = MediaStream
    state_partitioning_keys = ["user_id"]
    path = "/{media_id}/children"  # media_id is populated using child context keys from MediaStream
    inject_context_fields = ["media_id", "carousel_album_id"]
    fields = [
        "id",
        "ig_id",
        "carousel_album_id",
        "media_type",
        "media_url",
        "owner",
        "permalink",
        "shortcode",
        "thumbnail_url",
        "timestamp",
        "username",
    ]


class MediaCommentsStream(MediaStream):
    """Define custom stream."""

    name = "media_comments"
    parent_stream_type = MediaStream
    state_partitioning_keys = ["user_id"]
    primary_keys = ["id","timestamp"]
    path = "/{media_id}/comments" 
    inject_context_fields = ["media_id"]
    fields = [
        "id",
        "text",
        "timestamp",
        "username",
        "like_count",
        "replies",
        "hidden",
        "owner",
        "parent_id",
    ]
    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
            description="Media comment ID.",
        ),
        th.Property(
            "user_id",
            th.StringType,
            description="Instagram user ID.",
        ),
        th.Property(
            "media_id",
            th.StringType,
            description="Instagram media ID.",
        ),
        th.Property(
            "parent_id",
            th.StringType,
            description="ID of the parent IG Comment if this comment was created on another IG Comment",
        ),
        th.Property(
            "replies",
            th.ObjectType(),
        ),
        th.Property(
            "like_count",
            th.IntegerType,
            description="Count of likes on the media. Excludes likes on album child media and likes on promoted posts "
            "created from the media. Includes replies on comments.",
        ),
        th.Property(
            "text",
            th.StringType,
        ),
        th.Property(
            "timestamp",
            th.DateTimeType,
            description="ISO 8601 formatted creation date in UTC (default is UTC ±00:00)",
        ),
        th.Property(
            "hidden",
            th.BooleanType,
        ),
        th.Property(
            "owner",
            th.ObjectType(
                th.Property(
                    "id",
                    th.StringType,
                    description="ID of Instagram user who created the media.",
                ),
                th.Property(
                    "username",
                    th.StringType,
                    description="Username of Instagram user who created the media.",
                ),
            ),
            description="ID of Instagram user who created the media. Only returned if the app user making the query "
            "also created the media, otherwise username field will be returned instead.",
        ),
    ).to_dict()

class BaseMediaInsightsStream(InstagramStream):
    """Shared logic for MediaInsights and StoryInsights."""
    
    parent_stream_type = MediaStream
    records_jsonpath = "$.data[*]"
    replication_key = None
    breakdown_metric = None
    breakdowns = None

    @property
    def path(self):
        api_version = self.config.get("api_version")
        if api_version:
            return f"/{api_version}" + "/{media_id}/insights"
        return "/{media_id}/insights"

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)

        # Breakdown request
        if context.get("breakdown_run") and self.breakdown_metric:
            params["metric"] = self.breakdown_metric
            params["breakdown"] = self.breakdowns
        else:
            params["metric"] = ",".join(
                self._metrics_for_media_type(
                    context["media_type"], context["media_product_type"]
                )
            )
        params["period"] = "lifetime"
        return params

    def get_records(self, context):
        grouped_rows: dict[str, dict] = {}

        def _merge_record(record: dict, is_breakdown: bool = False):
            key = record.get("id")
            if not key:
                return

            base = grouped_rows.setdefault(key, {"id": key})
            for k, v in record.items():
                if v is None:
                    continue

                # Story navigation appears in both standard and breakdown responses; keep the first.
                if is_breakdown and k == "story_navigation" and k in base:
                    continue

                base[k] = v

        # standard request
        self._current_context = context or {}
        for r in super().get_records(context):
            r["metric_type"] = "standard"
            _merge_record(r)

        # breakdown request
        if self.breakdown_metric:
            bd = dict(context or {})
            bd["breakdown_run"] = True
            self._current_context = bd
            for r in super().get_records(bd):
                r["metric_type"] = "breakdown"
                _merge_record(r, is_breakdown=True)

        for row in grouped_rows.values():
            yield row

    def validate_response(self, response):
        err = response.json().get("error", {})
        msg = err.get("message") or ""
        if (
            err.get("error_user_title") == "Media posted before business account conversion"
            or "(#10) Not enough viewers" in msg
        ):
            self.logger.warning(f"Skipping insights: {err}")
            return
        super().validate_response(response)

    def parse_response(self, response):
        resp = response.json()
        data = resp.get("data") or ([resp] if "name" in resp else [])
        if not data:
            return

        record = {}
        for metric in data:
            mid = metric.get("id", "").split("/")[0]
            if mid:
                record["id"] = mid

            name = metric.get("name", "").lower()
            ctx = getattr(self, "_current_context", {}) or {}

            # prefix resolution using media type/product type instead of description
            media_product_type = ctx.get("media_product_type")
            media_type = ctx.get("media_type")
            if media_product_type == "STORY":
                prefix = "story_"
            elif media_product_type == "REELS":
                prefix = "reel_"
            elif media_type == "CAROUSEL_ALBUM":
                prefix = "carousel_album_"
            else:
                prefix = "video_photo_"

            key = f"{prefix}{name}"

            # normal values
            values = metric.get("values", [])
            val = values[0].get("value") if values else None
            if isinstance(val, dict):
                for k,v in val.items():
                    record[f"{key}_{k}"] = v
            elif val is not None:
                record[key] = val
                if name in ("likes", "comments"):
                    record[name] = val

            # breakdowns
            total = metric.get("total_value", {})
            for bd in total.get("breakdowns", []):
                for res in bd.get("results", []):
                    action = res["dimension_values"][0]
                    record[f"{prefix}{action}"] = res.get("value")

        if record:
            yield record

class MediaInsightsStream(BaseMediaInsightsStream):
    """Media Insights stream."""

    name = "media_insights"
    parent_stream_type = MediaStream
    state_partitioning_keys = ["user_id"]
    replication_key = None
    records_jsonpath = "$.data[*]"

    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
            description="Unique media ID from Instagram.",
        ),
        # Reel metrics
        th.Property(
            "reel_comments",
            th.IntegerType,
            description="Number of comments on the reel.",
        ),
        th.Property(
            "reel_likes",
            th.IntegerType,
            description="Number of likes on the reel.",
        ),
        th.Property(
            "reel_plays",
            th.IntegerType,
            description="Number of plays for the reel.",
        ),
        th.Property(
            "reel_reach",
            th.IntegerType,
            description="Number of unique accounts that viewed the reel.",
        ),
        th.Property(
            "reel_saved",
            th.IntegerType,
            description="Number of times the reel was saved.",
        ),
        th.Property(
            "reel_shares",
            th.IntegerType,
            description="Number of times the reel was shared.",
        ),
        th.Property(
            "reel_views",
            th.IntegerType,
            description="Number of views for the reel.",
        ),
        th.Property(
            "reel_total_interactions",
            th.IntegerType,
            description="Total interactions on the reel.",
        ),
        th.Property(
            "likes",
            th.IntegerType,
            description="Number of likes on the media (no prefix).",
        ),
        th.Property(
            "comments",
            th.IntegerType,
            description="Number of comments on the media (no prefix).",
        ),
        # Feed (video/photo) metrics
        th.Property(
            "video_photo_impressions",
            th.IntegerType,
            description="Number of times the video or photo was seen.",
        ),
        th.Property(
            "video_photo_reach",
            th.IntegerType,
            description="Unique accounts that viewed the video or photo.",
        ),
        th.Property(
            "video_photo_reach",
            th.IntegerType,
            description="Unique accounts that viewed the video or photo.",
        ),
        th.Property(
            "video_photo_saved",
            th.IntegerType,
            description="Number of times the video or photo was saved.",
        ),
        th.Property(
            "video_photo_shares",
            th.IntegerType,
            description="Number of times the video or photo was shared.",
        ),
        th.Property(
            "video_photo_views",
            th.IntegerType, 
            description="Number of views for the video or photo.",
        ),
        th.Property(
            "carousel_album_total_interactions",
            th.IntegerType,
            description="Total interactions on the carousel post.",
        ),
        th.Property(
            "carousel_album_reach",
            th.IntegerType,
            description="Unique accounts that viewed the carousel post.",
        ),
        th.Property(
            "carousel_album_saved",
            th.IntegerType,
            description="Number of times the carousel post was saved.",
        ),
        th.Property(
            "carousel_album_shares",
            th.IntegerType,
            description="Number of times the carousel post was shared.",
        ),
        th.Property(
            "carousel_album_views",
            th.IntegerType,
            description="Number of views for the carousel post.",
        ),
        th.Property(
            "likes",
            th.IntegerType,
            description="Number of likes post or ad received.",
        ),
        th.Property(
            "comments",
            th.IntegerType,
            description="Number of comments post or ad received.",
        ),
    ).to_dict()

    def _metrics_for_media_type(self,media_type: str, media_product_type: str):
        # TODO: Define types for these function args
        if media_type in ("IMAGE", "VIDEO"):
            if media_product_type == "REELS":
                return [
                    "comments",
                    "likes",
                    "reach",
                    "saved",
                    "shares",
                    "views",
                    "total_interactions",
                ]
            else:  # media_product_type is "AD" or "FEED"
                metrics = [
                    "total_interactions",
                    "likes",
                    "comments",
                    "reach",
                    "saved",
                    "views",
                    "shares",
                ]
                return metrics
        elif media_type == "CAROUSEL_ALBUM":
            return [
                "likes",
                "comments",
                "reach",
                "saved",
                "shares",
                "total_interactions",
                "views",
            ]
        else:
            raise ValueError(
                f"media_type from parent record must be one of IMAGE, VIDEO, CAROUSEL_ALBUM, got: {media_type}"
            )

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        metrics = self._metrics_for_media_type(
            context["media_type"], context["media_product_type"]
        )
        params["metric"] = ",".join(metrics)
        params["period"] = (
            "lifetime"  # The other periods mentioned in the documentation are not working.
        )
        return params

class StoriesStream(BaseMediaStream):
    """Define custom stream."""

    name = "stories"
    path = "/{user_id}/stories"  # user_id is populated using child context keys from UsersStream
    parent_stream_type = UsersStream
    primary_keys = ["id","timestamp"]
    replication_key = "timestamp"
    records_jsonpath = "$.data[*]"
    inject_context_fields: list[str] = []
    fields = [
        "id",
        "ig_id",
        "caption",
        "is_story",
        "like_count",
        "media_product_type",
        "media_type",
        "media_url",
        "owner",
        "permalink",
        "shortcode",
        "thumbnail_url",
        "timestamp",
        "username",
    ]
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
            description="Media ID.",
        ),
        th.Property(
            "ig_id",
            th.StringType,
            description="Instagram media ID.",
        ),
        th.Property(
            "caption",
            th.StringType,
            description="Caption. Excludes album children. @ symbol excluded unless the app user can perform "
            "admin-equivalent tasks on the Facebook Page connected to the Instagram account used to "
            "create the caption.",
        ),
        th.Property(
            "carousel_album_id",
            th.StringType,
            description="ID of the parent carousel album if the media is part of a carousel album.",
        ),
        th.Property(
            "is_story",
            th.BooleanType,
            description="Indicates whether the media is a story.",
        ),
        th.Property(
            "like_count",
            th.IntegerType,
            description="Count of likes on the media. Excludes likes on album child media and likes on promoted posts "
            "created from the media. Includes replies on comments.",
        ),
        th.Property(
            "media_product_type",
            th.StringType,
            description="Surface where the media is published. Can be AD, FEED, IGTV, or STORY.",
        ),
        th.Property(
            "media_type",
            th.StringType,
            description="Media type. Can be CAROUSEL_ALBUM, IMAGE, or VIDEO.",
        ),
        th.Property(
            "media_url",
            th.StringType,
            description="Media URL. Will be omitted from responses if the media contains copyrighted material, "
            "or has been flagged for a copyright violation.",
        ),
        th.Property(
            "owner",
            th.ObjectType(
                th.Property(
                    "id",
                    th.StringType,
                    description="ID of Instagram user who created the media.",
                ),
                th.Property(
                    "username",
                    th.StringType,
                    description="Username of Instagram user who created the media.",
                ),
            ),
            description="ID of Instagram user who created the media. Only returned if the app user making the query "
            "also created the media, otherwise username field will be returned instead.",
        ),
        th.Property(
            "permalink",
            th.StringType,
            description="Permanent URL to the media.",
        ),
        th.Property(
            "shortcode",
            th.StringType,
            description="Shortcode to the media.",
        ),
        th.Property(
            "thumbnail_url",
            th.StringType,
            description="Media thumbnail URL. Only available on VIDEO media.",
        ),
        th.Property(
            "timestamp",
            th.DateTimeType,
            description="ISO 8601 formatted creation date in UTC (default is UTC ±00:00)",
        ),
        th.Property(
            "username",
            th.StringType,
            description="Username of user who created the media.",
        ),
    ).to_dict()

    def get_child_context(self, record, context):
        return {
            "user_id": context["user_id"],
            "media_id": record["id"],
            "media_type": record.get("media_type"),
            "media_product_type": "STORY",
        }

class StoryInsightsStream(BaseMediaInsightsStream):
    """Story insights stream."""

    name = "story_insights"
    parent_stream_type = StoriesStream
    state_partitioning_keys = ["user_id"]
    replication_key = None
    records_jsonpath = "$.data[*]"
    breakdowns = "story_navigation_action_type"
    breakdown_metric = "navigation"
    standard_metrics = [
                    "navigation",
                    "reach",
                    "replies",
                    "shares",
                    "total_interactions",
                    "views",
                ]

    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
            description="Unique media ID from Instagram.",
        ),
        # Story metrics
        th.Property(
            "story_tap_exit",
            th.IntegerType,
            description="Number of exits from the story.",
        ),
        th.Property(
            "story_navigation",
            th.IntegerType,
            description="Number of navigations on the story.",
        ),
        th.Property(
            "story_total_interactions",
            th.IntegerType,
            description="Total number of views for the story.",
        ),
        th.Property(
            "story_reach",
            th.IntegerType,
            description="Unique accounts that viewed the story.",
        ),
        th.Property(
            "story_likes",
            th.IntegerType,
            description="Number of likes on the story.",
        ),
        th.Property(
            "story_replies",
            th.IntegerType,
            description="Number of replies to the story.",
        ),
        th.Property(
            "story_shares",
            th.IntegerType,
            description="Number of shares of the story.",
        ),
        th.Property(
            "story_swipe_forward",
            th.IntegerType,
            description="Number of swipe forwards on the story.",
        ),
        th.Property(
            "story_tap_back",
            th.IntegerType,
            description="Number of backward taps on the story.",
        ),
        th.Property(
            "story_tap_forward",
            th.IntegerType,
            description="Number of forward taps on the story.",
        ),
        th.Property(
            "story_views",
            th.IntegerType,
            description="Number of views for the story.",
        ),
        th.Property(
            "likes",
            th.IntegerType,
            description="Number of likes post or ad received.",
        ),
        th.Property(
            "comments",
            th.IntegerType,
            description="Number of comments post or ad received.",
        ),
    ).to_dict()
    
    def _metrics_for_media_type(self,media_type, media_product_type):
        """Story insights always for STORY media."""
        if media_product_type != "STORY":
            return []

        # Same metrics you used previously
        return [
            "navigation",
            "reach",
            "replies",
            "shares",
            "total_interactions",
            "views",
        ]

class UserInsightsStream(InstagramStream):
    parent_stream_type = UsersStream
    # path = "/{user_id}/insights"  # user_id is populated using child context keys from UsersStream
    replication_key = "end_time"
    records_jsonpath = "$.data[*]"
    has_pagination = True
    min_start_date: datetime = pendulum.now("UTC").subtract(years=2).add(days=1)
    max_end_date: datetime = pendulum.today("UTC")
    max_time_window: timedelta = pendulum.duration(days=1)
    time_period: str  # TODO: Use an Enum type instead
    total_value_metrics: List[str]
    time_series_metrics: List[str]
    breakdown_metrics: List[str]
    reach_metrics: str
    metric_types = ["total_value"]
    breakdowns: List[str]

    def __init__(self, tap, **kwargs):
        super().__init__(tap, **kwargs)
        # dynamically build schema based on metrics list

    @property
    def path(self) -> str:
        api_version = self.config.get("api_version")
        if api_version:
            return f"/{api_version}" + "/{user_id}/insights"
        else:
            return "/{user_id}/insights"

    def _fetch_time_based_pagination_range(
        self,
        context,
        min_since: datetime,
        max_until: datetime,
        max_time_window: timedelta,
    ) -> Tuple[datetime, datetime]:
        """
        Make "since" and "until" pagination timestamps
        Args:
            context:
            min_since: Min datetime for "since" parameter. Defaults to 2 years ago, max historical data
                       supported for Facebook metrics.
            max_until: Max datetime for which data is available. Defaults to a day ago.
            max_time_window: Maximum duration (as a "tiemdelta") between "since" and "until". Default to
                             30 days, max window supported by Facebook

        Returns: DateTime objects for "since" and "until"
        """
        two_years_ago = pendulum.now().subtract(years=1)
        min_since = max(min_since, two_years_ago)
        thirty_days_ago = pendulum.now().subtract(days=30)
        yesterday = pendulum.now().subtract(days=1)
        is_time_series = context and context.get("metric_type") == "time_series"

        if is_time_series:
            # Instagram requirement:
            # follower_count only supports last 30 days, excluding today
            state_since = self.get_starting_timestamp(context)
            if state_since:
                since = max(state_since, thirty_days_ago)
            else:
                since = thirty_days_ago
            until = min(yesterday, max_until)
            return since, until

        start_date = self.get_starting_replication_key_value(context) or self.config.get("start_date")
        if isinstance(start_date, str):
            start_date = pendulum.parse(start_date)
        try:
            since = min(max(start_date, min_since), max_until)
            window_end = min(
                self.get_replication_key_signpost(context),
                pendulum.instance(since).add(seconds=max_time_window.total_seconds()),
            )
        # seeing cases where self.get_starting_timestamp() is null
        # possibly related to target-bigquery pushing malformed state - https://gitlab.com/meltano/sdk/-/issues/300
        except TypeError:
            since = min_since
            window_end = pendulum.instance(since).add(
                seconds=max_time_window.total_seconds()
            )
        until = min(window_end, max_until)
        return since, until

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        # TODO: Is there a cleaner way to do this?
        params = super().get_url_params(context, next_page_token)
        params["period"] = context.get("period", self.time_period)
        if next_page_token:
            return params
        # Add metrics based on metric trype and breakdowns
        if context and context.get("metric_type") == "time_series":
            params["metric"] = ",".join(self.time_series_metrics)
        elif context and context.get("breakdown_run"):
            params["metric"] = ",".join(self.breakdown_metrics)
            params["metric_type"] = (
                "total_value"  # breakdowns only supported for total_value metrics
            )
        elif context and context.get("reach"):
            params["metric"] = self.reach_metrics
            params["metric_type"] = "time_series"
            #params["period"] = context.get("period")
        else:
            params["metric"] = ",".join(self.total_value_metrics)
        if context and "metric_type" in context:
            params["metric_type"] = context["metric_type"]
        if context and context.get("breakdown_run"):
            params["breakdown"] = ",".join(self.breakdowns)

        if self.has_pagination:
            since, until = self._fetch_time_based_pagination_range(
                context,
                min_since=self.min_start_date,
                max_until=self.max_end_date,
                max_time_window=self.max_time_window,
            )
            params["since"] = int(since.timestamp())
            params["until"] = int(until.timestamp())
        return params

    def get_records(self, context):
        grouped_rows: dict[tuple, dict] = {}

        def _merge_record(record: dict):
            """Combine records by (id, end_time) so different requests fill columns."""
            key = (record.get("id"), record.get("end_time"))
            base = grouped_rows.setdefault(
                key,
                {
                    "id": record.get("id"),
                    "user_id": record.get("user_id"),
                    "end_time": record.get("end_time"),
                },
            )
            for k, v in record.items():
                if v is not None:
                    base[k] = v

        for metric_type in self.metric_types:
            # Create a modified context for this metric type
            mt_context = dict(context or {})
            mt_context["metric_type"] = metric_type

            self.logger.info(f"Requesting user insights with metric type={metric_type}")

            for record in super().get_records(mt_context):
                record["requested_metric_type"] = metric_type
                _merge_record(record)

        # metrics that can have breakdown need to run separately
        if self.breakdown_metrics:
            bd_context = dict(context or {})
            bd_context["breakdown_run"] = True  # custom flag

            self.logger.info("Requesting user insights with breakdown metrics")

            for record in super().get_records(bd_context):
                record["requested_metric_type"] = "breakdown"
                _merge_record(record)

        if self.reach_metrics:
            reach_periods = ["days_28", "week"]  # <-- your new requirement

            for period in reach_periods:
                reach_context = dict(context or {})
                reach_context["reach"] = True
                reach_context["period"] = period  # <-- pass period into context

                self.logger.info(f"Requesting reach metrics with period={period}")

                for record in super().get_records(reach_context):
                    record["requested_metric_type"] = "reach"
                    record["period"] = period        # <-- carry period into records
                    _merge_record(record)

        # Yield merged rows so metrics requested in separate calls land on same row
        for row in grouped_rows.values():
            yield row

    def _request(self, prepared_request, context):
        # Keep track of the context for parse_response
        self._current_context = context
        return super()._request(prepared_request, context)

    def _extract_until_from_paging_url(self, url: str):
        """Extract the 'until' UNIX timestamp from an Instagram paging URL."""
        try:
            parsed = urllib.parse.urlparse(url)
            qs = urllib.parse.parse_qs(parsed.query)
            until_value = qs.get("until", [None])[0]
            if until_value is None:
                return None
            return pendulum.from_timestamp(int(until_value), tz="UTC")
        except Exception:
            return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        data = resp_json.get("data", [])
        if not data:
            return

        # building a dict indexed by end_time so all metrics for that date combine
        rows_by_date = {}

        # Extract the actual 'until' used by Instagram pagination
        paging = resp_json.get("paging", {})
        paging_next = paging.get("next")
        paging_prev = paging.get("previous")

        actual_until = None

        if paging_next:
            actual_until = self._extract_until_from_paging_url(paging_next)
        elif paging_prev:
            actual_until = self._extract_until_from_paging_url(paging_prev)

        # If there is no paging block (single page), fall back to request URL
        if actual_until is None and response.request and response.request.url:
            actual_until = self._extract_until_from_paging_url(response.request.url)

        # Save for total_value metrics
        self._current_window_until = actual_until

        for metric in data:
            metric_name = metric["name"].lower()
            metric_id = metric.get("id", "")
            user_id = metric_id.split("/")[0] if "/" in metric_id else None

            # ---- TIME-SERIES REACH & OTHER VALUES ----
            if "values" in metric:
                period = metric.get("period")

                period_field_map = {
                    "days_28": f"{metric_name}_28d",
                    "week": f"{metric_name}_7d",
                }
                field_name = period_field_map.get(period, metric_name)

                for point in metric["values"]:
                    ts = pendulum.parse(point["end_time"])
                    end_date = ts.format("YYYY-MM-DD")

                    row = rows_by_date.setdefault(
                        end_date,
                        {"id": user_id, "user_id": user_id, "end_time": end_date},
                    )

                    row[field_name] = point.get("value")

                continue

            # ---- TOTAL VALUE METRICS ----
            if "total_value" in metric:
                total_obj = metric["total_value"]
                total_val = total_obj.get("value")
                
                # if it's a daily metric with total_value, still give it a date
                if getattr(self, "_current_window_until", None):
                    end_time_fmt = self._current_window_until.format("YYYY-MM-DD")
                else:
                    # fallback: today's date (only happens on first page with no paging block)
                    end_time_fmt = pendulum.now("UTC").format("YYYY-MM-DD")
                
                row = rows_by_date.setdefault(
                    end_time_fmt,
                    {"id": user_id, "user_id": user_id, "end_time": end_time_fmt},
                )
                for breakdown in total_obj.get("breakdowns", []):
                    for result in breakdown.get("results", []):
                        dims = "_".join(result.get("dimension_values", [])).lower()
                        row[f"{dims}"] = result.get("value")

                # Store the total metric value
                row[metric_name] = total_val

        # Yield all combined rows
        for row in rows_by_date.values():
            yield row


class DefaultUserInsightsDailyStream(UserInsightsStream):
    """Define custom stream."""

    name = "default_user_insights_daily"
    total_value_metrics = [
        "reach",
        "profile_views",
        "accounts_engaged",
        "total_interactions",
        "likes",
        "comments",
        "shares",
        "saves",
        "replies",
        "profile_links_taps",
        "views",
        "content_views",
    ]
    time_series_metrics = []
    reach_metrics = "reach"
    breakdown_metrics = ["follows_and_unfollows"]
    time_period = "day"
    metric_type = "total_value"
    breakdowns = ["follow_type"]

    def __init__(self, tap, **kwargs):
        metric_props = [
            th.Property(metric.lower(), th.IntegerType)
            for metric in (self.total_value_metrics + self.time_series_metrics)
        ]
        base_props = [
            th.Property("id", th.StringType),
            th.Property("user_id", th.StringType),
            th.Property("end_time", th.DateTimeType),
            th.Property("follower", th.IntegerType),
            th.Property("non_follower", th.IntegerType),
            th.Property("reach_7d", th.IntegerType),
            th.Property("reach_28d", th.IntegerType),
        ]
        schema = th.PropertiesList(*base_props, *metric_props).to_dict()

        super().__init__(tap=tap, schema=schema, **kwargs)

class BaseUserInsightsLifetimeStream(InstagramStream):
    parent_stream_type = UsersStream
    records_jsonpath = "$.data[*]"
    has_pagination = True

    min_start_date: datetime = pendulum.now("UTC").subtract(years=2).add(days=1)
    max_end_date: datetime = pendulum.today("UTC")
    max_time_window: timedelta = pendulum.duration(days=30)

    time_period = "lifetime"
    timeframe = "this_month"

    schema = None  # Child classes override

    @property
    def path(self) -> str:
        api_version = self.config.get("api_version")
        if api_version:
            return f"/{api_version}" + "/{user_id}/insights"
        return "/{user_id}/insights"

    # ----- Shared pagination logic -----
    def _fetch_time_based_pagination_range(
        self,
        context,
        min_since: datetime,
        max_until: datetime,
        max_time_window: timedelta,
    ) -> Tuple[datetime, datetime]:
        """
        Make "since" and "until" pagination timestamps
        Args:
            context:
            min_since: Min datetime for "since" parameter. Defaults to 2 years ago, max historical data
                       supported for Facebook metrics.
            max_until: Max datetime for which data is available. Defaults to a day ago.
            max_time_window: Maximum duration (as a "tiemdelta") between "since" and "until". Default to
                             30 days, max window supported by Facebook

        Returns: DateTime objects for "since" and "until"
        """
        two_years_ago = pendulum.now().subtract(years=1)
        min_since = max(min_since, two_years_ago)
        thirty_days_ago = pendulum.now().subtract(days=30)
        yesterday = pendulum.now().subtract(days=1)
        is_time_series = context and context.get("metric_type") == "time_series"

        if is_time_series:
            # Instagram requirement:
            # follower_count only supports last 30 days, excluding today
            state_since = self.get_starting_timestamp(context)
            if state_since:
                since = max(state_since, thirty_days_ago)
            else:
                since = thirty_days_ago
            until = min(yesterday, max_until)
            return since, until

        start_date = self.get_starting_replication_key_value(context) or self.config.get("start_date")
        if isinstance(start_date, str):
            start_date = pendulum.parse(start_date)
        try:
            since = min(max(start_date, min_since), max_until)
            window_end = min(
                self.get_replication_key_signpost(context),
                pendulum.instance(since).add(seconds=max_time_window.total_seconds()),
            )
        # seeing cases where self.get_starting_timestamp() is null
        # possibly related to target-bigquery pushing malformed state - https://gitlab.com/meltano/sdk/-/issues/300
        except TypeError:
            since = min_since
            window_end = pendulum.instance(since).add(
                seconds=max_time_window.total_seconds()
            )
        until = min(window_end, max_until)
        return since, until

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        if context and context.get("default_metric"):
            params["metric"] = ",".join(self.default_metrics)
            params["metric_type"] = "default"
        else:
            params["metric"] = ",".join(self.total_value_metrics)
            params["metric_type"] = "total_value"
        params["timeframe"] = self.timeframe
        params["period"] = self.time_period
        if context and "lifetime_breakdown" in context:
            params["breakdown"] = context["lifetime_breakdown"]

        if self.has_pagination:
            since, until = self._fetch_time_based_pagination_range(
                context,
                min_since=self.min_start_date,
                max_until=self.max_end_date,
                max_time_window=self.max_time_window,
            )
            params["since"] = since
            params["until"] = until
        return params


class UserInsightsLifetimeStream(BaseUserInsightsLifetimeStream):
    name = "user_insights_lifetime"
    replication_key = "end_time"

    total_value_metrics = ["engaged_audience_demographics", "follower_demographics"]
    default_metrics = ["online_followers"]
    metric_types = ["total_value"]
    breakdowns_list = ["age", "city", "country", "gender"]

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("user_id", th.StringType),
        th.Property("metric", th.StringType),
        th.Property("period", th.StringType),
        th.Property("end_time", th.DateTimeType),
        th.Property("breakdowns", th.StringType),
        th.Property("dimension_key", th.StringType),
        th.Property("key", th.StringType),
        th.Property("value", th.StringType),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
    ).to_dict()

    def get_records(self, context):
        if self.default_metrics:
            metric_context = dict(context or {})
            metric_context["default_metric"] = True
            for record in super().get_records(metric_context):
                record["requested_breakdown"] = None
                record["metric_type"] = "default"
                yield record
        
        if self.total_value_metrics:
            # Loop through each breakdown automatically
            for breakdown in self.breakdowns_list:
                b_ctx = {**(context or {}), "lifetime_breakdown": breakdown}
                for record in super().get_records(b_ctx):
                    record["requested_breakdown"] = breakdown
                    record["metric_type"] = "total_value"
                    yield record

    def parse_response(self, response):
        for row in response.json()["data"]:
            base = {
                "id": row["id"].split("/")[0],
                "metric": row["name"],
                "period": row["period"],
                "title": row["title"],
                "description": row["description"],
            }

            # ---- TOTAL VALUE LOGIC ----
            if "total_value" in row:
                total = row["total_value"]
                breakdowns = total.get("breakdowns") or []
                end_time = pendulum.now("UTC").to_datetime_string()

                if breakdowns:
                    for breakdown in breakdowns:
                        dim_keys = breakdown.get("dimension_keys") or []
                        dim_key_suffix = "_".join(dim_keys)
                        for result in breakdown.get("results", []):
                            dim_vals = result.get("dimension_values") or []
                            item = base.copy()
                            item["metric"] = row["name"]
                            item["dimension_key"] = dim_key_suffix or None
                            item["key"] = ".".join(dim_vals) if dim_vals else None
                            item["value"] = str(result.get("value"))
                            item["breakdowns"] = json.dumps(breakdown)
                            item["end_time"] = end_time
                            yield item
                else:
                    item = base.copy()
                    item["value"] = str(total.get("value"))
                    item["dimension_key"] = None
                    item["key"] = None
                    item["breakdowns"] = ""
                    item["end_time"] = end_time
                    yield item

            # ---- DEFAULT VALUES (ARRAY) ----
            if "values" in row:
                for val in row["values"]:
                    end_time = (
                        pendulum.parse(val["end_time"]).to_datetime_string()
                        if "end_time" in val
                        else pendulum.now("UTC").to_datetime_string()
                    )

                    for k, v in val.get("value", {}).items():
                        item = base.copy()
                        item["key"] = k
                        item["value"] = str(v)
                        item["breakdowns"] = ""
                        item["end_time"] = end_time
                        yield item

class UserInsightsCustomStream(UserInsightsStream):
    """Define custom stream."""

    def __init__(self, tap, report_config: Dict[str, Any], **kwargs):
        self.report_config = report_config
        self.name = report_config["name"]
        self.metrics = report_config.get("metrics", [])
        self.time_period = report_config.get("time_period", "day")
        self.timeframe = report_config.get("timeframe")
        self.metric_type = report_config.get("metric_type", "total_value")
        self.breakdown = report_config.get("breakdown")
        self.ig_user_ids = tap.config.get("ig_user_ids", [])

        metric_props = [
            th.Property(metric.lower(), th.IntegerType) for metric in self.metrics
        ]
        base_props = [
            th.Property("id", th.StringType),
            th.Property("user_id", th.StringType),
            th.Property("end_time", th.DateTimeType),
        ]
        schema = th.PropertiesList(*base_props, *metric_props).to_dict()

        super().__init__(tap=tap, schema=schema, **kwargs)

    @property
    def partitions(self) -> Optional[List[dict]]:
        """One partition per Instagram user ID."""
        return [{"user_id": user_id} for user_id in self.ig_user_ids]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Add parameters based on report definition."""
        params = super().get_url_params(context, next_page_token)

        if self.metrics:
            params["metric"] = ",".join(self.metrics)
        if self.breakdown:
            params["breakdown"] = self.breakdown
        if self.metric_type:
            params["metric_type"] = self.metric_type
        if self.time_period:
            params["period"] = self.time_period
        if self.timeframe:
            params["timeframe"] = self.timeframe
        self.logger.info(f"URL params: {params}")
        return params
