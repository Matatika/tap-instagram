"""Stream type classes for tap-instagram."""

from datetime import datetime, timedelta
import json
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pendulum
import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_instagram.client import InstagramStream

# SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class UsersStream(InstagramStream):
    """Define custom stream."""

    name = "users"
    path = "/{user_id}"
    primary_keys = ["id"]
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


class MediaStream(InstagramStream):
    """Define custom stream."""

    name = "media"
    path = "/{user_id}/media"  # user_id is populated using child context keys from UsersStream
    parent_stream_type = UsersStream
    primary_keys = ["id"]
    replication_key = "timestamp"
    records_jsonpath = "$.data[*]"
    fields = [
        "id",
        "ig_id",
        "caption",
        "comments_count",
        "is_comment_enabled",
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

    def make_since_param(self, context: Optional[dict]) -> datetime:
        state_ts = self.get_starting_timestamp(context)
        if state_ts:
            return pendulum.instance(state_ts).subtract(
                days=self.config["media_insights_lookback_days"]
            )
        else:
            return state_ts

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["fields"] = ",".join(self.fields)
        params["since"] = self.make_since_param(context)
        return params

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "user_id": context["user_id"],
            "media_id": record["id"],
            "media_type": record.get("media_type"),
            # media_product_type not present for carousel children media
            "media_product_type": record.get("media_product_type"),
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        for row in extract_jsonpath(self.records_jsonpath, input=response.json()):
            if "timestamp" in row:
                row["timestamp"] = pendulum.parse(row["timestamp"]).format(
                    "YYYY-MM-DD HH:mm:ss"
                )
            yield row


class StoriesStream(InstagramStream):
    """Define custom stream."""

    name = "stories"
    path = "/{user_id}/stories"  # user_id is populated using child context keys from UsersStream
    parent_stream_type = UsersStream
    primary_keys = ["id"]
    records_jsonpath = "$.data[*]"
    fields = [
        "id",
        "ig_id",
        "caption",
        "comments_count",
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

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["fields"] = ",".join(self.fields)
        return params

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            # "user_id": context["user_id"],
            "media_id": record["id"],
            "media_type": record["media_type"],
            # media_product_type not present for carousel children media
            "media_product_type": record.get("media_product_type"),
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        for row in extract_jsonpath(self.records_jsonpath, input=response.json()):
            if "timestamp" in row:
                row["timestamp"] = pendulum.parse(row["timestamp"]).format(
                    "YYYY-MM-DD HH:mm:ss"
                )
            yield row


class MediaChildrenStream(MediaStream):
    """Define custom stream."""

    name = "media_children"
    parent_stream_type = MediaStream
    state_partitioning_keys = ["user_id"]
    path = "/{media_id}/children"  # media_id is populated using child context keys from MediaStream
    # caption, comments_count, is_comment_enabled, like_count, media_product_type
    # not available on album children
    # TODO: Is media_product_type available on children of some media types? carousel vs album children?
    # https://developers.facebook.com/docs/instagram-api/reference/ig-media#fields
    fields = [
        "id",
        "ig_id",
        "media_type",
        "media_url",
        "owner",
        "permalink",
        "shortcode",
        "thumbnail_url",
        "timestamp",
        "username",
    ]

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        for row in extract_jsonpath(self.records_jsonpath, input=response.json()):
            if "timestamp" in row:
                row["timestamp"] = pendulum.parse(row["timestamp"]).format(
                    "YYYY-MM-DD HH:mm:ss"
                )
            yield row


class MediaCommentsStream(MediaStream):
    """Define custom stream."""

    name = "media_comments"
    parent_stream_type = MediaStream
    state_partitioning_keys = ["user_id"]
    path = "/{media_id}/comments"  # media_id is populated using child context keys from MediaStream
    # caption, comments_count, is_comment_enabled, like_count, media_product_type
    # not available on album children
    # TODO: Is media_product_type available on children of some media types? carousel vs album children?
    # https://developers.facebook.com/docs/instagram-api/reference/ig-media#fields
    fields = [
        "id",
        "text",
        "timestamp",
        "username",
        "like_count",
        "replies",
        "hidden",
        "owner",
        "parent_id"
    ]
    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
            description="Media ID.",
        ),
        th.Property(
            "user_id",
            th.StringType,
            description="Instagram user ID.",
        ),
        th.Property(
            "parent_id",
            th.StringType,
            description = "ID of the parent IG Comment if this comment was created on another IG Comment"
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

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        for row in extract_jsonpath(self.records_jsonpath, input=response.json()):
            if "timestamp" in row:
                row["timestamp"] = pendulum.parse(row["timestamp"]).format(
                    "YYYY-MM-DD HH:mm:ss"
                )
            yield row


class MediaInsightsStream(InstagramStream):
    """Define custom stream."""

    name = "media_insights"
    parent_stream_type = MediaStream
    state_partitioning_keys = ["user_id"]
    primary_keys = ["id"]
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
            "reel_total_interactions",
            th.IntegerType,
            description="Total interactions on the reel.",
        ),
        # Story metrics
        th.Property(
            "story_exits",
            th.IntegerType,
            description="Number of exits from the story.",
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
            "story_taps_back",
            th.IntegerType,
            description="Number of backward taps on the story.",
        ),
        th.Property(
            "story_taps_forward",
            th.IntegerType,
            description="Number of forward taps on the story.",
        ),
        # Feed (video/photo) metrics
        th.Property(
            "video_photo_engagement",
            th.IntegerType,
            description="Total engagement on the video or photo post.",
        ),
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
            "carousel_saved",
            th.IntegerType,
            description="Number of times the carousel post was saved.",
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

    @staticmethod
    def _metrics_for_media_type(self, media_type: str, media_product_type: str):
        # TODO: Define types for these function args
        if media_type in ("IMAGE", "VIDEO"):
            if media_product_type == "STORY":
                return [
                    "exits",
                    "likes",
                    "reach",
                    "replies",
                    "taps_forward",
                    "taps_back",
                    "total_interactions"
                ]
            elif media_product_type == "REELS":
                return [
                    "comments",
                    "likes",
                    "reach",
                    "saved",
                    "shares",
                    "total_interactions",
                ]
            else:  # media_product_type is "AD" or "FEED"
                metrics = [
                    "total_interactions",
                    "likes",
                    "comments",
                    "reach",
                    "saved",
                ]
                return metrics
        elif media_type == "CAROUSEL_ALBUM":
            return [
                "reach",
                "saved",
                "total_interactions",
            ]
        else:
            raise ValueError(
                f"media_type from parent record must be one of IMAGE, VIDEO, CAROUSEL_ALBUM, got: {media_type}"
            )

    @property
    def path(self):
        api_version = self.config.get("api_version")
        if api_version:
            return f"/{api_version}" + "/{media_id}/insights"
        else:
            return "/{media_id}/insights"
    
    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        metrics = self._metrics_for_media_type(
            self,context["media_type"], context["media_product_type"]
        )
        params["metric"] = ",".join(metrics)
        params["period"] = (
            "lifetime"  # The other periods mentioned in the documentation are not working.
        )
        return params

    def validate_response(self, response: requests.Response) -> None:
        resp_json = response.json()
        if (
            resp_json.get("error", {}).get("error_user_title")
            == "Media posted before business account conversion"
            or "(#10) Not enough viewers for the media to show insights"
            in str(response.json().get("error", {}).get("message"))
        ):
            self.logger.warning(f"Skipping: {response.json()['error']}")
            return
        super().validate_response(response)

    def parse_response(
        self, response: requests.Response
    ) -> Iterable[dict]:
        resp_json = response.json()

        # Handle API error cases gracefully
        if (
            resp_json.get("error", {}).get("error_user_title")
            == "Media posted before business account conversion"
            or "(#10) Not enough viewers for the media to show insights"
            in str(resp_json.get("error", {}).get("message"))
        ):
            self.logger.warning(f"Skipping media due to no insights: {resp_json.get('error')}")
            return

        # Instagram sometimes wraps a single record directly in "data"
        data_items = resp_json.get("data")
        if not data_items:
            if "name" in resp_json:
                data_items = [resp_json]
            else:
                return

        record = {}

        for metric in data_items:
            # Example: "18077067653335581/insights/comments/lifetime"
            insight_id = metric.get("id")
            if not insight_id:
                continue

            # Extract the media_id portion before the first "/"
            media_id = insight_id.split("/")[0]
            record["id"] = media_id

            metric_name = metric.get("name", "").lower()
            values = metric.get("values", [])
            if not values:
                continue

            val = values[0].get("value")
            desc = (metric.get("description") or "").lower()
            if "reel" in desc:
                prefix = "reel_"
            elif "story" in desc:
                prefix = "story_"
            elif "photo" in desc or "video" in desc:
                prefix = "video_photo_"
            elif "carousel" in desc:
                prefix = "carousel_album_"
            else:
                prefix = ""

            key_name = f"{prefix}{metric_name}"

            # Handle nested dicts
            if isinstance(val, dict):
                for subkey, subval in val.items():
                    record[f"{key_name}_{subkey}"] = subval
            else:
                record[key_name] = val

        if record:
            yield record

class StoryInsightsStream(InstagramStream):
    """Define custom stream."""

    name = "story_insights"
    path = "/{media_id}/insights"
    parent_stream_type = StoriesStream
    state_partitioning_keys = ["user_id"]
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.data[*]"

    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
            description="",
        ),
        th.Property(
            "name",
            th.StringType,
            description="",
        ),
        th.Property(
            "period",
            th.StringType,
            description="",
        ),
        th.Property(
            "end_time",
            th.DateTimeType,
            description="",
        ),
        th.Property(
            "context",
            th.StringType,
            description="",
        ),
        th.Property(
            "value",
            th.IntegerType,
            description="",
        ),
        th.Property(
            "title",
            th.StringType,
            description="",
        ),
        th.Property(
            "description",
            th.StringType,
            description="",
        ),
    ).to_dict()

    @staticmethod
    def _metrics_for_media_type(media_type: str, media_product_type: str):
        # TODO: Define types for these function args
        if media_type in ("IMAGE", "VIDEO"):
            if media_product_type == "STORY":
                return [
                    # "exits",
                    "impressions",
                    "reach",
                    "replies",
                    # "taps_forward",
                    # "taps_back",
                ]
            else:  # media_product_type is "AD" or "FEED"
                metrics = [
                    "total_interactions",
                    "impressions",
                    "reach",
                    "saved",
                ]
                if media_type == "VIDEO":
                    metrics.append("video_views")
                return metrics
        elif media_type == "CAROUSEL_ALBUM":
            return [
                "total_interactions",
                "impressions",
                "reach",
                "saved",
                "video_views",
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
        return params

    def validate_response(self, response: requests.Response) -> None:
        if (
            response.json().get("error", {}).get("error_user_title")
            == "Media posted before business account conversion"
            or "(#10) Not enough viewers for the media to show insights"
            in str(response.json().get("error", {}).get("message"))
        ):
            self.logger.warning(f"Skipping: {response.json()['error']}")
            return
        super().validate_response(response)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        # Handle the specific case where FB returns error because media was posted before business acct creation
        # TODO: Refactor to raise a specific error in validate_response and handle that instead
        if (
            resp_json.get("error", {}).get("error_user_title")
            == "Media posted before business account conversion"
            or "(#10) Not enough viewers for the media to show insights"
            in str(resp_json.get("error", {}).get("message"))
        ):
            return
        for row in resp_json["data"]:
            base_item = {
                "name": row["name"],
                "period": row["period"],
                "title": row["title"],
                "id": row["id"],
                "description": row["description"],
            }
            if "values" in row:
                for values in row["values"]:
                    if isinstance(values["value"], dict):
                        for key, value in values["value"].items():
                            item = {
                                "context": key,
                                "value": value,
                                "end_time": pendulum.parse(values["end_time"]).format(
                                    "YYYY-MM-DD HH:mm:ss"
                                ),
                            }
                            item.update(base_item)
                            yield item
                    else:
                        values.update(base_item)
                        if "end_time" in values:
                            values["end_time"] = pendulum.parse(
                                values["end_time"]
                            ).format("YYYY-MM-DD HH:mm:ss")
                        yield values


class UserInsightsStream(InstagramStream):
    parent_stream_type = UsersStream
    #path = "/{user_id}/insights"  # user_id is populated using child context keys from UsersStream
    primary_keys = ["id"]
    replication_key = "end_time"
    records_jsonpath = "$.data[*]"
    has_pagination = True
    min_start_date: datetime = pendulum.now("UTC").subtract(years=2).add(days=1)
    max_end_date: datetime = pendulum.today("UTC")
    max_time_window: timedelta = pendulum.duration(days=30)
    time_period: str  # TODO: Use an Enum type instead
    metrics: List[str]
    metric_type = "total_value"
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
        try:
            since = min(max(self.get_starting_timestamp(context), min_since), max_until)
            window_end = min(
                self.get_replication_key_signpost(context),
                pendulum.instance(since).add(seconds=max_time_window.total_seconds()),
            )
        # seeing cases where self.get_starting_timestamp() is null
        # possibly related to target-bigquery pushing malformed state - https://gitlab.com/meltano/sdk/-/issues/300
        except TypeError:
            since = min_since
            window_end = pendulum.instance(since).add(seconds=max_time_window.seconds)
        until = min(window_end, max_until)
        return since, until

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        # TODO: Is there a cleaner way to do this?
        params = super().get_url_params(context, next_page_token)
        if next_page_token:
            return params
        params["metric"] = ",".join(self.metrics)
        params["period"] = self.time_period
        params["metric_type"] = self.metric_type
        if getattr(self, "breakdowns", None):
            params["breakdowns"] = ",".join(self.breakdowns)

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

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        data = resp_json.get("data", [])
        if not data:
            return

        #building a dict indexed by end_time so all metrics for that date combine
        rows_by_date = {}

        for metric in data:
            metric_name = metric["name"].lower()
            metric_id = metric.get("id", "")
            user_id = metric_id.split("/")[0] if "/" in metric_id else None

            # Handle case with `values` list
            if "values" in metric:
                for val in metric["values"]:
                    end_time = val.get("end_time")
                    if not end_time:
                        continue
                    end_time_fmt = pendulum.parse(end_time).format("YYYY-MM-DD HH:mm:ss")
                    value = val.get("value")
                    row = rows_by_date.setdefault(
                        end_time_fmt,
                        {"id": user_id, "user_id": user_id, "end_time": end_time_fmt},
                    )
                    row[metric_name] = value

            # Handle case with `total_value`
            elif "total_value" in metric:
                total_obj = metric["total_value"]
                total_val = total_obj.get("value")
                # if it's a daily metric with total_value, still give it a date
                end_time_fmt = pendulum.now("UTC").format("YYYY-MM-DD HH:mm:ss")
                row = rows_by_date.setdefault(
                    end_time_fmt,
                    {"id": user_id, "user_id": user_id, "end_time": end_time_fmt},
                )
                if "breakdowns" in total_obj:
                    for breakdown in total_obj["breakdowns"]:
                        dims = "_".join(breakdown.get("dimension_values", []))
                        row[f"{metric_name}_{dims}"] = breakdown.get("value")
                    row[metric_name] = total_val
                row[metric_name] = total_val

        # Yield all combined rows
        for row in rows_by_date.values():
            yield row


class UserInsightsOnlineFollowersStream(UserInsightsStream):
    """Define custom stream."""

    name = "user_insights_online_followers"
    metrics = ["online_followers"]
    time_period = "lifetime"
    # TODO: Add note about online_followers seemingly only going back 30 days


class UserInsightsFollowersStream(UserInsightsStream):
    """Define custom stream."""

    name = "user_insights_followers"
    metrics = ["follower_count"]
    time_period = "day"
    min_start_date = pendulum.now("UTC").subtract(days=30)


class DefaultUserInsightsDailyStream(UserInsightsStream):
    """Define custom stream."""

    name = "default_user_insights_daily"
    metrics = [
        "reach"
        , "website_clicks"
        , "profile_views"
        , "accounts_engaged"
        , "total_interactions"
        , "likes"
        , "comments"
        , "shares"
        , "saves"
        , "replies"
        , "follows_and_unfollows"
        , "profile_links_taps"
        , "views"
        , "content_views"
    ]
    time_period = "day"
    metric_type = "total_value"
    breakdowns = ["contact_button_type","follow_type","media_product_type"]

    def __init__(self, tap, **kwargs):
        metric_props = [
            th.Property(metric.lower(), th.IntegerType)
            for metric in self.metrics
        ]
        base_props = [
            th.Property("id", th.StringType),
            th.Property("user_id", th.StringType),
            th.Property("end_time", th.DateTimeType),
        ]
        schema = th.PropertiesList(*base_props, *metric_props).to_dict()

        super().__init__(tap=tap, schema=schema, **kwargs)


class UserInsightsLifetimeStream(InstagramStream):
    """Define custom stream."""

    name = "user_insights_lifetime"
    parent_stream_type = UsersStream
    primary_keys = ["id","breakdowns"]
    replication_key = "end_time"
    records_jsonpath = "$.data[*]"
    has_pagination = True
    min_start_date: datetime = pendulum.now("UTC").subtract(years=2).add(days=1)
    max_end_date: datetime = pendulum.today("UTC")
    max_time_window: timedelta = pendulum.duration(days=30)
    metrics = ["engaged_audience_demographics", "follower_demographics"]
    time_period = "lifetime"
    timeframe = "this_month"
    breakdowns_list = ["age", "city", "country", "gender"]
    metric_type = "total_value"
    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
            description="",
        ),
        th.Property(
            "user_id",
            th.StringType,
            description="",
        ),
        th.Property(
            "metric",
            th.StringType,
            description="",
        ),
        th.Property(
            "period",
            th.StringType,
            description="",
        ),
        th.Property(
            "end_time",
            th.DateTimeType,
            description="",
        ),
        th.Property(
            "breakdowns",
            th.StringType,
            description="",
        ),
        th.Property(
            "value",
            th.StringType,
            description="",
        ),
        th.Property(
            "title",
            th.StringType,
            description="",
        ),
        th.Property(
            "description",
            th.StringType,
            description="",
        ),
    ).to_dict()

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
        try:
            since = min(max(self.get_starting_timestamp(context), min_since), max_until)
            window_end = min(
                self.get_replication_key_signpost(context),
                pendulum.instance(since).add(seconds=max_time_window.total_seconds()),
            )
        # seeing cases where self.get_starting_timestamp() is null
        # possibly related to target-bigquery pushing malformed state - https://gitlab.com/meltano/sdk/-/issues/300
        except TypeError:
            since = min_since
            window_end = pendulum.instance(since).add(seconds=max_time_window.seconds)
        until = min(window_end, max_until)
        return since, until
    
    def get_url_params(self, context, next_page_token):
        """Return a dictionary of URL query parameters."""
        params = super().get_url_params(context, next_page_token)
        params["metric"] = ",".join(self.metrics)
        params["timeframe"] = self.timeframe
        params["period"] = self.time_period
        params["metric_type"] = self.metric_type
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
    
    @property
    def path(self) -> str:
        api_version = self.config.get("api_version")
        if api_version:
            return f"/{api_version}" + "/{user_id}/insights"
        else:
            return "/{user_id}/insights"
    
    def get_records(self, context):

        for breakdown in self.breakdowns_list:

            # Create a modified context for this breakdown
            b_context = dict(context or {})
            b_context["lifetime_breakdown"] = breakdown

            self.logger.info(f"Requesting lifetime insights with breakdown={breakdown}")

            for record in super().get_records(b_context):
                record["requested_breakdown"] = breakdown
                yield record
    
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        resp_json = response.json()
        for row in resp_json["data"]:
            base_item = {
                "metric": row["name"],
                "period": row["period"],
                "title": row["title"],
                "id": row["id"],
                "description": row["description"],
            }
            if "total_value" in row:
                total = row["total_value"]
                item = dict(base_item)
                item["value"] = total.get("value")
                breakdowns = total.get("breakdowns")
                if breakdowns:
                    item["breakdowns"] = json.dumps(breakdowns)
                # Use current date/time as end_time if not given
                item["end_time"] = pendulum.now("UTC").format("YYYY-MM-DD HH:mm:ss")
                yield item
    


class UserInsightsWeeklyStream(UserInsightsStream):
    """Define custom stream."""

    name = "user_insights_weekly"
    metrics = [
        "impressions",
        "reach",
    ]
    time_period = "week"


class UserInsights28DayStream(UserInsightsStream):
    """Define custom stream."""

    name = "user_insights_28day"
    metrics = [
        "impressions",
        "reach",
    ]
    time_period = "days_28"


class UserInsightsCustomStream(UserInsightsStream):
    """Define custom stream."""

    def __init__(self, tap, report_config: Dict[str, Any],**kwargs):
        
        self.report_config = report_config
        self.name = report_config["name"]
        self.metrics = report_config.get("metrics", [])
        self.time_period = report_config.get("time_period", "day")
        self.timeframe = report_config.get("timeframe")
        self.metric_type = report_config.get("metric_type", "total_value")
        self.breakdown = report_config.get("breakdown")
        self.ig_user_ids = tap.config.get("ig_user_ids", [])

        metric_props = [
            th.Property(metric.lower(), th.IntegerType)
            for metric in self.metrics
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
