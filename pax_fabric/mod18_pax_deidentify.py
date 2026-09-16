"""Deterministic, one-way de-identification for PAX v1.11.15 CSV output."""

from __future__ import annotations

import hashlib
import hmac
import csv
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable

DEFAULT_SALT = b"PAX-Deidentify-Salt-v1-DO-NOT-CHANGE-7f3c1e9b2d846050a1c4e8b3"
EMAIL_DOMAIN = "deidentified.domain"

JSON_FIELD_TYPES = {
    "UserId": "upn",
    "MailboxOwnerUPN": "upn",
    "LogonUserSid": "sid",
    "MailboxOwnerSid": "sid",
    "MailboxGuid": "guid",
    "UserKey": "guid",
    "TokenObjectId": "guid",
    "SessionId": "guid",
    "AppAccessContext.AADSessionId": "guid",
    "AppAccessContext.UniqueTokenId": "token",
    "ClientIP": "ip",
    "ClientIPAddress": "ip",
    "ActorIpAddress": "ip",
    "SiteUrl": "resource",
    "SourceRelativeUrl": "resource",
    "SourceFileName": "file",
    "CopilotEventData.ThreadId": "token",
    "CopilotEventData.AccessedResources[].SiteUrl": "resource",
    "CopilotEventData.AccessedResources[].Name": "file",
    "Folders[].FolderItems[].InternetMessageId": "token",
    "Folders[].FolderItems[].Id": "token",
    "Folders[].FolderItems[].ImmutableId": "token",
    "Folders[].FolderItems[].Subject": "token",
    "Folders[].Id": "token",
    "Item.InternetMessageId": "token",
    "Item.Id": "token",
    "Item.ImmutableId": "token",
    "Item.Subject": "token",
    "Item.Attachments": "token",
    "Item.ParentFolder.Id": "token",
}

RAW_COLUMN_TYPES = {
    "UserId": "upn",
    "MailboxOwnerUPN": "upn",
    "MailboxGuid": "guid",
    "LogonUserSid": "sid",
    "MailboxOwnerSid": "sid",
    "DeviceDisplayName": "name",
    "SiteUrl": "resource",
    "SourceRelativeUrl": "resource",
    "SourceFileName": "file",
    "AccessedResource_SiteUrl": "resource",
    "AccessedResource_Name": "file",
    "ClientIP": "ip",
    "ClientIPAddress": "ip",
    "ActorIpAddress": "ip",
    "UserKey": "guid",
    "TokenObjectId": "guid",
    "SessionId": "guid",
    "ThreadId": "token",
    "ChatId": "token",
    "ConversationId": "token",
    "Site": "resource",
    "MeetingURL": "resource",
    "TeamName": "name",
    "ChannelName": "name",
    "VideoName": "file",
    "FormName": "file",
    "userPrincipalName": "upn",
    "displayName": "name",
    "mail": "upn",
    "givenName": "name",
    "surname": "name",
    "UserName": "upn",
    "employeeId": "token",
    "onPremisesImmutableId": "token",
    "proxyAddresses_Primary": "proxy",
    "proxyAddresses_All": "proxy",
    "id": "guid",
    "manager_id": "guid",
    "manager_userPrincipalName": "upn",
    "manager_displayName": "name",
    "manager_mail": "upn",
    "ManagerID": "guid",
}

JSON_COLUMNS = ("AuditData", "CopilotEventData")
JSON_FIELD_TYPES_CASEFOLD = {
    key.casefold(): value for key, value in JSON_FIELD_TYPES.items()
}
RAW_COLUMN_TYPES_CASEFOLD = {
    key.casefold(): value for key, value in RAW_COLUMN_TYPES.items()
}
JSON_COLUMNS_CASEFOLD = {column.casefold() for column in JSON_COLUMNS}
DEIDENTIFIED_DOMAIN_SUFFIX = "@deidentified.domain"
IDENTITY_COLUMNS = (
    "Audit_UserId",
    "UserId",
    "PersonId",
    "userPrincipalName",
    "MailboxOwnerUPN",
)


def classify_csv_deidentification(path: str | Path) -> str:
    """Classify an append target as deid, raw, or unknown."""
    source = Path(path)
    if not source.is_file():
        return "unknown"
    try:
        with source.open("r", encoding="utf-8-sig", newline="") as source_file:
            reader = csv.DictReader(source_file)
            if not reader.fieldnames:
                return "unknown"
            columns_by_casefold = {
                column.casefold(): column for column in reader.fieldnames
            }
            identity_column = next(
                (
                    columns_by_casefold[column.casefold()]
                    for column in IDENTITY_COLUMNS
                    if column.casefold() in columns_by_casefold
                ),
                None,
            )
            if identity_column is None:
                return "unknown"
            saw_value = False
            for index, row in enumerate(reader):
                if index >= 200:
                    break
                value = str(row.get(identity_column) or "")
                if not value:
                    continue
                saw_value = True
                if value.rstrip().lower().endswith(DEIDENTIFIED_DOMAIN_SUFFIX):
                    return "deid"
            return "raw" if saw_value else "unknown"
    except (OSError, csv.Error, UnicodeError):
        return "unknown"


def assert_append_deidentify_consistency(
    targets: Iterable[tuple[str, str]],
    *,
    run_deidentified: bool,
    is_rollup: bool,
) -> None:
    """Reject append combinations that would mix or re-hash identities."""
    for path, label in targets:
        if not path:
            continue
        state = classify_csv_deidentification(path)
        if state == "unknown":
            continue
        target_deidentified = state == "deid"
        if target_deidentified != run_deidentified:
            description = (
                "is already DEIDENTIFIED but this run is NOT deidentified"
                if target_deidentified
                else "is NOT deidentified but this run IS deidentified"
            )
            raise ValueError(
                f"Deidentify/append mismatch: the {label} target {description}. "
                "Mixing deidentified and raw identities in one file corrupts "
                "user joins and distinct counts. Use a separate target for "
                f"deidentified output, or use a matching deidentify state. Target: {path}"
            )
        if target_deidentified and run_deidentified and not is_rollup:
            raise ValueError(
                "Deidentify cannot append to an already-deidentified file in "
                f"non-rollup mode: the {label} target would have its existing "
                "rows re-hashed. Append on a raw master and deidentify a fresh "
                f"copy, or use Rollup/RollupPlusRaw. Target: {path}"
            )


class PaxDeidentifier:
    """Produces stable analytics-safe tokens without retaining a reverse map."""

    def __init__(self, salt: bytes = DEFAULT_SALT):
        self.salt = salt
        self._cache: dict[tuple[str, str, int], str] = {}

    def _hex(self, value: str, length: int = 12) -> str:
        key = ("hex", value, length)
        if key not in self._cache:
            norm = value.strip().lower().encode("utf-8")
            self._cache[key] = hmac.new(self.salt, norm, hashlib.sha256).hexdigest()[:length]
        return self._cache[key]

    def deid_name(self, value: str) -> str:
        return self._hex(value) if value else value

    def deid_upn(self, value: str) -> str:
        return f"{self._hex(value)}@{EMAIL_DOMAIN}" if value else value

    def deid_guid(self, value: str) -> str:
        if not value:
            return value
        digest = self._hex(value, 32)
        return f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:]}"

    def deid_sid(self, value: str) -> str:
        if not value:
            return value
        digest = self._hex(value, 32)
        return "S-1-5-21-" + "-".join(str(int(digest[i:i + 8], 16)) for i in range(0, 32, 8))

    def deid_ip(self, value: str) -> str:
        if not value:
            return value
        digest = self._hex(value, 32)
        if ":" in value:
            return ":".join(digest[i:i + 4] for i in range(0, 32, 4))
        return ".".join(str(int(digest[i:i + 2], 16)) for i in range(0, 8, 2))

    def deid_resource(self, value: str) -> str:
        return f"site_{self._hex(value)}" if value else value

    def deid_file(self, value: str) -> str:
        return f"file_{self._hex(value)}" if value else value

    def deid_token(self, value: str) -> str:
        return self._hex(value) if value else value

    def deid_proxy(self, value: str) -> str:
        if not value:
            return value
        values = []
        for entry in value.split(";"):
            if not entry:
                values.append(entry)
            elif ":" in entry:
                prefix, address = entry.split(":", 1)
                values.append(f"{prefix}:{self.deid_upn(address)}")
            else:
                values.append(self.deid_upn(entry))
        return ";".join(values)

    def deid_by_type(self, value: str, value_type: str) -> str:
        return {
            "upn": self.deid_upn,
            "name": self.deid_name,
            "guid": self.deid_guid,
            "sid": self.deid_sid,
            "token": self.deid_token,
            "resource": self.deid_resource,
            "file": self.deid_file,
            "proxy": self.deid_proxy,
            "ip": self.deid_ip,
        }[value_type](value)

    def deid_json(self, value: str) -> str:
        """Recursively scrub identity and resource fields from AuditData JSON.

        A malformed value is redacted, never passed through, so enabling the
        switch cannot leak PII through an unparseable nested payload.
        """
        if not value:
            return value
        try:
            node = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return "[REDACTED-DEIDENTIFY]"
        if node is None:
            return value

        def scrub(item: Any, path: str = "") -> Any:
            if isinstance(item, dict):
                return {
                    key: scrub(value, f"{path}.{key}" if path else key)
                    for key, value in item.items()
                }
            if isinstance(item, list):
                return [scrub(value, f"{path}[]") for value in item]
            if item in (None, ""):
                return item
            value_type = JSON_FIELD_TYPES_CASEFOLD.get(path.casefold())
            if value_type and isinstance(item, str):
                return self.deid_by_type(item, value_type)
            return item

        return json.dumps(scrub(node), ensure_ascii=False, separators=(",", ":"))

    def deidentify_purview_record(self, row: dict[str, Any]) -> dict[str, Any]:
        out = dict(row)
        for key, value in tuple(out.items()):
            if value in (None, ""):
                continue
            text = str(value)
            key_folded = key.casefold()
            if key_folded in JSON_COLUMNS_CASEFOLD:
                out[key] = self.deid_json(text)
            elif key_folded in RAW_COLUMN_TYPES_CASEFOLD:
                out[key] = self.deid_by_type(
                    text, RAW_COLUMN_TYPES_CASEFOLD[key_folded]
                )
        return out

    def deidentify_record(self, dataset: str, row: dict[str, Any]) -> dict[str, Any]:
        return self.deidentify_purview_record(row)

    def deidentify_rows(self, dataset: str, rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        return [self.deidentify_record(dataset, row) for row in rows]

    def deidentify_csv(self, path: str | Path) -> None:
        source = Path(path)
        if not source.is_file():
            return

        temp_path: str | None = None
        try:
            with source.open("r", encoding="utf-8-sig", newline="") as source_file:
                reader = csv.DictReader(source_file)
                if not reader.fieldnames:
                    return
                active = any(
                    column.casefold() in RAW_COLUMN_TYPES_CASEFOLD
                    or column.casefold() in JSON_COLUMNS_CASEFOLD
                    for column in reader.fieldnames
                )
                if not active:
                    return
                with tempfile.NamedTemporaryFile(
                    "w",
                    encoding="utf-8",
                    newline="",
                    delete=False,
                    dir=source.parent,
                    prefix=f"{source.name}.",
                    suffix=".deid.tmp",
                ) as temp_file:
                    temp_path = temp_file.name
                    writer = csv.DictWriter(
                        temp_file,
                        fieldnames=reader.fieldnames,
                        lineterminator="\n",
                        extrasaction="ignore",
                    )
                    writer.writeheader()
                    row_count = 0
                    for row in reader:
                        writer.writerow(self.deidentify_purview_record(row))
                        row_count += 1
            if row_count == 0:
                os.unlink(temp_path)
                temp_path = None
                return
            os.replace(temp_path, source)
            temp_path = None
        finally:
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)


default_deidentifier = PaxDeidentifier()
