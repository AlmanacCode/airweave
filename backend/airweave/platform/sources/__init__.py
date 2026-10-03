"""Lazy public source exports; importing native contracts does not load every provider."""

from importlib import import_module

_SOURCE_MODULES = {
    "AirtableSource": "airtable",
    "ApolloSource": "apollo",
    "AsanaSource": "asana",
    "AttioSource": "attio",
    "BitbucketSource": "bitbucket",
    "BoxSource": "box",
    "CalSource": "calcom",
    "ClickUpSource": "clickup",
    "CodaSource": "coda",
    "ConfluenceSource": "confluence",
    "CTTISource": "ctti",
    "Document360Source": "document360",
    "DropboxSource": "dropbox",
    "EnronSource": "enron",
    "ExceptionStubSource": "exception_stub",
    "FileStubSource": "file_stub",
    "FirefliesSource": "fireflies",
    "FreshdeskSource": "freshdesk",
    "GitHubSource": "github",
    "GitLabSource": "gitlab",
    "HerbCodeReviewSource": "herb",
    "HerbDocumentsSource": "herb",
    "HerbMeetingsSource": "herb",
    "HerbMessagingSource": "herb",
    "HerbPeopleSource": "herb",
    "HerbResourcesSource": "herb",
    "GmailSource": "gmail",
    "GoogleCalendarSource": "google_calendar",
    "GoogleDocsSource": "google_docs",
    "GoogleDriveSource": "google_drive",
    "GoogleSlidesSource": "google_slides",
    "HubspotSource": "hubspot",
    "IncrementalStubSource": "incremental_stub",
    "IntercomSource": "intercom",
    "JiraSource": "jira",
    "LinearSource": "linear",
    "MondaySource": "monday",
    "NotionSource": "notion",
    "OneDriveSource": "onedrive",
    "OneNoteSource": "onenote",
    "OutlookCalendarSource": "outlook_calendar",
    "OutlookMailSource": "outlook_mail",
    "PipedriveSource": "pipedrive",
    "PowerPointSource": "powerpoint",
    "SalesforceSource": "salesforce",
    "ServiceNowSource": "servicenow",
    "SharePointSource": "sharepoint",
    "SharePoint2019V2Source": "sharepoint2019v2.source",
    "SharePointOnlineSource": "sharepoint_online.source",
    "SharePointOnlineAppSource": "sharepoint_online.source",
    "ShopifySource": "shopify",
    "SlabSource": "slab",
    "SliteSource": "slite",
    "SlackSource": "slack",
    "SnapshotSource": "snapshot",
    "StripeSource": "stripe",
    "StubSource": "stub",
    "TeamsSource": "teams",
    "TimedSource": "timed",
    "TodoistSource": "todoist",
    "TrelloSource": "trello",
    "WisprSource": "wispr",
    "WordSource": "word",
    "ZendeskSource": "zendesk",
    "ZoomSource": "zoom",
    "ZohoCRMSource": "zoho_crm",
}

__all__ = [*_SOURCE_MODULES, "ALL_SOURCES"]


def __getattr__(name: str):
    """Keep the registry and direct imports compatible, loading providers only on demand."""
    if name == "ALL_SOURCES":
        result = [__getattr__(source) for source in _SOURCE_MODULES]
    elif name in _SOURCE_MODULES:
        result = getattr(import_module(f".{_SOURCE_MODULES[name]}", __name__), name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = result
    return result


def __dir__() -> list[str]:
    """Preserve discoverability of lazy public exports."""
    return sorted(set(globals()) | set(__all__))
