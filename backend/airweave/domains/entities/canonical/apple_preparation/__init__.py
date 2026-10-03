"""Offline, original-preserving Apple source body preparation."""

from .adapter import prepare_message_body, prepare_notes_body
from .models import ApplePreparationError, PreparationLimits, PreparedMessageBody, PreparedNotesBody

__all__ = [
    "prepare_message_body",
    "prepare_notes_body",
    "ApplePreparationError",
    "PreparationLimits",
    "PreparedMessageBody",
    "PreparedNotesBody",
]
