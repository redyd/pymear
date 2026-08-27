from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CreatePollRequest:
    title: str
    choices: list[str]
    duration: int
    channel_points_voting_enabled: bool = True
    channel_points_per_vote: int = 0
    bits_voting_enabled: bool = False
    bits_per_vote: int = 0

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "choices": self.choices,
            "duration": self.duration,
            "channel_points_voting_enabled": self.channel_points_voting_enabled,
            "channel_points_per_vote": self.channel_points_per_vote,
            "bits_voting_enabled": self.bits_voting_enabled,
            "bits_per_vote": self.bits_per_vote,
        }
