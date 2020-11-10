
from __future__ import annotations

from abc import ABC
from typing import Tuple

class ProposalGenerator(ABC):

    def get_proposal(self) -> Tuple[int, int]:
        pass


class IncProposalGenerator(ProposalGenerator):

    def __init__(self, seed: int) -> None:
        self.x = seed

    def get_proposal(self) -> Tuple[int, int]:
        result = (self.x, self.x)
        self.x += 1000
        return result
