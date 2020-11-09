
from __future__ import annotations

from abc import ABC
import asyncio
import random
import time
from typing import List, Tuple

FUZZ_MAX_TIME = 1
PROB_KILL = 0.01

async def fuzz() -> None:
    await asyncio.sleep(random.random() * FUZZ_MAX_TIME)

def should_chaos_monkey_die():
    return random.random() < PROB_KILL

def log_message(color: str, id: int, msg : str) -> None:
    print(color, f'[{id}] {msg}')

class Coordinator:
    # ANSI colors
    c = (
        "\033[0m",  # End of color
        "\033[36m",  # Cyan
        "\033[91m",  # Red
        "\033[35m",  # Magenta
    )

    def __init__(self):
        self.processors = []

    def register(self, obj: Processor) -> Tuple[int, str]:
        self.processors.append(obj)
        id = len(self.processors)-1
        return id, self.c[id % len(self.c)]

    def get_quorum_of_acceptors(self):
        acceptors = [x for x in self.processors if isinstance(x, Acceptor)]
        # for now just return all
        return acceptors


class ProposalGenerator(ABC):

    def get_proposal(self) -> Tuple[int, int]:
        pass


class IncProposalGenerator(ProposalGenerator):

    def __init__(self, seed: int) -> None:
        self.x = seed

    def get_proposal(self) -> Tuple[int, int]:
        result = (self.x, self.x)
        self.x += 1
        return result


class Processor(ABC):

    def __init__(self, coordinator: Coordinator) -> None:
        self.coordinator = coordinator
        self.id, self.color = coordinator.register(self)
        self.alive = True
        self.log('created as an ' + self.__class__.__name__.lower())

    def log(self, msg: str) -> None:
        log_message(self.color, self.id, msg)

    async def run(self) -> None:
        while self.alive:
            if should_chaos_monkey_die():
                self.log('killed by chaos monkey')
                self.alive = False
                continue
            await fuzz()
            self.action_loop()

    def action_loop(self) -> None:
        raise NotImplementedError


class Acceptor(Processor):

    def __init__(self, coordinator: Coordinator) -> None:
        super().__init__(coordinator)

    def action_loop(self):
        pass


class Proposer(Acceptor):

    def __init__(self,
                 coordinator: Coordinator,
                 proposal_generator: ProposalGenerator) -> None:
        super().__init__(coordinator)
        self.proposal_generator = proposal_generator
        self.proposal = None
        self.number = None
        self.acceptors = None
        self.i_think_im_leader = False

    @property
    def i_think_im_leader(self) -> bool:
        return self._i_think_im_leader

    @i_think_im_leader.setter
    def i_think_im_leader(self, x: bool) -> None:
        self._i_think_im_leader = x

    def action_loop(self) -> None:
        super().action_loop()
        if self.proposal is None:
            self.generate_proposal()

    def generate_proposal(self):
        p, n = self.proposal_generator.get_proposal()
        if self.number is not None:
            assert n > self.number  # a rule of paxos
        self.proposal = p
        self.number = n
        self.coordinator.get_quorum_of_acceptors()


class Learner(Processor):

    def __init__(self, coordinater: Coordinator) -> None:
        super().__init__(coordinater)

    def action_loop(self) -> None:
        pass


async def run_paxos() -> None:
    loop = asyncio.get_event_loop()
    coord = Coordinator()
    processors: List[Acceptor] = []
    for i in range(1):
        mess_gen = IncProposalGenerator(i)
        processors.append(Proposer(coord, mess_gen))
    for i in range(3):
        processors.append(Acceptor(coord))
    tasks = [loop.create_task(p.run()) for p in processors]
    for task in tasks:
        await task
    print('Done!')
