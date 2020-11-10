
from __future__ import annotations

from abc import ABC
import asyncio
import collections
import functools
import math
import random
import typing
from typing import Deque, Dict, List, Optional, Tuple

from . import message
from . import util
from .process_generator import IncProposalGenerator

FUZZ_MAX_TIME = 1
PROB_FREEZE = 0.01
FREEZE_SCALE = 10
PROB_KILL = 0.01
PROB_MESSAGE_LOSS = 0.01

async def run_paxos() -> None:
    loop = asyncio.get_event_loop()
    coord = Coordinator()
    processors: List[Processor] = []
    prop_args = lambda i: [coord, IncProposalGenerator(i), 50]
    for i in range(4):
        processors.append(Proposer(*prop_args(i)))
    for i in range(4):
        processors.append(Acceptor(coord))
    processors.append(SleepyProposer(*prop_args(99), sleep_for=10))
    processors.append(SleepyProposer(*prop_args(120), sleep_for=20))
    processors.append(Monitor(coord))
    tasks = [p.run() for p in processors]
    with util.db_on_exception():
        await asyncio.gather(*tasks)
    util.do_sys_log('Done!')

async def fuzz() -> None:
    t = random.random() * FUZZ_MAX_TIME
    if random.random() < PROB_FREEZE:
        t *= FREEZE_SCALE
    await asyncio.sleep(t)

def chaos_monkey_should_die():
    return random.random() < PROB_KILL

def chaos_monkey_should_lose_message():
    return random.random() < PROB_MESSAGE_LOSS


class Coordinator:
    # ANSI colors
    c = (
        "\033[91m",  # Red
        "\033[35m",  # Magenta
        "\033[34m",  # Blue
        "\033[32m",  # Green
    )

    def __init__(self):
        self.processors = []
        self.log("created as coordinator")

    def log(self, msg: str) -> None:
        util.do_coord_log(msg)

    def register(self, processor: Processor) -> Tuple[int, str]:
        self.processors.append(processor)
        id = len(self.processors)-1
        return id, self.c[id % len(self.c)]

    def send_letter(self, letter: message.Letter) -> None:
        if chaos_monkey_should_lose_message():
            self.log('CHAOS MONKEY losing message from'
                          f' {letter.frm} to {letter.to}')
        else:
            self.processors[letter.to].mailbox.append(letter)

    def _get_acceptors(self, exclude: List[Processor] = []) -> List[Acceptor]:
        return [x for x in self.processors
                     if isinstance(x, Acceptor)
                     and x not in exclude]

    def get_quorum_of_acceptor_ids(
            self,
            exclude: List[Processor] = []) -> List[int]:
        acceptors = self._get_acceptors(exclude=exclude)
        min_num = int(0.5*len(acceptors)+1)
        max_num = len(acceptors)
        num = int(1*len(acceptors))  # arbitrary
        num = min(max_num, max(min_num, num))
        return [x.id for x in random.sample(acceptors, num)]

    def report_accepted(self):
        acceptors = self._get_acceptors()

        def calc_status(x, item):
            val = getattr(x, item)
            if val is None:
                return ''
            return str(val * (1 if x.alive else -1))
        for item in ['accepted_value', 'accepted_number']:
            status = ','.join(calc_status(x, item) for x in acceptors)
            self.log(item + ' status: ' + status)
        done = acceptors[0].accepted_value is not None
        done = done and util.all_equal(x.accepted_value for x in acceptors)
        done = done or not any(x.alive for x in acceptors)
        return not done


class Processor(ABC):

    def __init__(self, coordinator: Coordinator) -> None:
        self.coordinator = coordinator
        self.id, self.color = coordinator.register(self)
        self.alive = True
        self.mailbox: Deque[message.Message] = collections.deque()
        self.log('created as ' + self.__class__.__name__.lower())

    def log(self, msg: str) -> None:
        util.do_log(self.color, self.id, msg)

    async def send_message_to(self, other: int, msg: message.Message) -> None:
        await fuzz()
        letter = message.Letter(self.id, other, msg)
        self.coordinator.send_letter(letter)
        self.log("sent message to " + str(other) + ": " + str(msg))
        await fuzz()

    async def process_mailbox(self):
        while len(self.mailbox) > 0:
            await fuzz()
            letter = self.mailbox.popleft()
            assert letter.to == self.id
            fn = self.handle_message.dispatch(type(letter.message))
            await fn(self, letter.message, letter.frm)
            self.log(f"recv message from {letter.frm}: {letter.message}")
        await fuzz()

    async def _handle_message_catch(self, msg: message.Message, frm:int) -> None:
        raise Exception("No message handling logic for message type " +
                        msg.__class__.__name__ + " in " +
                        self.__class__.__name__ + " sent by " +
                        str(frm))
    handle_message = functools.singledispatch(_handle_message_catch)

    async def run(self) -> None:
        while self.alive:
            if chaos_monkey_should_die():
                self.log('killed by CHAOS MONKEY')
                self.alive = False
                continue
            await fuzz()
            await self.process_mailbox()
            await fuzz()
            await self.action_loop()

    async def action_loop(self) -> None:
        pass


class Acceptor(Processor):

    def __init__(self, coordinator: Coordinator) -> None:
        super().__init__(coordinator)
        self.highest_number_seen: Optional[int] = None
        self.accepted_number: Optional[int] = None # potentially previously accepted
        self.accepted_value: Optional[int] = None

    @message.message_handler_decorator
    async def _handle_prepare_message(self, msg: message.PrepareMessage, frm: int) -> None:
        number = msg.number
        if self.highest_number_seen is None\
                or self.highest_number_seen < number:
            self.highest_number_seen = number
            response = message.PromiseMessage(number,
                                 self.accepted_number,
                                 self.accepted_value)
            await self.send_message_to(frm, response)
        else:
            # TODO optional: send a denial
            pass

    @message.message_handler_decorator
    async def _handle_accept_message(self, msg: message.AcceptMessage, frm: int) -> None:
        if msg.number == self.highest_number_seen:
            self.log(f'accepted {msg.proposal}')
            self.accepted_number = msg.number
            self.accepted_value = msg.proposal

    async def action_loop(self) -> None:
        await super().action_loop()


class Proposer(Processor):

    def __init__(self,
                 coordinator: Coordinator,
                 proposal_generator: ProposalGenerator,
                 propose_every: int) -> None:
        super().__init__(coordinator)
        self.proposal_generator = proposal_generator
        self.propose_every = propose_every
        self.loop_num = 0
        self.proposal: Optional[int] = None
        self.number: Optional[int] = None
        self.acceptors: Optional[List[int]] = None
        self.promises: Dict[int, message.PromiseMessage] = {}

    @message.message_handler_decorator
    async def _handle_promise_message(self,
                                      msg: message.PromiseMessage,
                                      frm: int) -> None:
        self.promises[frm] = msg
        assert self.acceptors is not None
        if len(self.promises) == math.ceil(0.5 * len(self.acceptors)):
            self.v = max((m.prev_acc_value
                          for m in self.promises.values()
                          if m.prev_acc_value is not None),
                         default=self.proposal)
            assert self.number is not None
            assert self.v is not None
            response = message.AcceptMessage(self.number, self.v)
            for a in self.acceptors:
                await self.send_message_to(a, response)

    async def action_loop(self) -> None:
        await super().action_loop()
        if self.loop_num % self.propose_every == 0:
            await self.generate_proposal()
        self.loop_num += 1

    async def generate_proposal(self) -> None:
        p, n = self.proposal_generator.get_proposal()
        if self.number is not None:
            assert n > self.number  # a rule of paxos
        self.proposal = p
        self.number = n
        self.acceptors = self.coordinator.get_quorum_of_acceptor_ids(exclude=[])
        self.promises = {}
        for a in self.acceptors:
            await self.send_message_to(a, message.PrepareMessage(n))


class OneShotProposer(Proposer):
    def __init__(self,
                 coordinator: Coordinator,
                 proposal_generator: ProposalGenerator) -> None:
        super().__init__(coordinator, proposal_generator, 2**32)


class SleepyProposer(Proposer):
    def __init__(self, *args, sleep_for=20, **kwargs):
        super().__init__(*args, **kwargs)
        self.sleep_for = sleep_for

    async def run(self):
        await asyncio.sleep(self.sleep_for)
        await super().run()


class Learner(Processor):

    def __init__(self, coordinater: Coordinator) -> None:
        super().__init__(coordinater)

    async def action_loop(self) -> None:
        pass

class Monitor(Processor):
    # this processor cheats in order to give a glboal view

    def __init__(self, coordinater: Coordinator) -> None:
        super().__init__(coordinater)
        self.loop_num = 0

    async def run(self) -> None:
        while self.alive:
            await fuzz()
            self.loop_num += 1
            if self.loop_num % 10 == 0:
                self.alive = self.coordinator.report_accepted()
        self.log('monitor killed')

