
from __future__ import annotations

from abc import ABC
import asyncio
import collections
import functools
import random
from typing import Deque, List, Optional, Tuple

FUZZ_MAX_TIME = 0.5
PROB_KILL = 0.01
PROB_MESSAGE_LOSS = 0.01

async def fuzz() -> None:
    await asyncio.sleep(random.random() * FUZZ_MAX_TIME)

def should_chaos_monkey_die():
    return random.random() < PROB_KILL

def should_chaos_monkey_lose_message():
    return random.random() < PROB_MESSAGE_LOSS

def do_log(color: str, id: int, msg : str) -> None:
    print(color, f'[{id}] {msg}')

def do_system_log(msg):
    print("\033[0m", f'[SYS] {msg}')

class Coordinator:
    # ANSI colors
    c = (
        "\033[0m",   # Plain
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

    def send_letter(self, letter: Letter) -> None:
        if should_chaos_monkey_lose_message():
            do_system_log('CHAOS MONKEY losing message from'
                          f' {letter.frm} to {letter.to}')
        else:
            self.processors[letter.to].mailbox.append(letter)

    def get_quorum_of_acceptors(self, exclude) -> List[int]:
        acceptors = [x.id for x in self.processors
                     if isinstance(x, Acceptor)
                     and x not in exclude]
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


class Message(ABC):
    pass

    def __str__(self):
        raise NotImplementedError


class Letter:
    def __init__(self, frm: int, to: int, message: Message) -> None:
        self.frm = frm
        self.to = to
        self.message = message


class PrepareMessage(Message):
    def __init__(self, number: int) -> None:
        self.number = number

    def __str__(self):
        return 'PREPARE N=' + str(self.number)


class PromiseMessage(Message):
    def __init__(self, number: int,
                 prev_acc_number: Optional[int],
                 prev_acc_value: Optional[int]) -> None:
        self.number = number
        self.prev_acc_number = prev_acc_number
        self.prev_acc_value = prev_acc_value

    def __str__(self):
        return f'PROMISE N={self.number},' \
               f' PREV_ACC_N={self.prev_acc_number}' \
               f' PREV_ACC_VAL={self.prev_acc_value}'

class Processor(ABC):

    def __init__(self, coordinator: Coordinator) -> None:
        self.coordinator = coordinator
        self.id, self.color = coordinator.register(self)
        self.alive = True
        self.mailbox: Deque[Message] = collections.deque()
        self.handle_message = functools.singledispatch(self._handle_message_catch)
        self.log('created as an ' + self.__class__.__name__.lower())

    async def send_message_to(self, other: int, message: Message) -> None:
        await fuzz()
        letter = Letter(self.id, other, message)
        self.coordinator.send_letter(letter)
        self.log("sent message to " + str(other) + ": " + str(message))

    async def process_mailbox(self):
        while len(self.mailbox) > 0:
            letter = self.mailbox.popleft()
            assert letter.to == self.id
            await fuzz()
            await self.handle_message(letter.message, letter.frm)
            self.log(f"recv message from {letter.frm}: {letter.message}")
            await fuzz()

    async def _handle_message_catch(self, msg: Message, frm:int) -> None:
        raise Exception("No message handling logic for message type " +
                        msg.__class__.__name__ + " in " +
                        self.__class__.__name__ + " sent by " +
                        str(frm))

    def log(self, msg: str) -> None:
        do_log(self.color, self.id, msg)

    async def run(self) -> None:
        while self.alive:
            if should_chaos_monkey_die():
                self.log('killed by CHAOS MONKEY')
                self.alive = False
                continue
            await self.process_mailbox()
            await fuzz()
            await self.action_loop()

    async def action_loop(self) -> None:
        raise NotImplementedError


class Acceptor(Processor):

    def __init__(self, coordinator: Coordinator) -> None:
        super().__init__(coordinator)
        self.highest_number_seen: Optional[int] = None
        self.prev_acc_number: Optional[int] = None
        self.prev_acc_value: Optional[int] = None
        self.handle_message.register(PrepareMessage, self._handle_prepare_message)

    async def _handle_prepare_message(self, msg: PrepareMessage, frm: int) -> None:
        number = msg.number
        if self.highest_number_seen is None\
                or self.highest_number_seen < number:
            self.highest_number_seen = number
            response = PromiseMessage(number,
                                 self.prev_acc_number,
                                 self.prev_acc_value)
            await self.send_message_to(frm, response)
        else:
            # TODO optional: send a denial
            pass

    async def action_loop(self) -> None:
        pass


class Proposer(Acceptor):

    def __init__(self,
                 coordinator: Coordinator,
                 proposal_generator: ProposalGenerator) -> None:
        super().__init__(coordinator)
        self.proposal_generator = proposal_generator
        self.proposal: Optional[int] = None
        self.number: Optional[int] = None
        self.acceptors: Optional[List[int]] = None
        self.i_think_im_leader = False
        self.handle_message.register(PromiseMessage, self._handle_promise_message)

    @property
    def i_think_im_leader(self) -> bool:
        return self._i_think_im_leader

    @i_think_im_leader.setter
    def i_think_im_leader(self, x: bool) -> None:
        self._i_think_im_leader = x

    async def _handle_promise_message(self, msg:PromiseMessage, frm:int):
        pass

    async def action_loop(self) -> None:
        await super().action_loop()
        if self.proposal is None:
            await self.generate_proposal()

    async def generate_proposal(self):
        p, n = self.proposal_generator.get_proposal()
        if self.number is not None:
            assert n > self.number  # a rule of paxos
        self.proposal = p
        self.number = n
        self.acceptors = self.coordinator.get_quorum_of_acceptors(exclude=[self])
        for a in self.acceptors:
            await self.send_message_to(a, PrepareMessage(n))


class Learner(Processor):

    def __init__(self, coordinater: Coordinator) -> None:
        super().__init__(coordinater)

    async def action_loop(self) -> None:
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
    tasks = [p.run() for p in processors]
    await asyncio.gather(*tasks)
    do_system_log('Done!')
