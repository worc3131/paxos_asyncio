
from __future__ import annotations

from abc import ABC
import asyncio
import math
from typing import Dict, List, Optional

from . import chaos
from . import message
from . import util

class Processor(ABC, message.MessageMixin):

    def __init__(self, coordinator: Coordinator) -> None:
        super().__init__()
        self.coordinator = coordinator
        self.id, self.color = coordinator.register(self)
        self.alive = True
        self.log('created as ' + self.__class__.__name__.lower())

    def log(self, msg: str) -> None:
        util.do_log(self.color, self.id, msg)

    async def run(self) -> None:
        while self.alive:
            if chaos.chaos_monkey_should_die():
                self.log('killed by CHAOS MONKEY')
                self.alive = False
                continue
            await chaos.fuzz()
            await self.process_mailbox()
            await chaos.fuzz()
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
    # TODO: implement

    def __init__(self, coordinater: Coordinator) -> None:
        super().__init__(coordinater)
        raise NotImplementedError

    async def action_loop(self) -> None:
        pass

class Monitor(Processor):
    # this processor cheats in order to give a glboal view

    def __init__(self, coordinater: Coordinator) -> None:
        super().__init__(coordinater)
        self.loop_num = 0

    async def run(self) -> None:
        while self.alive:
            await chaos.fuzz()
            self.loop_num += 1
            if self.loop_num % 10 == 0:
                self.alive = self.coordinator.report_accepted()
        self.log('monitor killed')

