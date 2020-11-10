
from __future__ import annotations

from abc import ABC
import collections
import functools
import typing
from typing import Deque, Optional

from . import chaos

class MessageMixin:

    def __init__(self) -> None:
        self.mailbox: Deque[Message] = collections.deque()

    async def send_message_to(self, other: int, msg: Message) -> None:
        await chaos.fuzz()
        letter = Letter(self.id, other, msg)
        self.coordinator.send_letter(letter)
        self.log("sent message to " + str(other) + ": " + str(msg))
        await chaos.fuzz()

    async def process_mailbox(self):
        while len(self.mailbox) > 0:
            await chaos.fuzz()
            letter = self.mailbox.popleft()
            assert letter.to == self.id
            fn = self.handle_message.dispatch(type(letter.message))
            await fn(self, letter.message, letter.frm)
            self.log(f"recv message from {letter.frm}: {letter.message}")
        await chaos.fuzz()

    async def _handle_message_catch(self, msg: Message, frm:int) -> None:
        raise Exception("No message handling logic for message type " +
                        msg.__class__.__name__ + " in " +
                        self.__class__.__name__ + " sent by " +
                        str(frm))
    handle_message = functools.singledispatch(_handle_message_catch)


class message_handler_decorator:
    def __init__(self, fn):
        self.fn = fn

    def __set_name__(self, owner: Processor, name):
        types = typing.get_type_hints(self.fn)
        owner.handle_message.register(types['msg'], self.fn)
        setattr(owner, name, self.fn)


class Message(ABC):

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

    def __str__(self) -> str:
        return 'PREPARE N=' + str(self.number)


class PromiseMessage(Message):
    def __init__(self, number: int,
                 prev_acc_number: Optional[int],
                 prev_acc_value: Optional[int]) -> None:
        self.number = number
        self.prev_acc_number = prev_acc_number
        self.prev_acc_value = prev_acc_value

    def __str__(self) -> str:
        return f'PROMISE N={self.number},' \
               f' PREV_ACC_N={self.prev_acc_number},' \
               f' PREV_ACC_VAL={self.prev_acc_value}'


class AcceptMessage(Message):
    def __init__(self, number: int, proposal: int) -> None:
        self.number = number
        self.proposal = proposal

    def __str__(self) -> str:
        return f'ACCEPT N={self.number},' \
               f' PROPOSAL={self.proposal}'

