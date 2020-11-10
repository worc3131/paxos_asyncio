
from __future__ import annotations

from abc import ABC
import typing
from typing import Optional

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


class message_handler_decorator:
    def __init__(self, fn):
        self.fn = fn

    def __set_name__(self, owner: Processor, name):
        types = typing.get_type_hints(self.fn)
        owner.handle_message.register(types['msg'], self.fn)
        setattr(owner, name, self.fn)

