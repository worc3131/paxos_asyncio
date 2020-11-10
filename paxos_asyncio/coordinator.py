
import random
from typing import List, Tuple

from . import chaos
from . import message
from . import participants
from . import util

class Coordinator:
    # ANSI colors
    c = (
        "\033[91m",  # red
        "\033[35m",  # magenta
        "\033[34m",  # blue
        "\033[32m",  # green
    )

    def __init__(self):
        self.processors = []
        self.log("created as coordinator")

    def log(self, msg: str) -> None:
        util.do_coord_log(msg)

    def register(self, processor: participants.Processor) -> Tuple[int, str]:
        self.processors.append(processor)
        id = len(self.processors)-1
        return id, self.c[id % len(self.c)]

    def send_letter(self, letter: message.Letter) -> None:
        if chaos.chaos_monkey_should_lose_message():
            self.log('CHAOS MONKEY losing message from'
                          f' {letter.frm} to {letter.to}')
        else:
            self.processors[letter.to].mailbox.append(letter)

    def _get_acceptors(self,
                       exclude: List[participants.Processor] = []
                       ) -> List[participants.Acceptor]:
        return [x for x in self.processors
                     if isinstance(x, participants.Acceptor)
                     and x not in exclude]

    def get_quorum_of_acceptor_ids(
            self,
            exclude: List[participants.Processor] = []) -> List[int]:
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
