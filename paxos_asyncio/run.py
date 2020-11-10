from __future__ import annotations

from . import chaos
from .coordinator import Coordinator
from .participants import *
from .process_generator import IncProposalGenerator

def gen(i):
    return IncProposalGenerator(i)

async def run_all(processors):
    tasks = [p.run() for p in processors]
    with util.db_on_exception():
        await asyncio.gather(*tasks)
    util.do_sys_log('Done!')

async def run_paxos0() -> None:
    coord = Coordinator()
    processors = []
    processors.append(OneShotProposer(coord, gen(0)))
    processors.append(Acceptor(coord))
    await run_all(processors)

async def run_paxos1():
    coord = Coordinator()
    processors = []
    processors.append(OneShotProposer(coord, gen(0)))
    for i in range(3):
        processors.append(Acceptor(coord))
    await run_all(processors)

async def run_paxos2():
    coord = Coordinator()
    processors = []
    for i in range(3):
        processors.append(OneShotProposer(coord, gen(i)))
    for i in range(5):
        processors.append(Acceptor(coord))
    processors.append(Monitor(coord))
    await run_all(processors)

async def run_paxos3():
    coord = Coordinator()
    processors = []
    for i in range(3):
        processors.append(Proposer(coord, gen(i), 5))
    for i in range(5):
        processors.append(Acceptor(coord))
    processors.append(Monitor(coord))
    await run_all(processors)

async def run_paxos4() -> None:
    coord = Coordinator()
    processors: List[participants.Processor] = []
    prop_args = lambda i: [coord, gen(i), 10]
    for i in range(3):
        processors.append(Proposer(*prop_args(i)))
    for i in range(5):
        processors.append(Acceptor(coord))
    processors.append(SleepyProposer(*prop_args(99), sleep_for=10))
    processors.append(Monitor(coord))
    await run_all(processors)
