
from __future__ import annotations

from typing import Iterable

def do_log(color: str, id: int, msg : str) -> None:
    print(color, f'[{id}] {msg}')

def do_coord_log(msg):
    print("\033[0m", f'[CO] {msg}')

def do_sys_log(msg):
    print("\033[0m", f'[SYS] {msg}')

def all_equal(x: Iterable):
    x = iter(x)
    try:
        first = next(x)
        while True:
            val = next(x)
            if first != val:
                return False
    except StopIteration:
        return True

class db_on_exception:

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            import code, traceback
            traceback.print_exc()
            frame = self._get_last_frame(exc_tb)
            namespace = dict(frame.f_globals)
            namespace.update(frame.f_locals)
            if 'exit' not in namespace:
                def exit():
                    raise SystemExit
                namespace['exit'] = exit
            try:
                code.interact(local=namespace)
            except SystemExit:
                pass

    @staticmethod
    def _get_last_frame(tb):
        while tb.tb_next:
            tb = tb.tb_next
        return tb.tb_frame
