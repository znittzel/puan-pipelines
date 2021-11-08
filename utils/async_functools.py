import asyncio
import functools
from typing import Callable

def async_partial(fn: Callable, *args, **kwargs):

    """
        Wraps functools `partial` into a async function.

        Return:
            coro Callable
    """
    partial_fn = functools.partial(fn, *args, **kwargs)
    async def runner(*inner_args, **inner_kwargs):
        return await partial_fn(*inner_args, **inner_kwargs)

    return runner