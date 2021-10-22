import asyncio
import functools
import ourpipes.framework as ourpipes

def test_can_run_default_schema_executor_compressed():

    async def f(x,y):
        return x+y

    memory = {
        "a": 1,
        "b": 2,
        "c": 3
    }
    schema = {
        "x": ("f", (("a"), ("b", "y"))),
        "y": ("f", (("x", "x"), ("c", "y"))),
    }

    functions = {
        "f": f,
    }

    compression_name = "gzip_pickle"

    try:
        result = asyncio.run(
            ourpipes.execute_compressed_pipeline(
                functions, 
                compression_name, 
                ourpipes.compress_pipeline(
                    compression_name, 
                    schema, 
                    memory,
                    "y"
                ),
            ),
        )
        assert result == 6
    except Exception as e:
        assert False, str(e)

def test_can_run_default_schema_executor():

    async def f(x,y):
        return x+y

    memory = {
        "a": 1,
        "b": 2,
        "c": 3
    }
    schema = {
        "x": ("f", (("a", "x"), ("b", "y"))),
        "y": ("f", (("x", "x"), ("c", "y"))),
    }
    functions = {
        "f": f,
    }

    try:
        result = asyncio.run(ourpipes.execute_schema(functions, schema, memory, "y"))
        assert result == 6
    except Exception as e:
        assert False, str(e)

def test_run_default_schema_executor_should_fail():

    async def f(x,y):
        raise Exception("cannot compute, sorry...")

    memory = {
        "a": 1,
        "b": 2,
        "c": 3
    }
    schema = {
        "x": ("f", (("a", "x"), ("b", "y"))),
        "y": ("f", (("x", "x"), ("c", "y"))),
    }
    functions = {
        "f": f,
    }

    try:
        result = asyncio.run(ourpipes.execute_schema(functions, schema, memory, "y"))
        assert isinstance(result, Exception)
    except Exception as e:
        assert False, str(e)

def test_custom_function_executor():

    async def custom_executor(*fns, **kwargs):
        return [fn(*ars, **kws) for fn, ars, kws in fns]

    def custom_wrapper(fn_name: str, functions: dict, args, kwargs):
        return functions[fn_name], args, kwargs

    def f(x,y):
        return x+y

    memory = {
        "a": 1,
        "b": 2,
        "c": 3
    }
    schema = {
        "x": ("f", (("a", "x"), ("b", "y"))),
        "y": ("f", (("x", "x"), ("c", "y"))),
    }
    functions = {
        "f": f,
    }

    result = asyncio.run(ourpipes.execute_schema(functions, schema, memory, "y", paralell_function_executor=custom_executor, sequential_function_wrapper=custom_wrapper))
    assert result == 6

def test_run_schema_with_key_error_should_fail():

    async def f(x,y):
        return x+y

    memory = {
        "a": 1,
        "b": 2,
        "c": 3
    }
    schema = {
        "x": ("f", (("a", "y"), ("b", "y"))),
    }
    functions = {
        "f": f,
    }

    try:
        result = asyncio.run(ourpipes.execute_schema(functions, schema, memory))
    except Exception as e:
        assert True, str(e)    

def test_run_schema_executor_in_schema():

    async def f(x, y):
        return x+y

    async def g(x, y):
        return {x: y}

    async def h(x, y):
        return {**x, **y}

    async def extract(key: str, from_dict: dict):
        return from_dict[key]

    atomic_functions = {
        "f": f,
        "g": g,
        "h": h,
        "extract": extract,
    }
    async def wrap(schema, memory, key):
        return await ourpipes.execute_schema(atomic_functions, schema, memory, key)

    functions = {
        **atomic_functions,
        **{
            "e": wrap
        }
    }

    memory = {
        "a": 1,
        "b": 2,
        "c": {
            "a": 1
        },
        "d": {
            "f": ("f", (("a", "x"), ("x", "y"))),
        },
        "e": "x",
        "key": "f"
    }
    schema = {
        "x": ("f", (("a", "x"), ("b", "y"))),
        
        "g": ("g", (("e", "x"), ("x", "y"))),
        "h": ("h", (("c", "x"), ("g", "y"))),
        
        "y": ("e", (("d", "schema"), ("h", "memory"), ("key", "key"))),
    }

    result = asyncio.run(ourpipes.execute_schema(functions, schema, memory, "y"))
    assert result == 4

def test_run_schema_as_far_as_possible():

    async def f(x,y):
        return x+y

    memory = {
        "a": 1,
        "b": 2
    }
    schema = {
        "x": ("f", (("a", "x"), ("b", "y"))),
        "y": ("g", (("x", "x"),)),
    }
    functions = {
        "f": f,
    }

    try:
        executed = asyncio.run(ourpipes.execute_schema(functions, schema, memory))
        new_schema, new_memory = ourpipes.fast_forward_schema(schema, executed)
        assert "y" in new_schema and "x" not in new_schema
        assert "x" in new_memory and "a" not in new_memory and not "b" in new_memory

        # compute new scheme and memory when now g is available
        async def g(x):
            return x +1

        new_executed = asyncio.run(
            ourpipes.execute_schema({"g": g}, new_schema, new_memory)
        )
        new_new_schema, new_new_memory = ourpipes.fast_forward_schema(new_schema, new_memory)
        assert new_executed['y'] == 4
        assert new_new_schema == {}
        assert new_new_memory == {}
    except Exception as e:
        assert False, str(e)    

def test_run_schema_as_far_as_possible_while_can_be_completed():

    async def f(x,y):
        return x+y

    async def g(x):
        return x +1

    memory = {
        "a": 1,
        "b": 2
    }
    schema = {
        "x": ("f", (("a", "x"), ("b", "y"))),
        "y": ("g", (("x", "x"),)),
    }
    functions = {
        "f": f,
        "g": g,
    }

    try:
        executed = asyncio.run(ourpipes.execute_schema(functions, schema, memory))
        new_schema, new_memory = ourpipes.fast_forward_schema(schema, memory)
        assert executed["y"] == 4
    except Exception as e:
        assert False, str(e)    