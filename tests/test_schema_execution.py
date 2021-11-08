import asyncio
import functools
import ourpipes.framework as ourpipes
from utils.async_functools import async_partial

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

def test_non_async_functions():

    def f(x, y):
        return x+y

    def g(x):
        return x*2

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
        executed = asyncio.run(ourpipes.execute_schema(functions, schema, memory, 'y'))
        assert executed == 6
    except Exception as e:
        assert False, str(e)  

def test_compiling_and_running_schema_with_kwargs():

    def f(x, y):
        return x+y

    def g(x, y):
        return x*y

    try:
        compiled = ourpipes.compile_schema_async(
            name="add_n_multiply", 
            description="Adding numbers followed by multiplication",
            functions={'f': f, 'g': g},
            schema={
                "x": ("f", (("a", "x"), ("b", "y"))),
                "y": ("g", (("x", "x"), ("c", "y"))),
            },
            executor=async_partial(
                ourpipes.execute_schema, 
                extract_key="y",
            ),
        )

        result = asyncio.run(compiled(a=3, b=2, c=2))
        assert result == 10

    except Exception as e:
        assert False, str(e)

def test_compiling_and_running_schema_with_args():

    def f(x, y):
        return x+y

    def g(x, y):
        return x*y

    try:
        compiled = ourpipes.compile_schema_async(
            name="add_n_multiply", 
            description="Adding numbers followed by multiplication",
            functions={'f': f, 'g': g},
            schema={
                "x": ("f", (("a", "x"), ("b", "y"))),
                "y": ("g", (("x", "x"), ("c", "y"))),
            },
            executor=async_partial(
                ourpipes.execute_schema, 
                extract_key="y",
            ),
        )

        result = asyncio.run(compiled(3, 2, 2))
        assert result == 10

    except Exception as e:
        assert False, str(e)

def test_automake_memory_from_schema():

    def f(x, y):
        return x+y

    def g(x, y):
        return x*y

    def h(x, y):
        return x-y

    try:
        functions = {k.__name__: k for k in [f, g, h]}
        schema = {
            'x': ('f', (('a', 'x'), ('b', 'y'))),
            'y': ('g', (('c', 'x'), ('d', 'y'))),
            'z': ('h', (('x', 'x'), ('y', 'y'))),
            'n': ('f', (('z', 'x'), ('z', 'y')))
        }
        memory = ourpipes.automake_memory(schema, args=[1,2,3,4])
        result = asyncio.run(ourpipes.execute_schema(functions, schema, memory, 'n'))
        assert result == -18
    except Exception as e:
        assert False, str(e)

def test_make_schema_from_string_and_run():

    def f(x, y):
        return x+y

    def g(x):
        return x*2

    def h(x, y):
        return x*y

    try:
        fnstr = "(a:x, b:y) -> f:x. (c:x) -> g:y. (x:x, y:y) -> h:z"
        schema = ourpipes.funstr2schema(fnstr)

        # Should be a, b, and c as arguments
        memory = {'a': 1, 'b': 2, 'c': 3}
        result = asyncio.run(ourpipes.execute_schema({k.__name__: k for k in [f, g, h]}, schema, memory, 'z'))

        assert result == 18
    except Exception as e:
        assert False, str(e)

def test_running_schema_with_args_kwargs_function_inputs():

    def f_args(*args):
        return sum(*args)

    def f_kwargs(**kwargs):
        return "".join(kwargs['b'].keys())

    def concat(thesum: int, thekeys: str) -> dict:
        return {thekeys: thesum}

    try:
        functions = ourpipes.fns2dict(f_args, f_kwargs, concat)
        schema = {
            'x': ("f_args", (('a'), )),
            'y': ("f_kwargs", (('b', 'b'), )),
            'z': ("concat", (('x', 'thesum'), ('y', 'thekeys'))),
        }
        memory = {
            'a': [1,2,3,4],
            'b': {'a': 1, 'b': 2, 'c': 3},
        }
        result = asyncio.run(ourpipes.execute_schema(functions, schema, memory, 'z'))
        assert result == {'abc': 10}
    except Exception as e:
        assert False, str(e)

def test_compile_multiple_functions_from_strings_and_run():

    def f(x, y, z):
        return x+y+z

    def g(x, y):
        return x-y

    def h(x, y):
        return x*y

    functions = ourpipes.fns2dict(f,g,h)
    schema_string_memories = [
        ("(a:x, b:y, c:z) -> f:x. (a:x, x:y) -> g:y. (b:x, y:y) -> h:z", {'a': 1, 'b': 2, 'c': 3}, 'z', -10),
        ("(a:x, b:y) -> h:x. (a:x, x:y) -> g:y. (b:x, y:y, x:z) -> f:z", {'a': 1, 'b': 2, 'c': 3}, 'z', 3),
        ("(a:x, b:y) -> h:x", {'a': 1, 'b': 2, 'c': 3}, 'x', 2),
    ]

    try:
        for funstr, memory, final, expected_result in schema_string_memories:
            actual_result = asyncio.run(
                ourpipes.execute_schema(functions, ourpipes.funstr2schema(funstr), memory, final),
            )
            assert expected_result == actual_result
    except Exception as e:
        assert False, str(e)

def test_compile_function_and_use_in_other_schema():

    def f(x, y):
        return x+y

    def g(x, y):
        return x*y

    functions = ourpipes.fns2dict(f, g)
    try:
        add_mul_add = ourpipes.compile_schema_async(
            name="add_mul_add", 
            functions=functions, 
            schema=ourpipes.funstr2schema("(a:x, b:y) -> f:x. (a:x, b:y) -> g:y. (x:x, y:y) -> f:z"), 
            executor=async_partial(
                ourpipes.execute_schema,
                extract_key="z",
            ),
        )

        functions[add_mul_add.__name__] = add_mul_add
        compiled = ourpipes.compile_schema_async(
            name="final_fn", 
            functions=functions, 
            schema=ourpipes.funstr2schema("(a:a, b:b) -> add_mul_add:x. (x:x, c:y) -> g:y"), 
            executor=async_partial(
                ourpipes.execute_schema,
                extract_key="y",
            ),
        )

        result = asyncio.run(compiled(a=1, b=2, c=3))
        assert result == 15
    except Exception as e:
        assert False, str(e)

def test_cut_schema():

    try:
        sub_schema = ourpipes.cut_schema(
            schema=ourpipes.funstr2schema("(a:x, b:y) -> f:x. (x:x) -> g:y. (x:x, y:y) -> h:z"),
            start=2,
        )
        assert len(sub_schema) == 2
        assert all((k in sub_schema for k in ["y", "z"]))
    except Exception as e:
        assert False, str(e)

def test_run_schema_with_insufficient_functions():

    def f(x):
        return x+1

    functions = ourpipes.fns2dict(f)
    schema = ourpipes.funstr2schema("(x:x) -> f:y. (y:y) -> g:z")
    try:
        # Should calculate as long as it can...
        result = asyncio.run(ourpipes.execute_schema(functions, schema, {"x": 2}))
        assert result['y'] == 3
        assert isinstance(result['z'], Exception)
    except Exception as e:
        assert False, str(e)