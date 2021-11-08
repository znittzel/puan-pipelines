
import functools
import asyncio
import inspect
import itertools
import gzip
import pickle

from toposort import toposort
from typing import Callable, Tuple, Any

def schema2execution_order(schema: dict) -> iter:
    return toposort(
        {
            key: set(
                [
                    y[0] 
                    for y in fn[1]
                ]
            ) 
            for key, fn in schema.items()
        }
    )

def default_sequential_function_wrapper(fn_name: str, functions: dict, args, kwargs):
    """
        Let's check f and compute.

        Return:
            Any
    """
    async def wrap_fn(fn_name: str, functions: dict, *args, **kwargs):
        if not fn_name in functions:
            return Exception(f"function '{fn_name}' does not exists")

        fn = functions[fn_name]
        if inspect.iscoroutinefunction(fn):
            return await fn(*args, **kwargs)
        else:
            return fn(*args, **kwargs)

    return asyncio.create_task(wrap_fn(fn_name, functions, *args, **kwargs))

async def default_paralell_function_executor(*fns, **kwargs):

    """
        Uses asyncio to compute a list of functions.

        Return:
            List of function results
    """

    return await asyncio.gather(*fns, **kwargs)

async def asyncfn_executor_wrapper(fn, *args, **kwargs):

    """
        Checks if fn is a coroutine function, and if so
        awaits it. Else function is computed normally.

        Return:
            Function result
    """

    return await fn(*args, **kwargs) if inspect.iscoroutinefunction(fn) else fn(*args, **kwargs)

async def execute_schema(
    functions: dict,
    schema: dict, 
    memory: dict, 
    extract_key: str = None,

    paralell_function_executor: Callable = default_paralell_function_executor, 
    sequential_function_wrapper: Callable = default_sequential_function_wrapper,
) -> Tuple[str, dict]:

    """
        Executes functions in schema in topological sort order. If there is
        no such order, an exception is raised. Currently, the function does
        not type check so user needs to be careful when using. 

        Here's an example:

        functions:
            f : x, y -> x + y
            g : x, y -> x - y
            h : x, y -> x ^ y

        memory = {
            "a": 1,
            "b": 2,
            "c": 3
        }
        func_schema = {
            # output:, (func, ((memory_var, func_param_var), ...))
            "x": (f, (("a", "x"), ("b", "y"))),
            "y": (g, (("a", "x"), ("c", "y"))),
            "z": (h, (("x", "x"), ("y", "y"))),
        }

        Running this executor with given functions and inputs will result in {"z": 27}. 
        NOTE that the output variable name and function parameter variable must collaborate. 
        The result from a function will be saved in memory by the variable name of output 
        variable and may later be looked up by another function.

        We will return a tuple of (status, result), where the status is either "success" or
        "fail". When fail, the exception from underlying functions will be forward. Result
        is the latest function step computed.

        Return:
            Tuple[str, dict]
    """

    def select_function(fn_name: str, functions: dict) -> Callable:

        """
            Selecting function wrapper needed for when there's a
            key error in functions, just to append more info into
            the exception
        """
        if not fn_name in functions:
            return Exception(f"service doesn't offer function '{fn_name}'")

        return functions[fn_name]

    execution_order = schema2execution_order(schema)
    for i, fn_pars in enumerate(execution_order):

        # Update memory
        memory.update(
            {
                variable: result
                for variable, result in zip(
                    filter(
                        lambda variable: not variable in memory,
                        fn_pars
                    ),
                    await paralell_function_executor(
                        *map(
                            lambda variable: sequential_function_wrapper(
                                fn_name=schema[variable][0], 
                                functions=functions,
                                args=(
                                    memory[var]
                                    for var in filter(
                                        lambda x: len(x) == 1,
                                        schema[variable][1]
                                    )
                                ),
                                kwargs={
                                    param: memory[var] 
                                    for var, param in filter(
                                        lambda x: len(x) == 2,
                                        schema[variable][1]
                                    )
                                },
                            ),
                            filter(
                                lambda variable: not variable in memory,
                                fn_pars
                            ),
                        ),
                        return_exceptions=True,
                    )
                )
            }
        )

    return memory[extract_key] if extract_key in memory else memory

def fast_forward_schema(
    schema: dict, 
    memory: dict, 
):
    """
        Will analyze scheme how far possible it can be forward, 
        which can later be executed. This can be necessary when 
        e.g this service doesn't offer all functions that scheme requires.
        
        Return:
            Tuple[memory, scheme]
    """   
    # Keep schema points if they either are not in memory
    # or if the memory spot is an Exception
    new_schema = {
        k: v 
        for k,v in schema.items() 
        if not k in memory or isinstance(memory[k], Exception)
    }

    new_memory = {
        k: v
        for k, v in memory.items()
        if any(
            itertools.chain(
                (
                    any(k == arg[0] for arg in args) 
                    for _, args in new_schema.values()
                ),
            ),
        )
    }

    return new_schema, new_memory


def default_decompression(compression_name: str, data):

    """
        Decompression mapping, maps compression string name
        to different decompression functions.

        Return:
            Any
    """

    return {
        'gzip':         gzip.decompress,
        'gzip_pickle':  lambda x: pickle.loads(gzip.decompress(x)),
    }.get(compression_name, 'gzip_pickle')(data)

def default_compression(compression_name: str, data):

    """
        Compression mapping, maps compression string name
        to different compression functions.

        Return:
            byte-string
    """
    return {
        'gzip':         gzip.compress,
        'gzip_pickle':  lambda x: gzip.compress(pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)),
    }.get(compression_name, 'gzip_pickle')(data)

async def execute_compressed_pipeline(
    functions: dict, 
    decompression_name: str, 
    payload: str, 
    decompress_map: Callable = default_decompression, 
    pipeline_executor: Callable = execute_schema,
):

    """
        execute_compressed_pipeline is a pipeline executor wrapper
        that expects a compressed pipeline in payload, defined how in header,
        and computes the pipeline.

        Return:
            Any
    """

    return await pipeline_executor(
        functions=functions,
        **decompress_map(
            decompression_name,
            payload, 
        ),
    )    

def compress_pipeline(compression_name: str, schema: dict, memory: dict, extract_key: str = None, compression_map: Callable = default_compression):

    """
        Compress pipeline into a bytes object.

        Return:
            byte-string
    """

    return compression_map(
        compression_name, 
        {
            'schema': schema, 
            'memory': memory,
            'extract_key': extract_key,
        },
    )

def schema2arguments(schema: dict) -> list:

    """
        Variables that are defined as function inputs,
        but are not declared as expecting from a function
        output, is called an argument.

        Return:
            list
    """

    return list({
        (i,j): input_name
        for i, (output, fn_params) in enumerate(schema.items())
        for j, (input_name, _) in enumerate(fn_params[1])
        if not input_name in schema
    }.values())

def schema2signature(schema: dict, functions: dict) -> tuple:

    """
        Returns some kind of artificial function
        signature from schema. The tuple consists
        of input argument names and return types.

        Return:
            Tuple[List[str], List[str]]
    """
    
    # Find all inputs that does not exists as a function
    # in schema
    arguments = schema2arguments(schema)
    execution_order = list(list(schema2execution_order(schema))[-1])
    return_types = [
        str(inspect.signature(functions[schema[output_name][0]]).return_annotation)
        for output_name in execution_order
    ]

    return (arguments, return_types)

def automake_memory(schema: dict, args: list = [], kwargs: dict = {}) -> dict:

    """
        Will generate schema memory input based on positional and keyword arguments.
        NOTE kwargs are in higher order than args and therefore preferred.

        Return:
            dict
    """
    arguments = schema2arguments(schema)
    for i, arg in enumerate(filter(lambda arg: arg not in kwargs, arguments)):
        if len(args) > i:
            kwargs[arg] = args[i]

    return kwargs

def compile_schema_async(
    name: str, 
    functions: dict, 
    schema: dict, 
    executor: Callable,
    description: str = "",
) -> Callable:

    """
        Compiling schema, functions and an executor into a virtual async function with name `name`.

        Return:
            Callable[dict]
    """

    def async_compiler(functions: dict, schema: dict, executor: Callable) -> Callable:

        async def wrap_executor(*args, **kwargs) -> Any:
            return await executor(
                functions=functions, 
                schema=schema, 
                memory=automake_memory(
                    schema=schema, 
                    args=args, 
                    kwargs=kwargs,
                ),
            )
        
        wrap_executor.__name__ = name
        wrap_executor.__qualname__ = name
        wrap_executor.__doc__ = description
        return wrap_executor

    return async_compiler(functions, schema, executor)

def funstr2schema(funstr: str) -> dict:

    """
        A `funstr` is a functional-string, containing information
        of a schema in one string, written in a certain way.

        Explinations: 
            Let funstr = (a:x, b:y) -> f:x. (c:x) -> g. (f:x, g:y) -> h:y 
            (:x, :y) -> f mening that f is expected to have x, y as keyword
            input. (a:x, b:y) mening variable a maps to x, and variable b to y.
            (:, :) -> f meaning that f is expected to have two positional arg inputs.
            f:x meaning that result from ... -> f will be stored in variable x.
            Dot (.) meaning end of statement.
    """

    try:
        schema = {}
        statements = funstr.replace(" ", "").split(".")
        for statement in statements:
            arguments_string, function_string = statement.split("->")
            poskey_arguments = arguments_string.replace("(", "").replace(")", "").split(",")
            arguments = [tuple(arg.split(':')) for arg in poskey_arguments]
            fn_name, outvar = function_string.split(":")

            schema[outvar] = (fn_name, arguments)
        
        return schema
    except Exception as e:
        raise Exception(f"could not create schema from function-string '{funstr}' because of error: {e}")

def fns2dict(*functions) -> dict:

    """
        Returns a dictionary of function name -> function,
        given functions as *arguments.

        Return:
            Dict[str, Callable]
    """

    return {f.__name__: f for f in functions}

def cut_schema(schema: dict, start: int = 0, stop: int = None) -> dict:

    """
        Cut schema from point `start` to point `stop`.
        Starts default is 0, and stops default is at end.

        Return:
            dict (schema)
    """
    
    schema_order = list(schema2execution_order(schema))
    stop = stop if stop is not None else len(schema_order)

    schema_order_cut = schema_order[start:stop]
    schema_cut = {
        k: v
        for k, v in schema.items()
        if any((k in fns for fns in schema_order_cut))
    }
    return schema_cut
