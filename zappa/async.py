import os
import json
import importlib
import inspect
import boto3

AWS_REGION = os.environ.get('AWS_REGION')
AWS_LAMBDA_FUNCTION_NAME = os.environ.get('AWS_LAMBDA_FUNCTION_NAME')

lambdaclient = boto3.client('lambda')
ASYNC = True #TODO: can this be connected to zappa config somehow?

class AsyncResponse(object):
    def __init__(self, success, response):
        self.success = success
        self.response = response

def import_and_get_task(task_path):
    """
    Given a modular path to a function, import that module
    and return the function.
    """
    module, function = task_path.rsplit('.', 1)
    app_module = importlib.import_module(module)
    app_function = getattr(app_module, function)
    return app_function


def route_task(event):
    """
    Deserialises the message from event passed to zappa.handler.run_function
    imports the function, calls the function with args
    """
    message = event
    func = import_and_get_task(message['task_path'])
    return func(
        *message['args'], **message['kwargs']
    )


def send_async_task(task_path, *args, **kwargs):
    """
    Serialise the func path and arguments and
    send through zappa "command" handler
    """
    message = {
        'command': 'zappa.async.route_task',
        'task_path': task_path,
        'args': args,
        'kwargs': kwargs
    }
    response = lambdaclient.invoke(
        FunctionName=AWS_LAMBDA_FUNCTION_NAME,
        InvocationType='Event', #makes the call async
        Payload=json.dumps(message).encode('utf-8'))
    return AsyncResponse(
        success=response.get('StatusCode', 0) == 202,
        response=response)


def task():
    """
    Async task decorator for a function.
    Serialises and dispatches the task to Lambda
    Zappa routes the message to the same function
    Example:
        @task()
        def my_async_func(*args, **kwargs):
            dosomething()
        my_async_func.delay(*args, **kwargs)
    """
    def _delay(func):
        def _delay_inner(*args, **kwargs):
            module_path = inspect.getmodule(func).__name__
            task_path = '{module_path}.{func_name}'.format(
                module_path=module_path,
                func_name=func.__name__
            )
            if ASYNC and AWS_LAMBDA_FUNCTION_NAME:
                return send_async_task(task_path, *args, **kwargs)
            return func(*args, **kwargs)
        return _delay_inner

    def _wrap(func):
        func.delay = _delay(func)
        return func

    return _wrap
