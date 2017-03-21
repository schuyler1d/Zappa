import os
import json
import importlib
import inspect
import boto3

AWS_REGION = os.environ.get('AWS_REGION')
AWS_LAMBDA_FUNCTION_NAME = os.environ.get('AWS_LAMBDA_FUNCTION_NAME')

lambdaclient = boto3.client('lambda')
ASYNC = True #can this be connected to zappa config somehow?

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
    Gets SNS Message, deserialises the message,
    imports the function, calls the function with args
    """
    # record = event['Records'][0]
    # message = json.loads(
    #     record['Sns']['Message']
    # )
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
    if response.get('StatusCode', 0) == 202:
        return True


def task():
    """
    Async task decorator for a function.
    Serialises and dispatches the task to SNS.
    Lambda subscribes to SNS topic and gets this message
    Lambda routes the message to the same function
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
