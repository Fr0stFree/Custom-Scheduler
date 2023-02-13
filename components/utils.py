EVENTS = {
    'on_scheduler_started': set(),
    'on_scheduler_stopped': set(),
    'on_job_scheduled': set(),
    'on_job_started': set(),
    'on_job_done': set(),
    'on_job_failed': set(),
}


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


def coroutine(func):
    def wrapper(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen
    return wrapper


def register_event_handler(event):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            return func(self, *args, **kwargs)
        EVENTS.setdefault(event, set()).add(wrapper)
        return wrapper
    return decorator


def dispatch(event, *args, **kwargs):
    handlers = EVENTS.get(event)
    if handlers is None:
        raise ValueError(f'Event {event} is not registered')
    for handler in handlers:
        handler(*args, **kwargs)