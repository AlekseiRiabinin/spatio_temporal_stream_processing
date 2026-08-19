# Retry decorator
from functools import wraps
import time


def retry(attempts=3):

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):

            for attempt in range(attempts):
                try:
                    return func(*args, **kwargs)

                except Exception:
                    if attempt == attempts - 1:
                        raise

                    time.sleep(1)

        return wrapper

    return decorator


@retry(attempts=3)
def load_features():
    ...


# production
# +
# fault tolerance
# +
# ETL
