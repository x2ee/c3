from threading import local

ctx = local()

def cache(fn, duration="1w", action_on_expire="purge"):
    pass

