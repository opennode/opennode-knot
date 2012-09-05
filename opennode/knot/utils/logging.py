from opennode.oms.config import get_config


def log(msg, daemon_name):
    import threading
    if get_config().getboolean('debug', 'print_daemon_logs', False):
        print "[%s] (%s) %s" % (daemon_name, threading.current_thread(), msg)
