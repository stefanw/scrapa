try:
    from IPython.core.debugger import Tracer
    debug_here = Tracer()

    def excepthook(type, value, traceback):
        print('test')
        return debug_here()

except ImportError:
    excepthook = None
