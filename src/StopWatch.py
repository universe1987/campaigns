from time import time


class StopWatch(object):
    def __init__(self):
        self.clock = time()

    def tic(self, msg):
        print '{} takes {:0.2f} seconds'.format(msg, time() - self.clock)
        self.clock = time()

    def reset(self):
        self.clock = time()
