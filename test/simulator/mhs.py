"""Stand-ins for the MHS plumbing an Exposure talks through.

A command object, a cmdVar and a keyVar, reduced to what spsActor actually touches: enough
for the real Exposure, SpecModuleExposure and LampsControl objects to run unmodified.
"""

import threading

from opscore.protocols.messages import Keyword, Keywords


class Reply(object):
    """A single reply line, carrying typed keywords."""

    def __init__(self, keywords):
        self.keywords = Keywords([Keyword(name, values) for name, values in keywords])


class CmdVar(object):
    """The result of a command.

    Parameters
    ----------
    didFail : `bool`
        whether the command failed.
    keywords : list of (`str`, `list`)
        keyword name and values carried by the reply.
    timeLim : `float`
        seconds the caller allowed, reported back by interpretFailure.
    """

    def __init__(self, didFail=False, keywords=(), timeLim=60):
        self.didFail = didFail
        self.timeLim = timeLim
        self.replyList = [Reply(keywords)]

    @classmethod
    def failure(cls, reason, timeLim=60):
        """Return a failed cmdVar carrying reason as text, as a real actor would."""
        return cls(didFail=True, keywords=[('text', [reason])], timeLim=timeLim)


class Cmd(object):
    """A user command, recording what was said rather than sending it."""

    def __init__(self, name='cmd'):
        self.name = name
        self.replies = []
        self.lock = threading.Lock()

    def __str__(self):
        return f'Cmd({self.name})'

    def says(self, level=None):
        """Return what was replied, optionally for a single level."""
        return [text for code, text in self.replies if level is None or code == level]

    def _reply(self, code, text):
        with self.lock:
            self.replies.append((code, str(text)))

    def debug(self, text):
        self._reply('d', text)

    def diag(self, text):
        self._reply('d', text)

    def inform(self, text):
        self._reply('i', text)

    def warn(self, text):
        self._reply('w', text)

    def fail(self, text):
        self._reply('f', text)

    def finish(self, text=''):
        self._reply('F', text)


class KeyVar(object):
    """A keyword variable, firing its callbacks whenever it is given a new value."""

    def __init__(self, name, value=None):
        self.name = name
        self.value = value
        self.callbacks = []

    def getValue(self, doRaise=True):
        if self.value is None and doRaise:
            raise ValueError(f'{self.name} has no value')

        return self.value

    def addCallback(self, callback, callNow=False):
        self.callbacks.append(callback)

        if callNow:
            callback(self)

    def removeCallback(self, callback):
        self.callbacks.remove(callback)

    def set(self, value):
        """Set a new value and fire the callbacks, as the reactor thread would."""
        self.value = value

        for callback in list(self.callbacks):
            callback(self)


class Model(object):
    """An actor model, namely its keyVarDict."""

    def __init__(self, keys=()):
        self.keyVarDict = dict([(name, KeyVar(name)) for name in keys])

    def declare(self, name, value=None):
        """Declare a keyVar, leaving an already declared one alone."""
        if name not in self.keyVarDict:
            self.keyVarDict[name] = KeyVar(name, value)

        return self.keyVarDict[name]
