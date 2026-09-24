"""Turning a command string into the command object an actor would dispatch.

The scenarios drive spsActor through its own vocabulary rather than by instantiating
Exposure directly, so the keys, the argument parsing and the choice of exposure class are
exercised along with the logic under test.
"""

import opscore.protocols.keys as keys
import opscore.protocols.validation as validation
from opscore.protocols.parser import CommandParser

from .mhs import Cmd

parser = CommandParser()


class UserCmd(Cmd):
    """A user command carrying its parsed form, as the hub delivers it."""

    def __init__(self, cmdStr, parsed):
        Cmd.__init__(self, name=cmdStr)
        self.cmdStr = cmdStr
        self.cmd = parsed

    @property
    def isDone(self):
        return bool(self.says('F') or self.says('f'))

    @property
    def didFail(self):
        return bool(self.says('f'))


class CmdSet(object):
    """A command set bound to a simulated actor, callable with a raw command string."""

    def __init__(self, sim, cmdSetClass):
        self.sim = sim
        self.cmdSet = cmdSetClass(sim)
        keys.CmdKey.addKeys(self.cmdSet.keys)
        # the callback is registered the way an actor registers it, and called with the user
        # command rather than the parsed message, which is what a vocabulary function expects.
        self.validated = [(validation.Cmd(verb, args) >> func, func)
                          for verb, args, func in self.cmdSet.vocab]

    def call(self, cmdStr):
        """Parse cmdStr, hand it to the matching vocabulary function and return the command."""
        parsed = parser.parse(cmdStr)
        cmd = UserCmd(cmdStr, parsed)

        # match types the values in place, so the message the function gets must be the one
        # that was matched; a non-matching entry restores it from its own checkpoint.
        for valCmd, func in self.validated:
            if valCmd.match(parsed):
                func(cmd)
                return cmd

        raise ValueError(f'no vocabulary entry matches {cmdStr!r}')
