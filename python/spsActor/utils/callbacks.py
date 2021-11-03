import numpy as np


class MetaStatus(object):
    invalid = -1
    """ handle spectrograph module statuses as one """

    def __init__(self, spsActor):
        self.cbs = []
        self.spsActor = spsActor

    @property
    def spsModules(self):
        return list(self.spsActor.spsConfig.spsModules.values())

    def attachCallbacks(self):
        """ Attach all status callbacks, but clear the old ones first."""
        self.clearCallbacks()

        enuCallbacks = [('biaStatus', self.biaStatus)]

        for specModule in self.spsModules:
            for key, cb in enuCallbacks:
                kv = self.spsActor.models[specModule.enuName].keyVarDict[key]
                kv.addCallback(cb)
                self.cbs.append((kv, cb))

    def clearCallbacks(self):
        """ Clear existing status callback."""
        for keyvar, cb in self.cbs:
            keyvar.removeCallback(cb)

        self.cbs.clear()

    def biaStatus(self, *args, **kwargs):
        """ summarize sm1-4 biaStatus into a single biaStatus keyword."""
        array = []

        for specModule in self.spsModules:
            try:
                biaStatus = np.array(self.spsActor.models[specModule.enuName].keyVarDict['biaStatus'].getValue(),
                                     dtype=int)
            except ValueError:
                biaStatus = np.array(5 * [MetaStatus.invalid], dtype=int)

            array.append(biaStatus)

        array = np.array(array)
        values = dict()

        for i, key in enumerate(['power', 'period', 'duty', 'pulseOn', 'pulseOff']):
            try:
                [value] = np.unique(array[:, i])
            except ValueError:
                value = MetaStatus.invalid

            values[key] = value

        self.spsActor.bcast.inform('biaStatus={power},{period},{duty},{pulseOn},{pulseOff}'.format(**values))
