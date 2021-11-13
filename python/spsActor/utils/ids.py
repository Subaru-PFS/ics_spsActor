from ics.utils.sps.spectroIds import SpectroIds


class SpsIds(object):
    """ just some useful cam/spectro identifcation functions. """
    allCams = [SpectroIds(f'{arm}{specNum}') for arm in SpectroIds.validArms for specNum in SpectroIds.validModules]

    @staticmethod
    def camFromNums(specNum, armNum):
        """ Retrieve cam object from specNum and armNum. """
        [cam] = [cam for cam in SpsIds.allCams if (cam.specNum == int(specNum) and cam.armNum == int(armNum))]
        return cam

    @staticmethod
    def getMask(frames):
        """ Return bit mask of the newly generated images. """
        mask = 0

        for cam in SpsIds.allCams:
            bit = cam.camId - 1
            mask |= (1 << bit if cam.camName in frames else 0)

        return mask

    @staticmethod
    def camToArmDict(cams):
        """ Convert camera list to arm dictionary. """
        armDict = dict()

        for cam in cams:
            try:
                armDict[cam.specNum].append(cam.arm)
            except KeyError:
                armDict[cam.specNum] = [cam.arm]

        return armDict
