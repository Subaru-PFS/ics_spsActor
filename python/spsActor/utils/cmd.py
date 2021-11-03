import re


def pullOutException(trace):
    """ Try to pull out the full exception trace. """
    start = re.search("(?<=command failed: )", trace).end()
    end = re.search("(?<= in )(.*)(?= at )", trace).end()
    return trace[start:end]


def interpretFailure(cmdVar):
    """ return text which should explain that cmdVar failure. """
    cmdKeys = cmdVarToKeys(cmdVar)

    if 'Timeout' in cmdKeys:
        return f"TimeoutError('reached in {cmdVar.timeLim} secs')"
    elif 'NoTarget' in cmdKeys:
        return 'actor is not connected'
    elif 'text' in cmdKeys:
        trace = cmdKeys['text'].values[0]
        try:
            failure = pullOutException(trace)
        except:
            failure = trace
        return failure
    else:
        return 'unknown reason'


def cmdVarToKeys(cmdVar):
    """ Convert cmdVar keyword as dictionary."""
    return dict(sum([[(k.name, k) for k in reply.keywords] for reply in cmdVar.replyList], []))


def parse(cmdStr, **kwargs):
    """ parse keyword argument to the cmdStr in a mhs compliant way. """
    args = []
    for k, v in kwargs.items():
        if v is None or v is False:
            continue
        args.append(k if v is True else f'{k}={v}')

    return ' '.join([cmdStr.strip()] + args)
