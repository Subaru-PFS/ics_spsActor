class ShutterState(object):
    """
    Class to track and manage the state of the shutters for a spectrograph.

    Attributes:
    ----------
    spec : object
        The spectrograph object associated with this shutter state.
    states : list
        A list of states representing the shutter's history, starting with 'none'.
    """

    def __init__(self, spec):
        """
        Initialize the ShutterState object with the given spectrograph.

        Parameters:
        ----------
        spec : object
            The spectrograph object to which this shutter state belongs.
        """
        self.spec = spec
        self.states = ['none']

    @property
    def isOpen(self):
        """
        Check if the shutters are currently open.

        Returns:
        -------
        bool: True if the last state is 'open', False otherwise.
        """
        return 'open' in self.states[-1]

    @property
    def didExpose(self):
        """
        Check if the shutters opened and then closed, indicating an exposure.

        Returns:
        -------
        bool: True if the second last state was 'open' and the last state was 'close'.
        """
        return 'open' in self.states[-2] and 'close' in self.states[-1]

    @property
    def wasOpen(self):
        """
        Check if the shutters were open at any point.

        Returns:
        -------
        bool: True if 'open' appears in any of the previous states.
        """
        return any(['open' in state for state in self.states])

    def newStateValue(self, state):
        """
        Update the shutter state and track if it's a new state.

        Parameters:
        ----------
        state : str
            The new state of the shutter.

        Returns:
        -------
        bool: True if the new state differs from the current state, False otherwise.
        """
        isNew = False
        self.spec.actor.bcast.debug(f'text="{self.spec.specName} shutters {state}"')

        if self.states[-1] != state:
            self.states.append(state)
            isNew = True

        return isNew

    def callback(self, keyVar):
        """
        Callback to handle updates to the shutter state.

        Parameters:
        ----------
        keyVar : object
            The object that holds the new state value.

        Updates the state and triggers callbacks if the shutters are opened or closed after exposure.
        """
        state = keyVar.getValue(doRaise=False)

        isNew = self.newStateValue(state)

        if not isNew:
            return

        if self.isOpen:
            self.spec.shuttersOpenCB()

        if self.didExpose:
            self.spec.shuttersCloseCB()
