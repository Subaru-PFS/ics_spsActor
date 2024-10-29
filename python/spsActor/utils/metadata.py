import logging

from ics.utils.opdb import opDB


def findDesignIdAndNameFromDB(visitId):
    """
    Find the pfsDesignId and name from the OPDB database for a given visit ID.

    Parameters
    ----------
    visitId : int
        The visit ID for which to find the pfsDesignId and name.

    Returns
    -------
    tuple
        A tuple containing the pfsDesignId and name associated with the given visit ID.
    """
    sql = (
        f"SELECT pfs_config.pfs_design_id, design_name "
        f"FROM pfs_config_sps "
        f"INNER JOIN pfs_config ON pfs_config.visit0 = pfs_config_sps.visit0 "
        f"INNER JOIN pfs_design ON pfs_design.pfs_design_id = pfs_config.pfs_design_id "
        f"WHERE pfs_visit_id = {visitId}"
    )

    pfsDesignId, designName = opDB.fetchone(sql)
    return int(pfsDesignId), designName


def getPfsDesignIdAndName(visitId, doRaise=True):
    """
    Get the pfsDesignId and designName for a given visit ID.

    Parameters
    ----------
    visitId : int
        The visit ID for which to get the pfsDesignId and name.

    Returns
    -------
    tuple
        A tuple containing the pfsDesignId and name associated with the given visit ID.

    Notes
    -----
    This function first attempts to find the pfsDesignId and name from the OPDB. If no result is found, it
    falls back on loading the pfsConfig file from disk and extracting the pfsDesignId and name from there. If
    neither method is successful, it returns a default value of (0, "").
    """
    pfsDesignId, designName = 0, ""

    # Try to find the pfsDesignId and name from the OPDB database first.
    try:
        pfsDesignId, designName = findDesignIdAndNameFromDB(visitId)
    except TypeError:
        # If no result is found in the database, try loading the configuration file from disk.
        # EDIT 2024-10-24 pfsConfig file are now written when the shutter close, so there is no point checking the disk
        # at the begging of the exposure.
        if doRaise:
            raise RuntimeError(f'could not find pfsDesignId for {visitId} !')

        logger = logging.getLogger('opdb')
        logger.warning(f'Unable to find entry for pfs_config table with pfs_visitId={visitId}')

    return pfsDesignId, designName
