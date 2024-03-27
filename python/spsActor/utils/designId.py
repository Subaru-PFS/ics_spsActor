import glob
import logging

from ics.utils.opdb import opDB
from pfs.datamodel import PfsConfig


def findDesignIdAndNameFromDisk(visit_id):
    """
    Find the pfsDesignId and designName from the pfsConfig file on disk for a given visit ID.

    Parameters
    ----------
    visit_id : int
        The visit ID for which to find the pfsDesignId and name.

    Returns
    -------
    tuple
        A tuple containing the pfsDesignId and name associated with the given visit ID.
    """
    [filepath] = glob.glob(f'/data/raw/*/pfsConfig/pfsConfig-*-{visit_id:06d}.fits')
    pfsConfig = PfsConfig._readImpl(filepath)
    return pfsConfig.pfsDesignId, pfsConfig.designName


def findDesignIdAndNameFromDB(visit_id):
    """
    Find the pfsDesignId and name from the OPDB database for a given visit ID.

    Parameters
    ----------
    visit_id : int
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
        f"WHERE pfs_visit_id = {visit_id}"
    )

    design_id, design_name = opDB.fetchone(sql)
    return int(design_id), design_name


def getPfsDesignIdAndName(visit_id):
    """
    Get the pfsDesignId and name for a given visit ID.

    Parameters
    ----------
    visit_id : int
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
    design_id, design_name = 0, ""

    # Try to find the pfsDesignId and name from the OPDB database first.
    try:
        design_id, design_name = findDesignIdAndNameFromDB(visit_id)
    except TypeError:
        # If no result is found in the database, try loading the configuration file from disk.
        logger = logging.getLogger('opdb')
        logger.warning(f'Unable to find entry for pfs_config table with pfs_visit_id={visit_id}, trying from disk.')
        try:
            design_id, design_name = findDesignIdAndNameFromDisk(visit_id)
        except ValueError:
            # If neither method is successful, log a warning message.
            logger.warning(f'Unable to find pfsConfig file matching pfs_visit_id={visit_id}')

    return design_id, design_name
