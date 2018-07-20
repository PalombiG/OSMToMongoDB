# Script contente le funzioni comuni utilizzabili da tutti i restanti script
import logging
import logging.config
import arcpy

# Livelli di log possibili (relativamente al messaggio da mostrare)
INFO_LOG = 0
ERROR_LOG = 1
WARNING_LOG = 2
FILE_ONLY_LOG = 3

# funzione che restituisce il logger di default
def getLogger(loggerType, inipath, logpath):
    # apre il file ini contente i settings del log
    logging.config.fileConfig(inipath + "/logging.ini")
    logger = logging.getLogger("StandardLogger")
    fh = logging.FileHandler(logpath + "/"+loggerType+".log")
    # instanzia il formatter che stabilisce in che modo vanno mostrate le singole
    # righe di log
    formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(lineno)04d | %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

def RepresentsInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

# funzione che permette di mostrare un nuovo messaggio nel logger standard
def addLogMessage(msgstring, logger, log_type, add_arcpy_message = False, log_file_enabled = True):
    if add_arcpy_message:
        if (log_type==INFO_LOG): arcpy.AddMessage(msgstring)
        if (log_type==ERROR_LOG): arcpy.AddError(msgstring)
        if (log_type==WARNING_LOG): arcpy.AddWarning(msgstring)

    if (log_file_enabled):
        try:
            if (log_type == INFO_LOG): logger.info(msgstring)
            if (log_type == ERROR_LOG): logger.error(msgstring)
            if (log_type == WARNING_LOG): logger.warning(msgstring)
            if (log_type == FILE_ONLY_LOG): logger.info(msgstring)
        except:
            pass

# funzione utilizzata per leggere parametri dal file ini standard
def readParameter(settings, section, parameter):
    parvalue = ''
    try:
        parvalue = settings.get(section, parameter)
    except:
        pass
    return parvalue
