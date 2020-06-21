import configparser
import argparse
import logging
import os


def readconfig(configfile, sec):
    configdict = {}
    cwd = os.path.dirname(os.path.abspath(__file__))
    try:
        # Check Windows Path
        if cwd.find("\\") > 0:
            folderup = cwd.rfind("\\")
            configfilewithpath = cwd[:folderup] + "\\config\\" + configfile
        # Else go with linux path
        else:
            folderup = cwd.rfind("/")
            configfilewithpath = cwd[:folderup] + "/config/" + configfile

        config = configparser.RawConfigParser()
        config.read(configfilewithpath)
        options = config.options(sec)
    except:
        logging.error("Error reading config file {}".format(configfilewithpath))
        raise
    for option in options:
        try:
            configdict[option] = config.get(sec, option)
        except:
            print(('Exception!! could not find value for {}'.format(option)))
            configdict[option] = None
    return configdict
