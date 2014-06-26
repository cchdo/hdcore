from __future__ import print_function
from multiprocessing import Pool

from sqlalchemy.sql import select

from hdcore.model.db import cruises, engine

def _stamp_check(fname):
    with open(fname) as f:
        line = f.readline()
        return (line.startswith("CTD,"),fname)

def _check_file_stamps(fnames):
    pool = Pool()
    print("Checking file stamps...", end='')
    result = pool.map_async(_stamp_check, fnames)
    result = result.get()
    pool.close()
    pool.join()
    if all([r[1] for r in result]):
        print("\033[32mOK\033[0m")
    else:
        fails = []
        for f in result:
            if f[1] is False:
                fails.append(f[0])
        print("\033[31mFail\033[0m")
        print("{} has an improper filestamp for a CTD Exchange".format(fails))
        exit(1)

def _file_headers(fname_with_expo_list):
    fname = fname_with_expo_list[0]
    expocodes = fname_with_expo_list[1]
    with open(fname) as f:
        stamp = f.readline()
        line = f.readline()
        while line.startswith("#"):
            line = f.readline()
        
        key, value = line.split("=")
        if key.strip() != "NUMBER_HEADERS":
            return (False, fname, "Number of headers not defined")
        d = {}
        for _ in range(int(value) - 1):
            line = f.readline()
            key, value = line.split("=")
            d[key.strip()] = value.strip()
        errors = []    
        if "EXPOCODE" not in d:
            errors.append("Expocode not present")
        if d["EXPOCODE"] not in expocodes:
            errors.append("Expocode {} not recognized".format(d["EXPOCODE"]))

        if len(errors) > 0:
            return (False, fname, errors)
        else:
            return (True, fname, "ok")    


def _check_headers(fnames):
    print("Checking file headers...", end='')
    with engine.connect() as conn:
        s = select([cruises.c.expocode])
        result = conn.execute(s)
        expocodes = [r[0] for r in result.fetchall()]

    pool = Pool()
    expocodes = [expocodes for fname in fnames]
    args = zip(fnames, expocodes)
    result = pool.map_async(_file_headers, args)
    pool.close()
    pool.join()
    result = result.get()
    if all([r[0] for r in result]):
        print("\033[32mOK\033[0m")


def _check_file_parameters(fnames):
    def file_parameters(fname, params):
        with open(fname) as f:
            pass


def load(fnames):
    _check_file_stamps(fnames)
    _check_headers(fnames)
    #_check_file_parameters(fnames)
