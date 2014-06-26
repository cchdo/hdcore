from __future__ import print_function
from multiprocessing import Pool

from sqlalchemy.sql import select

from hdcore.model.db import cruises, engine, parameters

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
        if "STNNBR" not in d:
            errors.append("STNNBR not present")
        if "CASTNO" not in d:
            errors.append("CASTNO not present")
        if "DATE" not in d:    
            errors.append("DATE not present")
        if "LATITUDE" not in d:    
            errors.append("LATITUDE not present")
        if "LONGITUDE" not in d:    
            errors.append("LONGITUDE not present")

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

def _file_parameters(fname_with_params):
    import csv
    fname = fname_with_params[0]
    known = fname_with_params[1]
    with open(fname) as f:
        stamp = f.readline()
        line = f.readline()
        while line.startswith("#"):
            line = f.readline()
        key, value = line.split("=")
        for _ in range(int(value) - 1):
            line = f.readline()

        reader = csv.reader(f, delimiter=',')
        params = reader.next()
        units = [u if len(u) > 0 else None for u in reader.next()]
        pairs = zip(params, units)
        if len(params) is not len(units):
            return (False, fname, "Parameter unit mismatch")
        for pair in pairs:
            if pair not in known:
                return (False, fname, "{} is unknown".format(pair))
        else:
            return (True, fname, "ok")

def _check_file_parameters(fnames):
    print("Checking file parameters...", end='')
    with engine.connect() as conn:
        s = select([parameters.c.name, parameters.c.units_repr])
        result = conn.execute(s)
        params = [(r[0], r[1]) for r in result.fetchall()]
        params = [params for _ in fnames]
    
    pool = Pool()
    fnames_with_params = zip(fnames, params)
    result = pool.map_async(_file_parameters, fnames_with_params)
    pool.close()
    pool.join()
    result = result.get()
    if all([r[0] for r in result]):
        print("\033[32mOK\033[0m")

def load(fnames):
    _check_file_stamps(fnames)
    _check_headers(fnames)
    _check_file_parameters(fnames)
