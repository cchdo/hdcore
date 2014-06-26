from __future__ import print_function
from multiprocessing import Pool

from sqlalchemy.sql import select

from hdcore.model.db import cruises, engine, parameters

def _stamp_check(fname):
    with open(fname) as f:
        line = f.readline()
        return (line.startswith("CTD,"),fname)

def _check_file_stamps(fnames, print_status):
    if print_status:
        print("Checking file stamps...", end='')
    pool = Pool()
    result = pool.map_async(_stamp_check, fnames)
    result = result.get()
    pool.close()
    pool.join()
    if print_status:
        if all([r[1] for r in result]):
            print("\033[32mOK\033[0m")
        else:
            fails = []
            for f in result:
                if f[1] is False:
                    fails.append(f[0])
            print("\033[31mFail\033[0m")
            print("{} has an improper filestamp for a CTD Exchange".format(fails))

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


def _check_headers(fnames, print_status):
    if print_status:
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
    if print_status:
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

def _check_file_parameters(fnames, print_status):
    if print_status:
        print("Checking file parameters...", end='')
    with engine.connect() as conn:
        s = select([parameters.c.name, parameters.c.units_repr]).where(
                parameters.c.type=="cchdo"
                )
        result = conn.execute(s)
        params = [(r[0], r[1]) for r in result.fetchall()]
        params = [params for _ in fnames]
    
    pool = Pool()
    fnames_with_params = zip(fnames, params)
    result = pool.map_async(_file_parameters, fnames_with_params)
    pool.close()
    pool.join()
    result = result.get()
    if print_status:
        if all([r[0] for r in result]):
            print("\033[32mOK\033[0m")
        else:
            print("\033[31mFail\033[0m")

def _data(fname_with_params):
    import csv
    fname = fname_with_params[0]
    known = fname_with_params[1]
    ops = {}
    for param in known:
        ops[(param[0], param[1])] = (param[2], param[3])
    checker = {}
    checker['float'] = float
    checker['string'] = str
    checker['integer'] = int

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
        check_type = []
        check_allowed = []
        for pair in pairs:
            check_type.append(checker[ops[pair][0]])
            check_allowed.append(ops[pair][1])
         
        for row in reader:
            if row[0] == "END_DATA":
                return (True, fname, 'ok')
            if len(row) != len(pairs):
                return (False, fname, "Data row length mismatch")
            for d, t, a in zip(row, check_type, check_allowed):
                try:
                    t(d.strip())
                except ValueError:
                    return (False, fname, "invalid data")
                if a:
                    if d.strip() not in a:
                        return (False, fname, "invalid flag")
                
        

def _check_data(fnames, print_status):
    if print_status:
        print("Checking file data...", end='')
    with engine.connect() as conn:
        s = select([
            parameters.c.name,
            parameters.c.units_repr,
            parameters.c.integrity_data_type,
            parameters.c.integrity_allowed_values,
            ]).where(
                    parameters.c.type=="cchdo"
                    )
        result= conn.execute(s)
        params = [(r[0], r[1], r[2], r[3]) for r in result.fetchall()]
        params = [params for _ in fnames]

    fnames_with_params = zip(fnames, params)
    pool = Pool()
    result = pool.map_async(_data, fnames_with_params)
    result.get()
    if print_status:
        print("\033[32mOK\033[0m")

def load(fnames, print_status=False):
    _check_file_stamps(fnames, print_status)
    _check_headers(fnames, print_status)
    _check_file_parameters(fnames, print_status)
    _check_data(fnames, print_status)
