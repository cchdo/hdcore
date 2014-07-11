from __future__ import print_function
from multiprocessing import Pool

from sqlalchemy.sql import select

from hdcore.model.db import cruises, engine, parameters, quality

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
    if all([r[1] for r in result]):
        if print_status:
            print("\033[32mOK\033[0m")
        return True
    else:
        if print_status:
            fails = []
            for f in result:
                if f[1] is False:
                    fails.append(f[0])
            print("\033[31mFail\033[0m")
            print("{} has an improper filestamp for a CTD Exchange".format(fails))
        return False

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
            return (True, fname, "ok", d)    


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

    if all([r[0] for r in result]):
        station_id = []
        for r in result:
            station_id.append((r[3]["EXPOCODE"],
                r[3]["STNNBR"],
                r[3]["CASTNO"])
                )
        if len(station_id) is not len(set(station_id)):
            if print_status:
                print("\033[31mFail\033[0m")
                print("Non unique station casts present")
            return False
        if print_status:
            print("\033[32mOK\033[0m")
        return True
    else:
        if print_status:
            print("\033[31mFail\033[0m")
        return False

def _file_parameters(fname_with_params):
    # TODO check to see if the corrisponding column exists for a flag column
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
        if len(params) is not len(units):
            return (False, fname, "Parameter units mismatch")
        pairs = zip(params, units)
        if len(pairs) is not len(set(pairs)):
            return (False, fname, "Duplicated parameters")
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
    if all([r[0] for r in result]):
        if print_status:
            print("\033[32mOK\033[0m")
        return True
    else:
        if print_status:
            print("\033[31mFail\033[0m")
        return False

def _data(fname_with_params):
    import csv
    fname = fname_with_params[0]
    known = fname_with_params[1]
    q_flags = fname_with_params[2]
    known = {(k[0], k[1]):{
        "id":k[2],
        "format": k[3],
        "quality": k[4],
        "quality_class": k[5],
        } for k in known}

    def row_length_ok(row, pairs):
        return len(row) is len(pairs)

    def data_types_ok(row, pairs, known):
        for point, pair in zip(row, pairs):
            param = known[pair]
            if param["format"].endswith('f'):
                try:
                    float(point)
                except TypeError:
                    return False
            elif param["format"].endswith('s'):
                try:
                    str(point)
                except TypeError:
                    return False
            elif param["format"].endswith('i'):
                try:
                    int(point)
                except TypeError:
                    return False
            else:
                return False
        return True

    def data_values_ok(row, pairs, known, q_flags):
        """This is probably the most complicated check.
        Here the row is being checked to see if the following:
        1) Flag columns have allowed values in them
        2) Where flags indicate that there should be no data, the corresponding
        data column has a fill value
        3) where the flags indicate that there should be data, the
        corresponding data column has a non fill value
        """
        # check the flag values and make a list of things that should have data
        data_mask = [True for _ in row] #assume all positions should have data
        for point, pair in zip(row, pairs):
            if known[pair]["quality_class"] is None:
                continue
            allowed_values = {}
            for q in q_flags:
                if q[0] == known[pair]["quality_class"]:
                    allowed_values[q[1]] = q[2]
            if point not in allowed_values:
                return False
            
        return True

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
        # Exchange CTD files are pressure series
        index_id = params.index("CTDPRS") 
        index = []
        for row in reader:
            if row[index_id] in index:
                return (False, fname, "Non unique index parameter")
            index.append(row[index_id])
            if row[0] == "END_DATA":
                return (True, fname, 'ok')
            if not row_length_ok(row, pairs):
                return (False, fname, "Data row length mismatch")
            if not data_types_ok(row, pairs, known):
                return (False, fname, "Some data is not formatted well")
            if not data_values_ok(row, pairs, known, q_flags):
                return (False, fname, "Some data is inconsistent/invalid")

        return (False, fname, 'No "END_DATA" in file')

def _check_data(fnames, print_status):
    if print_status:
        print("Checking file data...", end='')
    with engine.connect() as conn:
        s = select([
            parameters.c.name,
            parameters.c.units_repr,
            parameters.c.id,
            parameters.c.format_string,
            parameters.c.quality,
            parameters.c.quality_class,
            ]).where(
                    parameters.c.type=="cchdo"
                    )
        result= conn.execute(s)
        params = [r for r in result.fetchall()]
        params = [params for _ in fnames]
        s = select([
            quality.c.quality_class,
            quality.c.value,
            quality.c.has_data,
            ])
        result = conn.execute(s)
        q_flags = [r for r in result.fetchall()]
        q_flags = [q_flags for _ in fnames]

    fnames_with_params = zip(fnames, params, q_flags)
    #print(_data(fnames_with_params[0]))
    #exit()
    pool = Pool()
    result = pool.map_async(_data, fnames_with_params)
    result = result.get()
    pool.close()
    pool.join()
    #TODO make this actually report things (at least which file is wrong)
    if all([r[0] for r in result]):
        if print_status:
            print("\033[32mOK\033[0m")
        return True
    else:
        if print_status:
            fails = []
            for f in result:
                if f[0] is False:
                    fails.append(f)
            print("\033[31mFail\033[0m")
            for fail in fails:
                print("{} failed due to {}".format(fail[1], fail[2]))
        return False

def load(fnames, print_status=False):
    if not _check_file_stamps(fnames, print_status):
        #TODO Figure out a real exception
        raise BaseException
    if not _check_headers(fnames, print_status):
        raise BaseException
    if not _check_file_parameters(fnames, print_status):
        raise BaseException
    if not _check_data(fnames, print_status):
        raise BaseException
