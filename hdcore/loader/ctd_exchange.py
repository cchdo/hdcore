import csv
import os
from tempfile import TemporaryDirectory
from zipfile import ZipFile
from itertools import repeat
from multiprocessing import Pool
from collections import Counter

from sqlalchemy.sql import select

from hdcore.model.db import (engine, parameters, quality, cruises, profiles,
        hydro_data)
from hdcore.error import (HeaderError, FileStampError, AmbigiousProfileError,
        FileIntegrityError, DataIntegrityError, ParameterError)

def _result_handler(results, print_status, exception):
    if all([r[0] for r in results]):
        if print_status:
            print("\033[32mOK\033[0m")
    else:
        if print_status:
            print("\033[31mFail\033[0m")
        messages = ["{0}: {1}".format(r[1], r[2]) for r in results if r[0] is
                False]
        message = "\n".join(messages)
        raise exception(message)

def _stamp_check(fname):
    with open(fname) as f:
        line = f.readline()
        if line.startswith("CTD,"):
            return (True, fname, "ok")
        else:
            return (False, fname, "the CTD excahnge file stamp was not found")

def _check_file_stamps(fnames, print_status):
    if print_status:
        print("Checking file stamps...", end='')
    pool = Pool()
    result = pool.map_async(_stamp_check, fnames)
    result = result.get()
    pool.close()
    pool.join()
    _result_handler(result, print_status, FileStampError)

def _file_headers(fname, expocodes):
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
    result = pool.starmap_async(_file_headers, zip(fnames, repeat(expocodes)))
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
            #http://stackoverflow.com/questions/11236006/identify-duplicate-values-in-a-list-in-python
            dupes = [k for k,v in Counter(station_id).items() if v>1]
            if print_status:
                print("\033[31mFail\033[0m")
            messages = ["Duplicated Expocode: {0}, Station: {1}, Cast: {2}".\
                    format(*dup) for dup in dupes]
            raise AmbigiousProfileError("\n".join(messages))

    _result_handler(result, print_status, HeaderError)

def _file_parameters(fname, known):
    with open(fname) as f:
        stamp = f.readline()
        line = f.readline()
        while line.startswith("#"):
            line = f.readline()
        key, value = line.split("=")
        for _ in range(int(value) - 1):
            line = f.readline()

        reader = csv.reader(f, delimiter=',')
        params = next(reader)
        units = [u if len(u) > 0 else None for u in next(reader)]
        if len(params) is not len(units):
            return (False, fname, "Parameter units mismatch")
        pairs = list(zip(params, units))
        if len(pairs) is not len(set(pairs)):
            return (False, fname, "Duplicated parameters")
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
    
    pool = Pool()
    result = pool.starmap_async(_file_parameters, zip(fnames, repeat(params)))
    pool.close()
    pool.join()
    result = result.get()
    _result_handler(result, print_status, ParameterError)

def _data(fname, known, q_flags):
    import csv
    known = {(k[0], k[1]):{
        "id":k[2],
        "format": k[3],
        "quality": (k[5], k[6]),
        "quality_class": k[4],
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
        data_with_flags = {}
        for point, pair in zip(row, pairs):
            if known[pair]["quality_class"] is not None:
                continue
            if known[pair]["quality"] in known:
                data_with_flags[known[pair]["quality"]] = point

        for point, pair in zip(row, pairs):
            if known[pair]["quality_class"] is None:
                continue
            q_key = (known[pair]["quality_class"], point)
            if q_key in q_flags:
                if pair not in data_with_flags: # Flag column with no data col
                    return False
                if q_flags[q_key] is True: #flag says there should be data
                    if data_with_flags[pair].startswith("-999"):
                        return False
                else: # guess there should be no data, it had better be a fill
                    if not data_with_flags[pair].startswith("-999"):
                        return False
            else:
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
        params = next(reader)
        units = [u if len(u) > 0 else None for u in next(reader)]
        pairs = list(zip(params, units))
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
        param_alias = parameters.alias()
        s = select([
            parameters.c.name,
            parameters.c.units_repr,
            parameters.c.id,
            parameters.c.format_string,
            parameters.c.quality_class,
            param_alias.c.name, #quality name
            param_alias.c.units, #quality units, probably None
            ]).where(
                    parameters.c.type=="cchdo"
                    ).select_from(
                            parameters.outerjoin(param_alias,
                                parameters.c.quality == param_alias.c.id)
                            )
        result= conn.execute(s)
        params = [r for r in result.fetchall()]
        s = select([
            quality.c.quality_class,
            quality.c.value,
            quality.c.has_data,
            ])
        result = conn.execute(s)
        q_flags = {(r[0], r[1]):r[2] for r in result.fetchall()}

    pool = Pool()
    result = pool.starmap_async(_data, 
                zip(fnames, repeat(params), repeat(q_flags))
                    )
    result = result.get()
    pool.close()
    pool.join()
    _result_handler(result, print_status, DataIntegrityError)


def load(fnames, print_status=False):
    try:
        _check_file_stamps(fnames, print_status)
        _check_headers(fnames, print_status)
        _check_file_parameters(fnames, print_status)
        _check_data(fnames, print_status)
    except Exception as e:
        raise FileIntegrityError("The files could not be loaded") from e

    


def load_zip(fname):
    with TemporaryDirectory() as tempdir:
        with ZipFile(fname) as zipf:
            zipf.extractall(path=tempdir)
            for base, dirs, fnames in os.walk(tempdir):
                if len(dirs) is not 0:
                    raise BaseException("Directories present in zipfile")
                load([os.path.join(base, fname) for fname in fnames])
            
