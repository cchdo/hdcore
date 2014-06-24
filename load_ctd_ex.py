from __future__ import print_function
import models.model as model
import models
from models import engine
from models import Session
from models.quality import Quality
import csv
from datetime import datetime
import numpy as np
import sqlalchemy
from sqlalchemy.orm import subqueryload, joinedload
import os
import cdecimal as decimal
import gc

session = Session()



def load_ctd_ex(filenames):
    import StringIO
    import sys
    import time
    t0 = time.time()
    t = len(filenames)
    for i, f in enumerate(filenames):
        with open(f, 'rb') as file:
            a = int((float(i)/float(t)) * 100)
            if i < (t-1):
                print("Loading Files Into Memory...{0}%".format(a), end='')
            if i == (t-1):
                print("Loading Files Into Memory...{0}%".format(100), end='\n')
            o = StringIO.StringIO()
            o.write(file.read())
            o.seek(0)

            #discard all headers and only retain data
            o.readline()
            l = o.readline()
            while l.startswith("#"):
                l = o.readline()

            num_headers = int(l.split("=")[1].strip())
            c = -1
            while c < num_headers:
                o.readline()
                c = c + 1

            filenames[i] = csv.reader(o, delimiter=',')
            sys.stdout.flush()
            if i < (t-1):
                print('\r', end='')
    

    #file_dict = {}
    #for i, f in enumerate(filenames):
    #    temp_dict = {}
    #    a = int((float(i)/float(t)) * 100)
    #    if i < (t-1):
    #        print("Reading File Headers...{0}%".format(a), end='')
    #    if i == (t-1):
    #        print("Reading File Headers...{0}%".format(100), end='\n')

    #    sys.stdout.flush()
    #    stamp = f.readline().strip() #TODO make sure this is sane
    #    #strip the comments
    #    s = f.readline()
    #    while s.startswith('#'):
    #        s = f.readline()
    #    num_headers = int(s.split('=')[1].strip())
    #    headers = {}

    #    count = 1
    #    while count < num_headers:
    #        l = f.readline().split('=')
    #        count = count + 1
    #        param = l[0].strip()
    #        value = l[1].strip()
    #        if "EXPOCODE" in param:
    #            headers["expocode"] = value
    #            continue
    #        if "SECT" in param:
    #            headers["section"] = value
    #            continue
    #        if "SECT_ID" in param:
    #            headers["section"] = value
    #            continue
    #        if "STNNBR" in param:
    #            headers["station_number"] = value
    #            continue
    #        if "CASTNO" in param:
    #            headers["cast_number"] = value
    #            continue
    #        if "DATE" in param:
    #            headers["date"] = value
    #            continue
    #        if "TIME" in param:
    #            headers["time"] = value
    #            continue
    #        if "LATITUDE" in param:
    #            headers["latitude"] = value
    #            continue
    #        if "LONGITUDE" in param:
    #            headers["longitude"] = value
    #            continue
    #        if "DEPTH" in param:
    #            headers["depth"] = value
    #            continue
    #        if "INSTRUMENT_ID" in param:
    #            headers["serial"] = value
    #            continue

    #    dt = datetime.strptime(headers["date"] + headers["time"], "%Y%m%d%H%M")
    #    xyt = model.XYT(headers["longitude"], headers["latitude"], dt)
    #    expocode = model.Expocode.find_or_new(session, headers["expocode"])
    #    session.add(expocode)
    #    #session.flush()
    #    section = model.Section(headers["section"], "trackline", "description")
    #    cruise = model.Cruise.find_or_new(session, expocode, None, None)
    #    cruise.expocode_id = expocode.id
    #    cruise.section = section

    #    station = model.Station(headers["station_number"], headers["cast_number"],
    #            headers["depth"])
    #    station.xyt = xyt
    #    station.cruise = cruise

    #    
    #    session.add(xyt)
    #    session.add(station)
    #    session.add(cruise)
    #    session.add(section)
    #    temp_dict["XYT"] = xyt
    #    temp_dict["station"] = station
    #    temp_dict["cruise"] = cruise
    #    temp_dict["section"] = section

    #    #print(xyt, expocode, station, cruise, section)
    #    #print headers
    #    reader = csv.reader(f, delimiter=',')
    #    temp_dict['reader'] = reader
    #    temp_params = reader.next()
    #    temp_units = reader.next()
    #    
    #    # we need to find the columns that are quality flags and note which
    #    # parameter they belong to, so we will buld a list of tuples containing
    #    # ("parameter name", "unit", param_index, quality_index) so we can quickly do
    #    # stuff
    #    parameters = []
    #    for i_p, p in enumerate(temp_params):
    #        if "FLAG_W" not in p and p is not '':
    #            #we have an actual parameter name!
    #            param = p
    #            param_index = i_p
    #            unit = None
    #            if temp_units[i_p] is not '':
    #                unit = temp_units[i_p]
    #            
    #            #now find the quality column (if any)
    #            flag_index = None
    #            for f_i, f in enumerate(temp_params):
    #                if param in f and "FLAG_W" in f:
    #                    flag_index = f_i

    #            parameters.append((param, unit, param_index, flag_index))

    #    for i_p, param in enumerate(parameters):
    #        p = model.Parameter.find_or_new(session, param[0], "short_name", 
    #                "precision")
    #        if param[1] is not None:
    #            u = model.Unit.find_or_new(session, param[1])
    #            p.unit = u
    #            session.add(u)
    #        parameters[i_p] = (p, param[2], param[3])
    #        session.add(p)

    #    session.flush()
    #    temp_dict['parameters'] = parameters

    #    if i < (t-1):
    #        print('\r', end='')
    #    file_dict[i] = temp_dict

    session.autoflush = False
    session.autocommit = False
    ins = []
    profile_id = 1
    gc.disable()
    for i, f in enumerate(filenames):
        a = int((float(i)/float(t)) * 100)
        if i < (t-1):
            print("Reading Data...{0}%".format(a), end='\r')
        if i == (t-1):
            print("Reading Data...{0}%".format(100), end='\n')
        sys.stdout.flush()
        #for the actual data, I am going to just load the entire dataset
        #(checking the row lengths)
        data = []
        #l = []
        while True:
            l = [l.strip() for l in f.next()]
            if "END_DATA" in l:
                break
            data.append(l)
        for i, d in enumerate(data):
            prs = d[0]
            tmp = d[2]
            oxy = d[6]
            ins.append({"value": prs, "sample_group": i, "profile_id": profile_id})
            ins.append({"value": tmp, "sample_group": i, "profile_id": profile_id + 1})
            ins.append({"value": oxy, "sample_group": i, "profile_id": profile_id + 2})
        profile_id = profile_id + 3
        

        ## Numpy for fancy slicing
        #data = list(f)
        #decimal_data = []
        #for l in data[:-1]:
        #    decimal_data.append([decimal.Decimal(x.strip()) for x in l])
        #for l in data[:-1]:
        #    decimal_data.append([x.strip() for x in l])
        #data = np.array(data[:-1])
        #data = data.astype(np.float)
        #quality_cache = {}

        # get just the CTDPRS (index) data
        #primaries = []
        #for row in data:
        #    for param in file_dict[i]['parameters']:
        #        if "CTDPRS" not in param[0].name:
        #            continue
        #        d = row[param[1]]
        #        d = model.Measurement(d)
        #        if param[2] is not None:
        #            q = row[param[2]]
        #            if q not in quality_cache:
        #                quality_cache[q] = session.query(Quality).filter_by(name = q).filter_by(type = 'woce').one()
        #                session.add(quality_cache[q])
        #            q = quality_cache[q]

        #            d.quality = q

        #        d.xyt = file_dict[i]['XYT']
        #        d.parameter = param[0]
        #        d.primary = d
        #        primaries.append(d)
        #        session.add(d)
        #    
        #for row, primary in zip(data, primaries):
        #    for param in file_dict[i]['parameters']:
        #        if "CTDPRS" in param[0].name:
        #            continue
        #        d = row[param[1]]
        #        if param[2] is not None:
        #            q = row[param[2]]
        #            if q not in quality_cache:
        #                quality_cache[q] = session.query(Quality).filter_by(name = q).filter_by(type = 'woce').one()
        #                session.add(quality_cache[q])
        #            q = quality_cache[q]

        #        ins.append({ 'XYT_id':file_dict[i]["XYT"].id, 'parameter_id':param[0].id,
        #                'quality_id':q.id, 'primary_id': primary.id, 'value': decimal.Decimal(d),
        #                'instrument_id' :None})

    gc.enable()
    print("Loading Data into Database...", end="")
    #sys.stdout.flush()
    #session.commit()
    print("Data", end="")

    i = model.Measurement.__table__.insert()

    con = engine.connect()
    trans = con.begin()
    sys.stdout.flush()
    con.execute(i, ins)
    trans.commit()
    print("MetaData", end="")
    sys.stdout.flush()
    #session.commit()
    print("DONE!")
    print(str(time.time() - t0),)


if __name__ == "__main__":
    models.db_init()
    datafiles = []
    for path, dirs, files in os.walk('test_data'):
        for file in files:
            datafiles.append(os.path.join(path, file))

    load_ctd_ex(datafiles)

    a = session.query(model.Measurement).first()
    print( "a =", a)

def exchange_from_expo(session, expocode):
    expocode = session.query(model.Expocode).filter_by(expocode = expocode).\
            options(joinedload('cruise'), joinedload("cruise.section"),
                    joinedload("cruise.stations"),).\
            one()
    cruise = expocode.cruise
    section = cruise.section
    stations = cruise.stations
    for station in stations:
        filename = (str(station.station_number) + "_" +
                    str(station.cast_number) + ".csv")
        filename = os.path.join('test_output', filename)
        f = open(filename, 'wb')
        xyt = station.xyt
        headers = []
        headers.append(('EXPOCODE', expocode.expocode))
        if section is not None:
            headers.append(('SECT_ID', section.name))
        headers.append(('STNNBR', station.station_number))
        headers.append(('CASTNO', station.cast_number))
        headers.append(('DATE', xyt.datetime.strftime('%Y%m%d')))
        headers.append(('TIME', xyt.datetime.strftime('%H%M')))
        headers.append(('LATITUDE', xyt.lat))
        headers.append(('LONGITUDE', xyt.lon))
        headers.append(('DEPTH', station.bottom_depth))
        num_headers = len(headers) + 1

        primaries = session.query(model.Measurement).\
                    options(joinedload('dependants'), joinedload('parameter')).\
                filter_by(xyt = station.xyt).\
                filter(model.Measurement.id ==
                        model.Measurement.primary_id).\
                            all()

        # get all the parameters and stuff (make this a conviencne method
        # somewhere

        #TODO make this order things

        #params = session.query(model.Parameter).\
        #            select_from(model.Measurement).\
        #            filter(model.Measurement.xyt == station.xyt).\
        #            all()
        params = [x.parameter for x in primaries[0].dependants]
        print( "NUMBER_HEADERS = " + str(num_headers), file=f)
        for header in headers:
            print( header[0] + ' = ' + str(header[1]), file=f)
        primary = primaries[0]
        p = []
        u = []
        p.append(primary.parameter.name.encode('utf-8'))
        u.append(primary.parameter.unit.name.encode('utf-8'))
        parameters = []
        for param in params:
            for dep in primary.dependants:
                if dep is primary:
                    continue
                if dep.parameter == param:
                    p.append(dep.parameter.name.encode('utf-8'))
                    u.append(dep.parameter.unit.name.encode('utf-8'))
        print( ','.join(p), file=f)
        print( ','.join(u), file=f)
            
        

        for primary in primaries:
            print( ','.join(primary.value_list(p)), file=f)
        #    print primary.dependants
        #    row = []
        #    row.append(str(primary.value))#.encode('utf-8'))
        #    row.append(primary.quality.name.encode('utf-8'))
        #    for param in params:
        #        for dep in primary.dependants:
        #            if dep is primary:
        #                continue
        #            if dep.parameter == param:
        #                row.append(str(dep.value))#.encode('utf-8'))
        #                row.append(str(dep.quality.name))#.encode('utf-8'))
        #    print ','.join(row)
