import model
from model import Session
import csv
from datetime import datetime
import numpy as np
import sqlalchemy

session = Session()


if __name__ == "__main__":
    with open('00101_ct1.csv', "r") as f:
        stamp = f.readline().strip()
        num_headers = int(f.readline().split('=')[1].strip())
        headers = {}

        count = 1
        while count < num_headers:
            l = f.readline().split('=')
            count = count + 1
            param = l[0].strip()
            value = l[1].strip()
            if "EXPOCODE" in param:
                headers["expocode"] = value
                continue
            if "SECT_ID" in param:
                headers["section"] = value
                continue
            if "STNNBR" in param:
                headers["station_number"] = value
                continue
            if "CASTNO" in param:
                headers["cast_number"] = value
                continue
            if "DATE" in param:
                headers["date"] = value
                continue
            if "TIME" in param:
                headers["time"] = value
                continue
            if "LATITUDE" in param:
                headers["latitude"] = value
                continue
            if "LONGITUDE" in param:
                headers["longitude"] = value
                continue
            if "DEPTH" in param:
                headers["depth"] = value
                continue
            if "INSTRUMENT_ID" in param:
                headers["serial"] = value
                continue

        dt = datetime.strptime(headers["date"] + headers["time"], "%Y%m%d%H%M")
        xyt = model.XYT(headers["longitude"], headers["latitude"], dt)
        expocode = model.Expocode(headers["expocode"])
        section = model.Section(headers["section"], "trackline", "description")
        cruise = model.Cruise(None, None)
        cruise.expocode = expocode
        cruise.section = section

        station = model.Station(headers["station_number"], headers["cast_number"],
                headers["depth"])
        station.xyt = xyt
        station.cruise = cruise

        
        session.add(xyt)
        session.add(expocode)
        session.add(station)
        session.add(cruise)
        session.add(section)

        print xyt, expocode, station, cruise, section
        #print headers
        reader = csv.reader(f, delimiter=',')
        temp_params = reader.next()
        temp_units = reader.next()
        
        # we need to find the columns that are quality flags and note which
        # parameter they belong to, so we will buld a list of tuples containing
        # ("parameter name", "unit", param_index, quality_index) so we can quickly do
        # stuff
        parameters = []
        for i, p in enumerate(temp_params):
            if "FLAG_W" not in p:
                #we have an actual parameter name!
                param = p
                param_index = i
                unit = None
                if temp_units[i] is not '':
                    unit = temp_units[i]
                
                #now find the quality column (if any)
                flag_index = None
                for f_i, f in enumerate(temp_params):
                    if param in f and "FLAG_W" in f:
                        flag_index = f_i

                parameters.append((param, unit, param_index, flag_index))

        #for the actual data, I am going to just load the entire dataset
        #(checking the row lengths)
        data = []
        l = []
        while True:
            l = [l.strip() for l in reader.next()]
            if "END_DATA" in l:
                break
            data.append(l)

        data = np.array(data)
        for param in parameters:
            p = model.Parameter(param[0], "short_name", "precision")
            if param[1] is not None:
                u = model.Unit(param[1])
                p.unit = u
                session.add(u)
            session.add(p)
            if param[3] is not None:
                for d, q in zip(data[:, param[2]], data[:, param[3]]):
                    #try:
                    #    q = session.query(model.Quality).filter_by(name =
                    #            q).one()
                    #except (sqlalchemy.orm.exc.NoResultFound,):
                    #    q = model.Quality(q, 'description')
                    q = model.Quality.find_or_new(session, q, "description")
                    session.add(q)

                    d = model.Measurement(d)
                    d.quality = q
                    d.xyt = xyt
                    d.parameter = p
                    session.add(d)
            if param[3] is None:
                for d in data[:, param[2]]:
                    d = model.Measurement(d)
                    d.xyt = xyt
                    d.parameter = p
                    session.add(d)
    session.commit()
