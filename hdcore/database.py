from datetime import date, time
from ujson import loads, load

from sqlalchemy.sql import select, insert
from sqlalchemy import and_, or_

from hdcore.model.db import (engine, profiles, hydro_data, cruises, parameters,
        quality)

def is_redundant():
    with engine.connect() as conn:
        d = {}
        s = select([parameters.c.id, parameters.c.quality_class]).where(
                parameters.c.type=="cchdo")
        r = conn.execute(s)
        r = r.fetchall()
        for e in r:
            if e[1] is not None:
                s = select([quality.c.value]).where(
                    quality.c.quality_class==e[1]).where(
                    or_(
                        quality.c.default_data_present==True,
                        quality.c.default_data_missing==True
                        )
                    )
                rr = conn.execute(s)
                rr = [ee[0] for ee in  rr.fetchall()]
                d[str(e[0])] = lambda x: x in rr
            else:
                d[str(e[0])] = lambda x: x.startswith("-999")
        return d


def versioned_load(data):
    with engine.connect() as conn, conn.begin() as trans:
        data_compress = is_redundant()
        s = select([cruises.c.id]).where(cruises.c.id==data["cruise_id"])
        r = conn.execute(s)
        if not r.fetchone():
            raise BaseException("TODO: cruise not found exception")
        for profile in data['profiles']:
            profile_insert = []
            s = select([profiles.c.id, profiles.c.samples]).where(
                    and_(
                        profiles.c.cruise_id==data["cruise_id"],
                        profiles.c.current==True,
                        profiles.c.station==profile["station"],
                        profiles.c.cast==profile["cast"],
                        )
                    )
            r = conn.execute(s)
            r = r.fetchall()
            if len(r) is 1:
                pass
            if len(r) > 1:
                raise BaseException("TODO: make non-unique profile error")
            exit()
            for d in profile['data']:
                # only store actual meaningful data..
                del_keys = []
                for dd in d['data']:
                    if data_compress[dd](d['data'][dd]):
                        del_keys.append(dd)
                for key in del_keys:
                    del d['data'][key]

                
                d['current'] = True
                d['cruise_id'] = data["cruise_id"]
                profile_insert.append(d)
            r = conn.execute(hydro_data.insert().\
                    returning(hydro_data.c.id).values(profile_insert))
            ids = ([e[0] for e in r.fetchall()])
            del profile["data"]
            profile["cruise_id"] = data["cruise_id"]
            profile["samples"] = ids
            profile["current"] = True
            profile["date_z"] = date(
                    int(profile["date_z"][:4]),
                    int(profile["date_z"][4:6]),
                    int(profile["date_z"][6:8]))
            if "time_z" in profile:
                profile["time_z"] = time(
                        int(profile["time_z"][:2]),
                        int(profile["time_z"][2:]))
            conn.execute(profiles.insert().values(profile))
        trans.commit()

def versioned_load_jsons(s):
    versioned_load(loads(s))

def versioned_load_jsonf(f):
    versioned_load(load(f))
