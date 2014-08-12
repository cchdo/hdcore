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
            old_profile_id = None
            old_profile = {}
            if len(r) > 1:
                raise BaseException("TODO: make non-unique profile error")
            if len(r) is 1:
                old_profile_id = r[0][0]
                s = select([hydro_data.c.key_param, hydro_data.c.key_value,
                    hydro_data.c.data, hydro_data.c.id]).where(hydro_data.c.id.in_(r[0][1]))
                r = conn.execute(s)
                for p in r.fetchall():
                    old_profile[(p[0], p[1])] = (p[3], loads(p[2]))

            old_ids = {old_profile[k][0] for k in old_profile}
            ids_still_current = []
            for d in profile['data']:
                # only store actual meaningful data..
                del_keys = []
                for dd in d['data']:
                    if data_compress[dd](d['data'][dd]):
                        del_keys.append(dd)
                for key in del_keys:
                    del d['data'][key]


                data_key = (d['key_param'], d['key_value'])
                if data_key in old_profile:
                    if old_profile[data_key][1] == d['data']:
                        ids_still_current.append(old_profile[data_key][0])
                        continue

                d['current'] = True
                d['cruise_id'] = data["cruise_id"]
                profile_insert.append(d)
            ids_still_current = set(ids_still_current)
            ids_mark_not_current = old_ids - ids_still_current
            if len(ids_mark_not_current) is 0:
                #no new data for this profile, continue to the next one
                continue
            r = conn.execute(hydro_data.insert().\
                    returning(hydro_data.c.id).values(profile_insert))
            ids = ([e[0] for e in r.fetchall()])
            ids = ids + list(ids_still_current)
            del profile["data"]
            profile["cruise_id"] = data["cruise_id"]
            profile["samples"] = ids
            profile["current"] = True
            profile["previous_id"] = old_profile_id
            profile["date_z"] = date(
                    int(profile["date_z"][:4]),
                    int(profile["date_z"][4:6]),
                    int(profile["date_z"][6:8]))
            if "time_z" in profile:
                profile["time_z"] = time(
                        int(profile["time_z"][:2]),
                        int(profile["time_z"][2:]))
            conn.execute(profiles.insert().values(profile))
            conn.execute(profiles.update().where(profiles.c.id==old_profile_id).\
                    values(current=False))
            conn.execute(hydro_data.update().where(hydro_data.c.id.in_(
                ids_mark_not_current)).values(current=False))
        trans.commit()

def versioned_load_jsons(s):
    versioned_load(loads(s))

def versioned_load_jsonf(f):
    versioned_load(load(f))
