from sqlalchemy.sql import select
from ujson import loads

from hdcore.model.db import engine, parameters, quality, profiles, hydro_data

def make_param_quality_dict(conn):
    s = parameters.select()
    r = conn.execute(s)
    params = r.fetchall()
    p_map = {}
    for p in params:
        p_map[p.id] = {}
        p_map[p.id]["parameter"] = p
        p_map[p.id]["quality"] = None
        p_map[p.id]["data"] = None
        p_map[p.id]["nodata"] = None
        p_map[p.id]["quality_for"] = []

    # For the quality parameters, make a list of what they can be the quality
    # of. To be used when writing the a quality column.
    for key in p_map:
        if p_map[key]["parameter"].quality:
            p_map[p_map[key]["parameter"].quality]["quality_for"].append(key)

    s = quality.select()
    r = conn.execute(s)
    qual = r.fetchall()
    q_map = {}
    for q in qual:
        if q.quality_class not in q_map:
            q_map[q.quality_class] = {}
        if q.default_data_present:
            q_map[q.quality_class]["data"] = q.value
        if q.default_data_missing:
            q_map[q.quality_class]["nodata"] = q.value
    for key in p_map:
        quality_class = p_map[key]["parameter"].quality_class
        if quality_class:
            p_map[key]["data"] = q_map[quality_class]["data"]
            p_map[key]["nodata"] = q_map[quality_class]["nodata"]
    return p_map
            

def data_to_list(data_dict, requested_ids, p_map, *, precision_list = False):
    l = []
    requested_set = set(requested_ids)
    if len(requested_set) != len(requested_ids):
        raise BaseException("TODO: make duplicate id exception")
    for r_id in requested_ids:
        r_id_s = str(r_id)
        if r_id_s in data_dict:
            l.append(data_dict[r_id_s])
            continue
        p = p_map[r_id]
        if p["parameter"].quality_class:
            q_for = set(p["quality_for"])
            data_id = q_for.intersection(requested_set)
            if len(data_id) is not 1:
                raise BaseException("TODO: make error for quality col with no data col")
            data_id = list(data_id)[0]
            if str(data_id) in data_dict:
                l.append(p["data"])
            else:
                l.append(p["nodata"])
        else:
            l.append(None)

    if precision_list:
        p_l = []
        for el in l:
            if not el:
                p_l.append(0)
                continue
            numbers = el.split(".")
            if len(numbers) is 1:
                p_l.append(0)
                continue
            if len(numbers) is 2:
                p_l.append(len(numbers[1]))
                continue
            else:
                raise ValueError("number with two decimal points")
        return l, p_l
    return l


FILL_VALUE = "-999"
def profile_to_exchange(p_id, param_ids=None):
    with engine.connect() as conn:
        s = profiles.select().where(profiles.c.id==p_id)
        r = conn.execute(s)
        profile = r.fetchone()
        p_map = make_param_quality_dict(conn)

        ctd_headers = [21,22,23,24,25,26] #TODO put this data in the db

        param_names = []
        param_units = []
        print_ids = []
        for param in profile.parameters:
            if param in ctd_headers:
                continue
            print_ids.append(param)
            param_names.append(p_map[param]["parameter"].name)
            if p_map[param]["parameter"].units_repr:
                param_units.append(p_map[param]["parameter"].units_repr)
            else:
                param_units.append("")
        print(",".join(param_names))
        print(",".join(param_units))

        s = select([hydro_data.c.data]).where(
                hydro_data.c.id.in_(profile.samples))
        r = conn.execute(s)
        for d in r.fetchall():
            d = loads(d.data)
            print(data_to_list(d, print_ids, p_map,))
