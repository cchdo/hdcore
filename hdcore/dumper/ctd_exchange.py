from sqlalchemy.sql import select, case, literal_column
from sqlalchemy import Float, func
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


def data_table_query_builder(requested_ids, p_map, *, precisions=None):
    if precisions:
        if len(requested_ids) != len(precisions):
            raise BaseException("TODO: list of params and precisions must be the same")
    select_list = []
    requested_set = set(requested_ids)
    if len(requested_set) != len(requested_ids):
        raise BaseException("TODO: make duplicate id exception")
    for i, r_id in enumerate(requested_ids):
        FILL_VALUE = "-999"
        p = p_map[r_id]
        if p["parameter"].quality_class:
            q_for = set(p["quality_for"])
            data_id = q_for.intersection(requested_set)
            if len(data_id) is not 1:
                raise BaseException("TODO: make error for quality col with no or ambigious data col")
            data_id = list(data_id)[0]
            c = case([
                (hydro_data.c.data.has_key(str(r_id)), hydro_data.c.data[str(r_id)].astext),
                (hydro_data.c.data.has_key(str(data_id)), p["data"])
                ], else_= p["nodata"])
            select_list.append(c)
            continue
        else:
            if precisions:
                if precisions[i] > 0:
                    FILL_VALUE = FILL_VALUE + "." + "0" * precisions[i]
            c = case([
                (hydro_data.c.data.has_key(str(r_id)), hydro_data.c.data[str(r_id)].astext)
                ], else_= FILL_VALUE)
            select_list.append(c)
    return select_list

def get_col_precision_query(requested_ids):
    select_list = []
    requested_set = set(requested_ids)
    if len(requested_set) != len(requested_ids):
        raise BaseException("TODO: make duplicate id exception")
    for r_id in requested_ids:
        pos = func.position(literal_column("'.'").in_([hydro_data.c.data[str(r_id)].astext]))
        length = func.char_length(hydro_data.c.data[str(r_id)].astext)
        c = func.max(
        case([
            (pos > 0,
                length - pos)
            ], else_=0))
        select_list.append(c)
    return select_list


def profile_data_in_excahnge(conn, profile, p_map, output_params, param_ids=None):
    p_id = profile.id
    param_names = []
    param_units = []
    print_ids = []
    for param in output_params:
        print_ids.append(param)
        param_names.append(p_map[param]["parameter"].name)
        if p_map[param]["parameter"].units_repr:
            param_units.append(p_map[param]["parameter"].units_repr)
        else:
            param_units.append("")

    s = get_col_precision_query(print_ids)
    subq = select([func.unnest(profiles.c.samples)]).where(profiles.c.id==p_id)
    s = select(s).where(
        hydro_data.c.id.in_(subq)
        )
    r = conn.execute(s)
    precisions = r.fetchone()

    output = ""
    output += (",".join(param_names) + "\n")
    output += (",".join(param_units) + "\n")

    s = data_table_query_builder(print_ids, p_map, precisions=precisions)
    subq = select([func.unnest(profiles.c.samples)]).where(profiles.c.id==p_id)
    s = select(s).where(
        hydro_data.c.id.in_(subq)
        ).order_by(hydro_data.c.data['1'].cast(Float))
    r = conn.execute(s)
    f_str = ""
    for i, p in enumerate(print_ids):
        length = p_map[p]["parameter"].format_string[:-1]
        if i is 0:
            f_str = "{:>" + length + "s}"
        else:
            f_str = f_str + ",{:>" + length + "s}"
    for d in r.fetchall():
        output += f_str.format(*d) + "\n"
    output += "END_DATA\n"
    return output

def ctd_profile_headers(conn, profile, h_ids, p_map, cruise):
    first_sample = profile.samples[0]
    s = select([hydro_data.c.data]).where(hydro_data.c.id==first_sample)
    r = conn.execute(s)
    data = r.fetchone().data
    data = loads(data)
    headers = []
    for id in h_ids:
        if str(id) in data:
            headers.append(p_map[id]["parameter"].name + " = " + data[str(id)])

    output = ""
    output += "CTD,20140814CCHSIODDB\n"
    comments = profile.comments.strip().splitlines()
    for comment in comments:
        output += "#" + comment + "\n"
    output += "NUMBER_HEADERS = " + str(len(headers)+1) + "\n"
    output += "\n".join(headers)
    output += "\n"
    return output
