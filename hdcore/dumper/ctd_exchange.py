from sqlalchemy.sql import select
from ujson import loads

from hdcore.model.db import engine, parameters, quality, profiles, hydro_data


FILL_VALUE = "-999"
def profile_to_exchange(p_id, param_ids=None):
    with engine.connect() as conn:
        s = profiles.select().where(profiles.c.id==p_id)
        r = conn.execute(s)
        profile = r.fetchone()

        s = parameters.select()
        r = conn.execute(s)
        params = r.fetchall()
        p_map = {}
        for p in params:
            p_map[p.id] = p

        ctd_headers = [21,22,23,24,25,26] #TODO put this data in the db

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
                q_map[q.quality_class]["notdata"] = q.value

        param_names = []
        param_units = []
        param_writers_order = []
        param_writers = {}
        for param in profile.parameters:
            if param in ctd_headers:
                continue
            param_writers_order.append(param)
            param_names.append(p_map[param].name)
            if p_map[param].units_repr:
                param_units.append(p_map[param].units_repr)
            else:
                param_units.append("")

            if not p_map[param].quality_class:
                param_writers[param] = (lambda x, p=str(param): x.get(p,FILL_VALUE))
            if p_map[param].quality in profile.parameters:
                q = p_map[param].quality
                dp = q_map[p_map[q].quality_class]["data"]
                dm = q_map[p_map[q].quality_class]["notdata"]
                param_writers[p_map[param].quality] = lambda x, p=str(q), d=str(param), dp=dp, dm=dm: str(x.get(p, dp if d in x else dm))
                print(p_map[p_map[param].quality])
        print(",".join(param_names))
        print(",".join(param_units))


        s = select([hydro_data.c.data]).where(
                hydro_data.c.id.in_(profile.samples))
        r = conn.execute(s)
        for d in r.fetchall():
            d = loads(d.data)
            print(",".join([param_writers[f](d) for f in param_writers_order]))
        

