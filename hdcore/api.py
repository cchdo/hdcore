from flask import Flask
from flask import jsonify
app = Flask(__name__)

from sqlalchemy.sql import select

from ujson import loads

from model.db import cruises, profiles, hydro_data, pending_profiles
from model.db import engine

@app.route("/api/v0/cruise")
def api_v0_cruise():
    with engine.connect() as conn:
        s = select([cruises.c.id, cruises.c.expocode])
        result = conn.execute(s)
        r = {c[0]: c[1] for c in result}
        return jsonify(r)

@app.route("/api/v0/cruise/<int:id>/profiles")
def api_v0_cruise_proiles(id):
    with engine.connect() as conn:
        s = select([profiles.c.id]).where(profiles.c.cruise_id==id)
        result = conn.execute(s)
        r = {"profiles": [r[0] for r in result.fetchall()]}
        return jsonify(r)

@app.route("/diff_test")
def diff_test():
    with engine.connect() as conn:
        s = select([pending_profiles]).where(
                pending_profiles.c.id==3
                )
        result = conn.execute(s)
        pending = result.fetchone()

    return str(pending.data)

@app.route("/")
def index():
    return "hdcore API interface"

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
