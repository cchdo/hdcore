import csv
import os
import xml.etree.ElementTree as et
from models import Session
from models import Platform

session = Session()

ship_codes = os.path.join('inits','platforms', 'SHIPC_20130816_c_1.445.txt')

platforms = []

#TODO Make this be able to "update" the list of platform codes
with open(ship_codes, 'rb') as sc:
    skip_header = sc.readline()
    reader = csv.reader(sc, delimiter='\t')
    for a in reader:
        if len(a) > 3:
            a[2] = "".join(a[2:])
        b = "<d>".decode('ascii')+a[2].decode('iso-8859-1').replace('&',
        '&amp;')+"</d>".decode('ascii')
        e = et.fromstring(b.encode('UTF-8'))
        title = e.find('title')
        country = e.find('country')
        notes = e.find('notes')
        if title is not None:
            title = title.text

        if country is not None:
            country = country.text

        if notes is not None:
            notes = notes.text
        
        platforms.append(Platform(a[1].decode('iso-8859-1'),
            code=a[0].decode('iso-8859-1'), country=country,
            title=title, notes = notes))

session.add_all(platforms)
session.commit()
