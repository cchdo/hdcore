--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: postgres; Type: COMMENT; Schema: -; Owner: abarna
--

COMMENT ON DATABASE postgres IS 'default administrative connection database';


--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- Name: adminpack; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS adminpack WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION adminpack; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION adminpack IS 'administrative functions for PostgreSQL';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: cruises; Type: TABLE; Schema: public; Owner: abarna; Tablespace: 
--

CREATE TABLE cruises (
    id integer NOT NULL,
    expocode text
);


ALTER TABLE cruises OWNER TO abarna;

--
-- Name: cruises_id_seq; Type: SEQUENCE; Schema: public; Owner: abarna
--

CREATE SEQUENCE cruises_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE cruises_id_seq OWNER TO abarna;

--
-- Name: cruises_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: abarna
--

ALTER SEQUENCE cruises_id_seq OWNED BY cruises.id;


--
-- Name: hydro_data; Type: TABLE; Schema: public; Owner: abarna; Tablespace: 
--

CREATE TABLE hydro_data (
    id integer NOT NULL,
    data jsonb,
    key_param integer NOT NULL,
    key_value text NOT NULL,
    current boolean DEFAULT true NOT NULL,
    cruise_id integer NOT NULL,
    CONSTRAINT key_param_in_data CHECK ((data ? (key_param)::text))
);


ALTER TABLE hydro_data OWNER TO abarna;

--
-- Name: hydro_data_id_seq; Type: SEQUENCE; Schema: public; Owner: abarna
--

CREATE SEQUENCE hydro_data_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE hydro_data_id_seq OWNER TO abarna;

--
-- Name: hydro_data_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: abarna
--

ALTER SEQUENCE hydro_data_id_seq OWNED BY hydro_data.id;


--
-- Name: parameters; Type: TABLE; Schema: public; Owner: abarna; Tablespace: 
--

CREATE TABLE parameters (
    id integer NOT NULL,
    type text NOT NULL,
    name text NOT NULL,
    units text,
    units_repr text,
    quality integer,
    canonical_id integer,
    format_string text,
    quality_class text
);


ALTER TABLE parameters OWNER TO abarna;

--
-- Name: COLUMN parameters.type; Type: COMMENT; Schema: public; Owner: abarna
--

COMMENT ON COLUMN parameters.type IS 'This would be something like "cchdo" or "cf" or "argo".';


--
-- Name: COLUMN parameters.name; Type: COMMENT; Schema: public; Owner: abarna
--

COMMENT ON COLUMN parameters.name IS 'The how the parameter is represented in a datafile, for eample "CTDPRS" would be a "cchdo" parameter for the CTD measured pressure';


--
-- Name: COLUMN parameters.units; Type: COMMENT; Schema: public; Owner: abarna
--

COMMENT ON COLUMN parameters.units IS 'The base representation of the units in terms that something could parse, some examples: ''dbar'', ''umol/kg''. Things like "PSS-78" are dimentionless and would have no units.';


--
-- Name: COLUMN parameters.units_repr; Type: COMMENT; Schema: public; Owner: abarna
--

COMMENT ON COLUMN parameters.units_repr IS 'The representation of the unit in a datafile, used for writing and reading of various data formats';


--
-- Name: COLUMN parameters.quality; Type: COMMENT; Schema: public; Owner: abarna
--

COMMENT ON COLUMN parameters.quality IS 'If this parameter is a quality code, which parameter is it a quality for';


--
-- Name: COLUMN parameters.canonical_id; Type: COMMENT; Schema: public; Owner: abarna
--

COMMENT ON COLUMN parameters.canonical_id IS 'Points to the parameter def that should be used when writing/returning the data, the presences of data here means that this definition exists for reading badly formatted or old format files';


--
-- Name: parameters_id_seq; Type: SEQUENCE; Schema: public; Owner: abarna
--

CREATE SEQUENCE parameters_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE parameters_id_seq OWNER TO abarna;

--
-- Name: parameters_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: abarna
--

ALTER SEQUENCE parameters_id_seq OWNED BY parameters.id;


--
-- Name: profiles; Type: TABLE; Schema: public; Owner: abarna; Tablespace: 
--

CREATE TABLE profiles (
    id integer NOT NULL,
    cruise_id integer NOT NULL,
    samples integer[],
    current boolean DEFAULT true NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    previous_id integer,
    parameters integer[],
    station text,
    "cast" text
);


ALTER TABLE profiles OWNER TO abarna;

--
-- Name: COLUMN profiles.parameters; Type: COMMENT; Schema: public; Owner: abarna
--

COMMENT ON COLUMN profiles.parameters IS 'This is whatever was in the file, for reconstructing data files with columns that have no data (all -999 or flag 9)';


--
-- Name: profiles_id_seq; Type: SEQUENCE; Schema: public; Owner: abarna
--

CREATE SEQUENCE profiles_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE profiles_id_seq OWNER TO abarna;

--
-- Name: profiles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: abarna
--

ALTER SEQUENCE profiles_id_seq OWNED BY profiles.id;


--
-- Name: quality; Type: TABLE; Schema: public; Owner: abarna; Tablespace: 
--

CREATE TABLE quality (
    id integer NOT NULL,
    quality_class text,
    value text,
    description text,
    has_data boolean,
    default_data_present boolean DEFAULT false,
    default_data_missing boolean DEFAULT false
);


ALTER TABLE quality OWNER TO abarna;

--
-- Name: COLUMN quality.has_data; Type: COMMENT; Schema: public; Owner: abarna
--

COMMENT ON COLUMN quality.has_data IS 'E.g. a woce bottle flag 1 should have no data';


--
-- Name: COLUMN quality.default_data_present; Type: COMMENT; Schema: public; Owner: abarna
--

COMMENT ON COLUMN quality.default_data_present IS 'If there is no flag data, and data is present, assume this flag';


--
-- Name: COLUMN quality.default_data_missing; Type: COMMENT; Schema: public; Owner: abarna
--

COMMENT ON COLUMN quality.default_data_missing IS 'If there is no data AND no flag present, assume this flag';


--
-- Name: quality_id_seq; Type: SEQUENCE; Schema: public; Owner: abarna
--

CREATE SEQUENCE quality_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE quality_id_seq OWNER TO abarna;

--
-- Name: quality_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: abarna
--

ALTER SEQUENCE quality_id_seq OWNED BY quality.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: abarna
--

ALTER TABLE ONLY cruises ALTER COLUMN id SET DEFAULT nextval('cruises_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: abarna
--

ALTER TABLE ONLY hydro_data ALTER COLUMN id SET DEFAULT nextval('hydro_data_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: abarna
--

ALTER TABLE ONLY parameters ALTER COLUMN id SET DEFAULT nextval('parameters_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: abarna
--

ALTER TABLE ONLY profiles ALTER COLUMN id SET DEFAULT nextval('profiles_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: abarna
--

ALTER TABLE ONLY quality ALTER COLUMN id SET DEFAULT nextval('quality_id_seq'::regclass);


--
-- Name: cruise_pk; Type: CONSTRAINT; Schema: public; Owner: abarna; Tablespace: 
--

ALTER TABLE ONLY cruises
    ADD CONSTRAINT cruise_pk PRIMARY KEY (id);


--
-- Name: data_pk; Type: CONSTRAINT; Schema: public; Owner: abarna; Tablespace: 
--

ALTER TABLE ONLY hydro_data
    ADD CONSTRAINT data_pk PRIMARY KEY (id);


--
-- Name: parameter_pk; Type: CONSTRAINT; Schema: public; Owner: abarna; Tablespace: 
--

ALTER TABLE ONLY parameters
    ADD CONSTRAINT parameter_pk PRIMARY KEY (id);


--
-- Name: profiles_pk; Type: CONSTRAINT; Schema: public; Owner: abarna; Tablespace: 
--

ALTER TABLE ONLY profiles
    ADD CONSTRAINT profiles_pk PRIMARY KEY (id);


--
-- Name: quality_pk; Type: CONSTRAINT; Schema: public; Owner: abarna; Tablespace: 
--

ALTER TABLE ONLY quality
    ADD CONSTRAINT quality_pk PRIMARY KEY (id);


--
-- Name: unique_name_unit; Type: CONSTRAINT; Schema: public; Owner: abarna; Tablespace: 
--

ALTER TABLE ONLY parameters
    ADD CONSTRAINT unique_name_unit UNIQUE (name, units);


--
-- Name: canonical_self_ref_fk; Type: FK CONSTRAINT; Schema: public; Owner: abarna
--

ALTER TABLE ONLY parameters
    ADD CONSTRAINT canonical_self_ref_fk FOREIGN KEY (canonical_id) REFERENCES parameters(id);


--
-- Name: cruise_fk; Type: FK CONSTRAINT; Schema: public; Owner: abarna
--

ALTER TABLE ONLY hydro_data
    ADD CONSTRAINT cruise_fk FOREIGN KEY (cruise_id) REFERENCES cruises(id);


--
-- Name: cruise_fk; Type: FK CONSTRAINT; Schema: public; Owner: abarna
--

ALTER TABLE ONLY profiles
    ADD CONSTRAINT cruise_fk FOREIGN KEY (cruise_id) REFERENCES cruises(id);


--
-- Name: key_param_fk; Type: FK CONSTRAINT; Schema: public; Owner: abarna
--

ALTER TABLE ONLY hydro_data
    ADD CONSTRAINT key_param_fk FOREIGN KEY (key_param) REFERENCES parameters(id);


--
-- Name: parameter_quality_self_ref; Type: FK CONSTRAINT; Schema: public; Owner: abarna
--

ALTER TABLE ONLY parameters
    ADD CONSTRAINT parameter_quality_self_ref FOREIGN KEY (quality) REFERENCES parameters(id) ON DELETE CASCADE;


--
-- Name: public; Type: ACL; Schema: -; Owner: abarna
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM abarna;
GRANT ALL ON SCHEMA public TO abarna;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

