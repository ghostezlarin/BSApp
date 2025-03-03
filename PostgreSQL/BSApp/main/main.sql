--
-- PostgreSQL database dump
--

-- Dumped from database version 14.15 (Ubuntu 14.15-0ubuntu0.22.04.1)
-- Dumped by pg_dump version 17.2

-- Started on 2025-03-03 22:31:19

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 6 (class 2615 OID 16645)
-- Name: main; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA main;


ALTER SCHEMA main OWNER TO postgres;

--
-- TOC entry 3346 (class 0 OID 0)
-- Dependencies: 6
-- Name: SCHEMA main; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA main IS 'Main schema of project.';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 215 (class 1259 OID 16728)
-- Name: t_log_uploads; Type: TABLE; Schema: main; Owner: postgres
--

CREATE TABLE main.t_log_uploads (
    id bigint NOT NULL,
    status bigint DEFAULT 0 NOT NULL,
    date_insert timestamp without time zone DEFAULT LOCALTIMESTAMP NOT NULL,
    upload_id bigint NOT NULL
);


ALTER TABLE main.t_log_uploads OWNER TO postgres;

--
-- TOC entry 216 (class 1259 OID 16740)
-- Name: s_log_uploads; Type: SEQUENCE; Schema: main; Owner: postgres
--

CREATE SEQUENCE main.s_log_uploads
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE main.s_log_uploads OWNER TO postgres;

--
-- TOC entry 3347 (class 0 OID 0)
-- Dependencies: 216
-- Name: s_log_uploads; Type: SEQUENCE OWNED BY; Schema: main; Owner: postgres
--

ALTER SEQUENCE main.s_log_uploads OWNED BY main.t_log_uploads.id;


--
-- TOC entry 211 (class 1259 OID 16682)
-- Name: s_organizations_id; Type: SEQUENCE; Schema: main; Owner: postgres
--

CREATE SEQUENCE main.s_organizations_id
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE main.s_organizations_id OWNER TO postgres;

--
-- TOC entry 213 (class 1259 OID 16711)
-- Name: t_uploads; Type: TABLE; Schema: main; Owner: postgres
--

CREATE TABLE main.t_uploads (
    id bigint NOT NULL,
    status bigint DEFAULT 0 NOT NULL,
    date_insert timestamp without time zone DEFAULT LOCALTIMESTAMP NOT NULL,
    name character varying(254) NOT NULL,
    code_mnemonic character varying(8) NOT NULL,
    "desc" character varying(254) NOT NULL,
    organization_id bigint NOT NULL
);


ALTER TABLE main.t_uploads OWNER TO postgres;

--
-- TOC entry 214 (class 1259 OID 16720)
-- Name: s_uploads_id; Type: SEQUENCE; Schema: main; Owner: postgres
--

CREATE SEQUENCE main.s_uploads_id
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE main.s_uploads_id OWNER TO postgres;

--
-- TOC entry 3348 (class 0 OID 0)
-- Dependencies: 214
-- Name: s_uploads_id; Type: SEQUENCE OWNED BY; Schema: main; Owner: postgres
--

ALTER SEQUENCE main.s_uploads_id OWNED BY main.t_uploads.id;


--
-- TOC entry 212 (class 1259 OID 16689)
-- Name: t_organizations; Type: TABLE; Schema: main; Owner: postgres
--

CREATE TABLE main.t_organizations (
    id bigint DEFAULT nextval('main.s_organizations_id'::regclass) NOT NULL,
    status bigint DEFAULT 0 NOT NULL,
    date_insert timestamp without time zone DEFAULT LOCALTIMESTAMP NOT NULL,
    code_mnemonic character varying(8) NOT NULL,
    name_full character varying(254) NOT NULL,
    name_short character varying(254) NOT NULL,
    inn character varying(12) NOT NULL,
    ogrn character varying(15) NOT NULL,
    address_legal character varying(254) NOT NULL,
    address_location character varying(254) NOT NULL
);


ALTER TABLE main.t_organizations OWNER TO postgres;

--
-- TOC entry 3189 (class 2604 OID 16741)
-- Name: t_log_uploads id; Type: DEFAULT; Schema: main; Owner: postgres
--

ALTER TABLE ONLY main.t_log_uploads ALTER COLUMN id SET DEFAULT nextval('main.s_log_uploads'::regclass);


--
-- TOC entry 3186 (class 2604 OID 16721)
-- Name: t_uploads id; Type: DEFAULT; Schema: main; Owner: postgres
--

ALTER TABLE ONLY main.t_uploads ALTER COLUMN id SET DEFAULT nextval('main.s_uploads_id'::regclass);


--
-- TOC entry 3193 (class 2606 OID 16700)
-- Name: t_organizations organisations_code_mnemonic_key; Type: CONSTRAINT; Schema: main; Owner: postgres
--

ALTER TABLE ONLY main.t_organizations
    ADD CONSTRAINT organisations_code_mnemonic_key UNIQUE (code_mnemonic);


--
-- TOC entry 3195 (class 2606 OID 16703)
-- Name: t_organizations organisations_pkey; Type: CONSTRAINT; Schema: main; Owner: postgres
--

ALTER TABLE ONLY main.t_organizations
    ADD CONSTRAINT organisations_pkey PRIMARY KEY (id);


--
-- TOC entry 3199 (class 2606 OID 16734)
-- Name: t_log_uploads t_log_uploads_pkey; Type: CONSTRAINT; Schema: main; Owner: postgres
--

ALTER TABLE ONLY main.t_log_uploads
    ADD CONSTRAINT t_log_uploads_pkey PRIMARY KEY (id);


--
-- TOC entry 3197 (class 2606 OID 16719)
-- Name: t_uploads t_uploads_pkey; Type: CONSTRAINT; Schema: main; Owner: postgres
--

ALTER TABLE ONLY main.t_uploads
    ADD CONSTRAINT t_uploads_pkey PRIMARY KEY (id);


--
-- TOC entry 3200 (class 2606 OID 16723)
-- Name: t_uploads fk_organization_id; Type: FK CONSTRAINT; Schema: main; Owner: postgres
--

ALTER TABLE ONLY main.t_uploads
    ADD CONSTRAINT fk_organization_id FOREIGN KEY (organization_id) REFERENCES main.t_organizations(id) NOT VALID;


--
-- TOC entry 3349 (class 0 OID 0)
-- Dependencies: 3200
-- Name: CONSTRAINT fk_organization_id ON t_uploads; Type: COMMENT; Schema: main; Owner: postgres
--

COMMENT ON CONSTRAINT fk_organization_id ON main.t_uploads IS 'Foreign key for organization id.';


--
-- TOC entry 3201 (class 2606 OID 16735)
-- Name: t_log_uploads fk_upload_id; Type: FK CONSTRAINT; Schema: main; Owner: postgres
--

ALTER TABLE ONLY main.t_log_uploads
    ADD CONSTRAINT fk_upload_id FOREIGN KEY (upload_id) REFERENCES main.t_uploads(id);


-- Completed on 2025-03-03 22:31:26

--
-- PostgreSQL database dump complete
--

