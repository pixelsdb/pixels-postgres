/* contrib/pixels_fdw/pixels_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pixels_fdw" to load this pixels. \quit

CREATE FUNCTION pixels_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION pixels_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pixels_fdw
  HANDLER pixels_fdw_handler
  VALIDATOR pixels_fdw_validator;
