#!/bin/bash

pg_dump --dbname=postgresql://postgres:postgres@localhost:5432/ml > /mnt/data/datafiles/datafile.sql
