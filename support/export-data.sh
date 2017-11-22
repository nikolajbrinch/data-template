#!/bin/bash

pg_dump --dbname=postgresql://postgres:postgres@localhost:5432/ml --clean > /mnt/data/datafiles/datafile.sql
