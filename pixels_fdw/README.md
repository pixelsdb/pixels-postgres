# pixels-postgres
The PostgreSQL FDW for Pixels.

export PIXELS_FDW_SRC=/path/to/yours

You'd better put this folder to your postgresql source code directory. (postgresql-xx.xx/contrib)

Please modify this line "shared_preload_libraries = 'pixels_fdw'	# (change requires restart)" in "postgresql.conf"

make pull
make deps
make
sudo make install

CREATE EXTENSION pixels_fdw;

CREATE SERVER pixels_server FOREIGN DATA WRAPPER pixels_fdw;  

create foreign table example (
    id           int,
    name         varchar,
    birthday     date,
    score        decimal(15, 2)
)
server pixels_server
options (
    filename '|/path1|/path2|/path3|',
    filters  'id > 1 & score < 90'
);