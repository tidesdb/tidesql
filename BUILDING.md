# Building TideSQL

TideSQL is a fork of MySQL Server 5.1 (Facebook fork) with TidesDB as an available storage engine.

## Prerequisites

### System Requirements

- **Operating System**: Linux (Debian/Ubuntu, RHEL/CentOS, Arch), macOS
- **Compiler**: GCC 7.0+ or Clang 6.0+ (with C++17 support)
- **Build Tools**: autoconf, automake, libtool, bison 3.x, perl

### Required Dependencies

#### TidesDB Library

TidesDB must be installed before building TideSQL:

```bash
# Clone TidesDB
git clone https://github.com/tidesdb/tidesdb
cd tidesdb

# Build and install
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
sudo cmake --install build
sudo ldconfig
```

#### Compression Libraries

TidesDB requires compression libraries:

```bash
# Debian/Ubuntu
sudo apt update
sudo apt install -y libzstd-dev liblz4-dev libsnappy-dev

# Fedora/RHEL/CentOS
sudo dnf install libzstd-devel lz4-devel snappy-devel

# Arch Linux
sudo pacman -S zstd lz4 snappy

# macOS (Homebrew)
brew install zstd lz4 snappy
```

#### MySQL Build Dependencies

```bash
# Debian/Ubuntu
sudo apt install -y build-essential cmake autoconf automake libtool \
    libncurses5-dev libssl-dev bison pkg-config

# Fedora/RHEL/CentOS
sudo dnf install gcc gcc-c++ cmake autoconf automake libtool \
    ncurses-devel openssl-devel bison

# macOS
brew install cmake autoconf automake libtool openssl ncurses bison
```

## Building TideSQL

### Option 1: Autotools Build (Recommended)

```bash
cd tidesql

# Generate configure script using MySQL's autorun script
./BUILD/autorun.sh

# Configure with TidesDB plugin
# Note: Use -Wno-narrowing and -fpermissive for modern GCC compatibility
./configure \
    --prefix=/usr/local/tidesql \
    --with-plugins=tidesdb,myisam,heap,csv \
    CXXFLAGS="-I/usr/local/include -Wno-narrowing -fpermissive" \
    LDFLAGS="-L/usr/local/lib -ltidesdb -lzstd -llz4 -lsnappy"

# Regenerate sql_yacc.cc for bison 3.x compatibility
cd sql
bison -y -p MYSQL -d --defines=sql_yacc.h -o sql_yacc.cc sql_yacc.yy
sed -i 's/#define yyerror         MYSQLerror/#define yyerror(ctx, msg) MYSQLerror_impl(msg)/' sql_yacc.cc
cd ..

# Build the main server
make -j$(nproc)

# Build the TidesDB plugin with proper linking
# The plugin must be linked with --no-as-needed to include libtidesdb
cd storage/tidesdb
g++ -DHAVE_CONFIG_H -I. -I../../include -I../../regex -I../../sql \
    -DMYSQL_DYNAMIC_PLUGIN -I/usr/local/include \
    -Wno-narrowing -fpermissive -fno-exceptions -fno-rtti -fPIC -shared \
    -o .libs/ha_tidesdb.so ha_tidesdb.cc \
    -L/usr/local/lib -Wl,-rpath,/usr/local/lib -Wl,--no-as-needed \
    -ltidesdb -lzstd -llz4 -lsnappy
cd ../..

# Install (optional)
sudo make install
```

### Regenerating Error Message Files

If you encounter errors about missing error messages (641 vs 645), regenerate them:

```bash
cd extra
./comp_err --charset=../sql/share/charsets \
    --in_file=../sql/share/errmsg.txt \
    --out_dir=../sql/share/ \
    --header_file=../include/mysqld_error.h \
    --name_file=../include/mysqld_ername.h \
    --state_file=../include/sql_state.h
```

### Option 2: CMake Build (Windows Only)

**Note:** The CMake build system in MySQL 5.1 is designed for Windows. For Linux/macOS, use the autotools build above.

```powershell
# Windows with Visual Studio
cd tidesql

# Run configure.js first to generate win/configure.data
cscript win\configure.js

mkdir build
cd build

cmake .. -G "Visual Studio 16 2019" -A x64
cmake --build . --config Release
```

### Build Options

| Option | Description |
|--------|-------------|
| `--with-plugins=tidesdb` | Include TidesDB storage engine |
| `--with-debug` | Enable debug build |
| `--with-ssl` | Enable SSL support |
| `--prefix=/path` | Installation directory |

## Post-Build Setup

### 1. Initialize Data Directory

```bash
# Create data directory
mkdir -p /tmp/tidesql-test/data

# Initialize system tables (from source directory)
perl scripts/mysql_install_db --no-defaults --srcdir=$(pwd) --datadir=/tmp/tidesql-test/data --force
```

### 2. Start the Server

```bash
# Start mysqld with TidesDB plugin directory
# LD_LIBRARY_PATH is required for libtidesdb
LD_LIBRARY_PATH=/usr/local/lib ./sql/mysqld --no-defaults \
    --datadir=/tmp/tidesql-test/data \
    --basedir=$(pwd) \
    --language=$(pwd)/sql/share/english \
    --socket=/tmp/tidesql-test.sock \
    --port=3307 \
    --plugin-dir=$(pwd)/storage/tidesdb/.libs &
```

### 3. Install TidesDB Plugin

```bash
# Install the TidesDB storage engine plugin (first time only)
./client/mysql --socket=/tmp/tidesql-test.sock -u root \
    -e "INSTALL PLUGIN tidesdb SONAME 'ha_tidesdb.so';"
```

### 4. Verify Installation

```bash
./client/mysql --socket=/tmp/tidesql-test.sock -u root -e "SHOW ENGINES;"
```

### Production Installation

After `make install`, create `/usr/local/tidesql/my.cnf`:

```ini
[mysqld]
basedir = /usr/local/tidesql
datadir = /usr/local/tidesql/data
socket = /tmp/tidesql.sock
port = 3307
language = /usr/local/tidesql/share/english
plugin-dir = /usr/local/tidesql/lib/plugin

# Load TidesDB plugin at startup
plugin-load = tidesdb=ha_tidesdb.so

# Optional: Make TidesDB the default engine
# default-storage-engine = TidesDB

[client]
socket = /tmp/tidesql.sock
port = 3307
```

Then start with:

```bash
/usr/local/tidesql/bin/mysqld_safe --defaults-file=/usr/local/tidesql/my.cnf &
```

## Verifying the Build

```bash
# Connect to TideSQL
./client/mysql --socket=/tmp/tidesql-test.sock -u root

# Check TidesDB is available
mysql> SHOW ENGINES;
+------------+---------+------------------------------------------------------+
| Engine     | Support | Comment                                              |
+------------+---------+------------------------------------------------------+
| TidesDB    | YES     | TidesDB LSM-based storage engine with ACID transactions |
| MyISAM     | DEFAULT | Default engine as of MySQL 3.23 with great performance  |
| MEMORY     | YES     | Hash based, stored in memory                            |
| CSV        | YES     | CSV storage engine                                      |
+------------+---------+------------------------------------------------------+

# Test creating a TidesDB table
mysql> CREATE DATABASE test_tidesdb;
mysql> USE test_tidesdb;
mysql> CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100)) ENGINE=TidesDB;
mysql> INSERT INTO t1 VALUES (1, 'Hello TidesDB');
mysql> SELECT * FROM t1;
+----+---------------+
| id | name          |
+----+---------------+
|  1 | Hello TidesDB |
+----+---------------+
```

## Troubleshooting

### TidesDB library not found

```bash
# Add library path when starting server
LD_LIBRARY_PATH=/usr/local/lib ./sql/mysqld ...

# Or update ldconfig permanently
echo "/usr/local/lib" | sudo tee /etc/ld.so.conf.d/tidesdb.conf
sudo ldconfig
```

### Plugin load error: undefined symbol

The TidesDB plugin must be linked with `--no-as-needed` to include libtidesdb:

```bash
cd storage/tidesdb
g++ -DHAVE_CONFIG_H -I. -I../../include -I../../regex -I../../sql \
    -DMYSQL_DYNAMIC_PLUGIN -I/usr/local/include \
    -Wno-narrowing -fpermissive -fno-exceptions -fno-rtti -fPIC -shared \
    -o .libs/ha_tidesdb.so ha_tidesdb.cc \
    -L/usr/local/lib -Wl,-rpath,/usr/local/lib -Wl,--no-as-needed \
    -ltidesdb -lzstd -llz4 -lsnappy
```

Verify linking:
```bash
ldd storage/tidesdb/.libs/ha_tidesdb.so | grep tidesdb
# Should show: libtidesdb.so => /usr/local/lib/libtidesdb.so
```

### Bison 3.x compatibility errors

If you see parser errors, regenerate sql_yacc.cc:

```bash
cd sql
bison -y -p MYSQL -d --defines=sql_yacc.h -o sql_yacc.cc sql_yacc.yy
sed -i 's/#define yyerror         MYSQLerror/#define yyerror(ctx, msg) MYSQLerror_impl(msg)/' sql_yacc.cc
```

### Error message file mismatch

If you see "had only 641 error messages, but it should contain at least 645":

```bash
cd extra
./comp_err --charset=../sql/share/charsets \
    --in_file=../sql/share/errmsg.txt \
    --out_dir=../sql/share/ \
    --header_file=../include/mysqld_error.h \
    --name_file=../include/mysqld_ername.h \
    --state_file=../include/sql_state.h
```

### Permission denied

```bash
sudo chown -R $USER:$USER /tmp/tidesql-test/data
chmod 750 /tmp/tidesql-test/data
```

## Development Build

For development with debug symbols and sanitizers:

```bash
./configure \
    --prefix=/usr/local/tidesql-debug \
    --with-plugins=tidesdb \
    --with-debug \
    CXXFLAGS="-g -O0 -fsanitize=address -I/usr/local/include" \
    LDFLAGS="-fsanitize=address -L/usr/local/lib"

make -j$(nproc)
```

## Running Tests

```bash
cd mysql-test
./mysql-test-run.pl --suite=tidesdb
```
