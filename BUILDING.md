# Building TideSQL

TideSQL is a fork of MySQL Server 5.1 with TidesDB as the default storage engine.

## Prerequisites

### System Requirements

- **Operating System**: Linux (Debian/Ubuntu, RHEL/CentOS, Arch), macOS, or FreeBSD
- **Compiler**: GCC 7.0+ or Clang 6.0+
- **Build Tools**: CMake 3.25+, autoconf, automake, libtool

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

# Generate configure script (if building from git)
autoreconf -i

# Configure with TidesDB as the default engine
./configure \
    --prefix=/usr/local/tidesql \
    --with-plugins=tidesdb,myisam,heap,csv \
    --with-mysqld-ldflags=-ltidesdb \
    CXXFLAGS="-I/usr/local/include" \
    LDFLAGS="-L/usr/local/lib"

# Build
make -j$(nproc)

# Install
sudo make install
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

## Post-Installation Setup

### 1. Create Data Directory

```bash
sudo mkdir -p /usr/local/tidesql/data
sudo chown -R mysql:mysql /usr/local/tidesql/data
```

### 2. Initialize Database

```bash
cd /usr/local/tidesql

# Initialize system tables
./scripts/mysql_install_db --user=mysql --datadir=/usr/local/tidesql/data
```

### 3. Create Configuration File

Create `/usr/local/tidesql/my.cnf`:

```ini
[mysqld]
basedir = /usr/local/tidesql
datadir = /usr/local/tidesql/data
socket = /tmp/tidesql.sock
port = 3307

# TidesDB is the default engine
default-storage-engine = TidesDB

# TidesDB configuration
tidesdb_data_dir = /usr/local/tidesql/data/tidesdb
tidesdb_flush_threads = 2
tidesdb_compaction_threads = 2
tidesdb_block_cache_size = 67108864
tidesdb_write_buffer_size = 67108864
tidesdb_enable_compression = ON
tidesdb_enable_bloom_filter = ON

[client]
socket = /tmp/tidesql.sock
port = 3307
```

### 4. Start the Server

```bash
# Start mysqld
/usr/local/tidesql/bin/mysqld_safe --defaults-file=/usr/local/tidesql/my.cnf &

# Or run directly
/usr/local/tidesql/bin/mysqld --defaults-file=/usr/local/tidesql/my.cnf
```

### 5. Secure Installation

```bash
/usr/local/tidesql/bin/mysql_secure_installation --socket=/tmp/tidesql.sock
```

## Verifying the Installation

```bash
# Connect to TideSQL
/usr/local/tidesql/bin/mysql -S /tmp/tidesql.sock -u root -p

# Check default engine
mysql> SHOW VARIABLES LIKE 'default_storage_engine';
+------------------------+---------+
| Variable_name          | Value   |
+------------------------+---------+
| default_storage_engine | TidesDB |
+------------------------+---------+

# Check TidesDB is available
mysql> SHOW ENGINES;
+------------+---------+--------------------------------------------+
| Engine     | Support | Comment                                    |
+------------+---------+--------------------------------------------+
| TidesDB    | DEFAULT | TidesDB LSM-based storage engine with ACID |
| MyISAM     | YES     | Default engine as of MySQL 3.23            |
| MEMORY     | YES     | Hash based, stored in memory               |
| CSV        | YES     | CSV storage engine                         |
+------------+---------+--------------------------------------------+
```

## Troubleshooting

### TidesDB library not found

```bash
# Add library path
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

# Or update ldconfig
echo "/usr/local/lib" | sudo tee /etc/ld.so.conf.d/tidesdb.conf
sudo ldconfig
```

### Plugin load error

Ensure TidesDB was compiled with the same compiler version as TideSQL.

### Permission denied

```bash
sudo chown -R mysql:mysql /usr/local/tidesql/data
sudo chmod 750 /usr/local/tidesql/data
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
