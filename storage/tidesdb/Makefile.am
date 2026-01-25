# Copyright (c) 2026 TidesDB
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

# Called from the top level Makefile

MYSQLDATAdir =          $(localstatedir)
MYSQLSHAREdir =         $(pkgdatadir)
MYSQLBASEdir=           $(prefix)
MYSQLLIBdir=            $(pkglibdir)
pkgplugindir =		$(pkglibdir)/plugin
INCLUDES =              -I$(top_srcdir)/include -I$(top_builddir)/include \
			-I$(top_srcdir)/regex \
			-I$(top_srcdir)/sql \
                        -I$(srcdir)
WRAPLIBS=

LDADD = -ltidesdb -lzstd -llz4 -lsnappy

DEFS =                  @DEFS@

noinst_HEADERS =	ha_tidesdb.h

EXTRA_LTLIBRARIES =	ha_tidesdb.la
pkgplugin_LTLIBRARIES =	@plugin_tidesdb_shared_target@
ha_tidesdb_la_LDFLAGS =	-module -rpath $(pkgplugindir) -ltidesdb -lzstd -llz4 -lsnappy
ha_tidesdb_la_CXXFLAGS=	$(AM_CXXFLAGS) -DMYSQL_DYNAMIC_PLUGIN
ha_tidesdb_la_CFLAGS =	$(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
ha_tidesdb_la_SOURCES =	ha_tidesdb.cc


EXTRA_LIBRARIES =	libtidesdb_engine.a
noinst_LIBRARIES =	@plugin_tidesdb_static_target@
libtidesdb_engine_a_CXXFLAGS =	$(AM_CXXFLAGS)
libtidesdb_engine_a_CFLAGS =	$(AM_CFLAGS)
libtidesdb_engine_a_SOURCES=	ha_tidesdb.cc

EXTRA_DIST =		CMakeLists.txt plug.in building.md c.md
# Don't update the files from bitkeeper
%::SCCS/s.%
