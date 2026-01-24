/* Copyright Abandoned 1996, 1999, 2001 MySQL AB
   This file is public domain and comes with NO WARRANTY of any kind */

/* Version numbers for protocol & mysqld */

#ifndef _mysql_version_h
#define _mysql_version_h
#ifdef _CUSTOMCONFIG_
#include <custom_conf.h>
#else
#define PROTOCOL_VERSION		
#define MYSQL_SERVER_VERSION		""
#define MYSQL_BASE_VERSION		"mysqld-"
#define MYSQL_SERVER_SUFFIX_DEF		""
#define FRM_VER				
#define MYSQL_VERSION_ID		
#define MYSQL_PORT			
#define MYSQL_PORT_DEFAULT		
#define MYSQL_UNIX_ADDR			""
#define MYSQL_CONFIG_NAME		"my"
#define MYSQL_COMPILATION_COMMENT	""

/* mysqld compile time options */
#endif /* _CUSTOMCONFIG_ */

#ifndef LICENSE
#define LICENSE				GPL
#endif /* LICENSE */

#endif /* _mysql_version_h */
