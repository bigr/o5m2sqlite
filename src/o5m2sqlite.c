#include "o5mreader.h"
#include "segmenter.h"

#include <string.h>
#include <float.h>
#include <uthash.h>




#ifdef SPATIALITE_AMALGAMATION
    #include <spatialite/sqlite3.h>    
#else
    #include <sqlite3.h>
#endif

#ifndef SPATIALITE_EXTENSION
    #include <spatialite.h>    
#endif

#include <spatialite/gaiageo.h>    


#pragma pack(push)
#pragma pack(1)
struct NodeCache {
    int64_t id; 
    int32_t lon;
    int32_t lat;
    UT_hash_handle hh;
};

struct WayCache {
    int64_t id;
    union {
		int64_t from;
		struct {
			int32_t lon;
			int32_t lat;
		} fromLL;
	};
	union {
		int64_t to;
		struct {
			int32_t lon;
			int32_t lat;
		} toLL;
	};
    
    UT_hash_handle hh;
};
#pragma pack(pop)

void delete_nodeCache(struct NodeCache* nodeCache) {
  struct NodeCache *nodeCacheItem, *tmp;

  HASH_ITER(hh, nodeCache, nodeCacheItem, tmp) {
    HASH_DEL(nodeCache,nodeCacheItem);
    free(nodeCacheItem);
  }
}

void delete_wayCache(struct WayCache* wayCache) {
  struct WayCache *wayCacheItem, *tmp;

  HASH_ITER(hh, wayCache, wayCacheItem, tmp) {
    HASH_DEL(wayCache,wayCacheItem);
    free(wayCacheItem);
  }
}


int insertTag(O5mreader* reader, sqlite3* db, sqlite3_stmt* selectTagStmt, sqlite3_stmt* insertTagStmt, sqlite3_stmt* insertNodeTagStmt,int64_t id) {
	O5mreaderIterateRet ret2;
	char *key, *val;
	int64_t tagId;
	int ret = 0;
	
	
	while ( (ret2 = o5mreader_iterateTags(reader, &key, &val)) == O5MREADER_ITERATE_RET_NEXT ) {		
		if ( 0 == strcmp(key,"source") || 0 == strcmp(key,"created_by") || 0 == strcmp(key,"converted_by") )
			continue;
		sqlite3_bind_text(selectTagStmt, 1, key,-1,0);
		sqlite3_bind_text(selectTagStmt, 2, val,-1,0);
		if (sqlite3_step(selectTagStmt) == SQLITE_DONE) {													
			sqlite3_bind_text(insertTagStmt, 1, key,-1,0);
			sqlite3_bind_text(insertTagStmt, 2, val,-1,0);
			if (sqlite3_step(insertTagStmt) != SQLITE_DONE) {
				fprintf(stderr,"Commit tag failed: %s\n",sqlite3_errmsg(db));
				sqlite3_reset(insertTagStmt);							
				return -1;
			}
			else {
				tagId = sqlite3_last_insert_rowid(db);
			}
			
			sqlite3_reset(insertTagStmt);
		}
		else {
			tagId = (int64_t)sqlite3_column_int(selectTagStmt,0);					
		}
		
		sqlite3_reset(selectTagStmt);
		
		sqlite3_bind_int(insertNodeTagStmt, 1, id);
		sqlite3_bind_int(insertNodeTagStmt, 2, tagId);
		if (sqlite3_step(insertNodeTagStmt) != SQLITE_DONE) {
			fprintf(stderr,"Commit node tag failed!\n");
			return -1;
		}
		sqlite3_reset(insertNodeTagStmt);
		ret++;
	}
	
	return ret;
}

int main(int argc, char **argv) {
	FILE* f;
	O5mreader* reader;
	O5mreaderDataset ds;
	O5mreaderIterateRet ret;
	sqlite3 *db;
	uint64_t nodes, ways, rels;
	
	int64_t wayNodeId;
	int64_t refId;
	
	int32_t oldLon,oldLat;
		
	int i, j, jj;	
	struct NodeCache *nodeCacheItem;
	struct NodeCache *nodeCache = NULL;
	struct WayCache *wayCacheItem;
	struct WayCache *wayCache = NULL;
	
	Segmenter_Groups outer, inner;
	
	int hasTags;
	char* role;
	int32_t roleId;
	uint8_t relType;		
	
	int first;
	gaiaLinestringPtr linestring;
	gaiaGeomCollPtr geom;
	int32_t waypoints[65536];
	unsigned char *blob;
	unsigned int blob_size;
	
	unsigned int segment;
	unsigned int object;
	unsigned int index;
	
	const char * waySql = "REPLACE INTO  way (id,geom,closed) VALUES (?1,?2,?3)";
	const char * relSql = "REPLACE INTO  rel (id) VALUES (?1)";
	const char * nodeSql = "REPLACE INTO  node(id,geom) VALUES (?1,?2)";	
	const char * insertTagSql = "INSERT INTO tag (key,value) VALUES (?1,?2)";
	const char * insertRoleSql = "INSERT INTO role (name) VALUES (?1)";
	const char * insertNodeTagSql = "REPLACE INTO node_tag (node_id,tag_id) VALUES (?1,?2)";
	const char * insertWayTagSql = "REPLACE INTO way_tag (way_id,tag_id) VALUES (?1,?2)";
	const char * insertRelTagSql = "REPLACE INTO rel_tag (rel_id,tag_id) VALUES (?1,?2)";
	
	const char * insertRelNodeSql = "REPLACE INTO rel_node (rel_id,node_id,role_id) VALUES (?1,?2,?3)";
	const char * insertRelWaySql = "REPLACE INTO rel_way (rel_id,way_id,role_id) VALUES (?1,?2,?3)";
	const char * insertRelChildSql = "REPLACE INTO rel_child (rel_id,child_id,role_id) VALUES (?1,?2,?3)";
	
	const char * outerSql = "REPLACE INTO rel_outer (rel_id,object,segment,way_id) VALUES (?1,?2,?3,?4)";
	const char * innerSql = "REPLACE INTO rel_inner (rel_id,object,segment,way_id) VALUES (?1,?2,?3,?4)";
	
	const char * selectTagSql = "SELECT id FROM tag WHERE key = ?1 AND value = ?2 LIMIT 1";
	const char * selectRoleSql = "SELECT id FROM role WHERE name = ?1 LIMIT 1";
	const char * selectNodeSql = "SELECT lon,lat FROM node WHERE id = ?1 LIMIT 1";
	sqlite3_stmt *nodeStmt, *wayStmt, *relStmt, *selectNodeStmt, *updateNodeStmt, 
		*insertTagStmt, *insertRoleStmt, *selectTagStmt, *selectRoleStmt, *insertNodeTagStmt, 
		*insertWayTagStmt, *insertRelTagStmt, *insertRelNodeStmt, 
		*insertRelWayStmt, *insertRelChildStmt, *outerStmt, *innerStmt, *stmt;
	
#ifndef SPATIALITE_EXTENSION
	spatialite_init (0);
#endif	
	sqlite3_open(argc < 2 ? "nodes.sqlite" : argv[1],&db);	

#ifdef SPATIALITE_EXTENSION
	sqlite3_enable_load_extension (db, 1);
	sqlite3_exec(db, "SELECT load_extension('libspatialite.so')", 0, 0, 0);
#endif
	
	//Geometry_init(&wkb,4326,2/*LINESTRING*/);
	
	
	sqlite3_open(argc < 2 ? "osm.sqlite" : argv[1],&db);
	
	
		
	sqlite3_exec(db, "PRAGMA synchronous=OFF", 0, 0, 0);
	sqlite3_exec(db, "PRAGMA count_changes=OFF", 0, 0, 0);
	sqlite3_exec(db, "PRAGMA journal_mode=MEMORY", 0, 0, 0);
	sqlite3_exec(db, "PRAGMA temp_store=MEMORY", 0, 0, 0);
	sqlite3_exec(db, "SELECT InitSpatialMetadata()", 0, 0, 0);	
	sqlite3_exec(db, "CREATE TABLE tag (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,key VARCHAR(512),value VARCHAR(512))",0,0,0);	
	sqlite3_exec(db, "CREATE INDEX i__tag__key ON tag(key)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__tag__value ON tag(value)",0,0,0);	
	sqlite3_exec(db, "CREATE TABLE node (id INTEGER PRIMARY KEY NOT NULL)",0,0,0);
	sqlite3_exec(db, "SELECT AddGeometryColumn('node', 'geom', 4326, 'POINT', 'XY')",0,0,0);		
	sqlite3_exec(db, "CREATE TABLE node_tag (node_id INTEGER NOT NULL,tag_id INTEGER NOT NULL)",0,0,0);	
	sqlite3_exec(db, "CREATE INDEX i__node_tag__node_id ON node_tag(node_id)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__node_tag__tag_id ON node_tag(tag_id)",0,0,0);	
	sqlite3_exec(db, "CREATE TABLE way (id INTEGER PRIMARY KEY NOT NULL, closed INTEGER NOT NULL)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__way__closed ON way(closed)",0,0,0);	
	sqlite3_exec(db, "SELECT AddGeometryColumn('way', 'geom', 4326, 'LINESTRING', 'XY')",0,0,0);	
	sqlite3_exec(db, "CREATE TABLE way_tag (way_id INTEGER NOT NULL,tag_id INTEGER NOT NULL)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__way_tag__way_id ON way_tag(way_id)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__way_tag__tag_id ON way_tag(tag_id)",0,0,0);		
	sqlite3_exec(db, "CREATE TABLE way_node (way_id BIGINT NOT NULL, node_id BIGINT NOT NULL)",0,0,0);
	sqlite3_exec(db, "CREATE UNIQUE INDEX u__way_has_node__id ON way_has_node(way_id,node_id);",0,0,0);
	sqlite3_exec(db, "CREATE TABLE rel (id INTEGER PRIMARY KEY NOT NULL)",0,0,0);
	sqlite3_exec(db, "CREATE TABLE role (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,name VARCHAR(512))",0,0,0);	
	sqlite3_exec(db, "CREATE UNIQUE INDEX i__role__name ON role(name)",0,0,0);
	sqlite3_exec(db, "CREATE TABLE rel_tag (rel_id INTEGER NOT NULL,tag_id INTEGER NOT NULL)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__rel_tag__way_id ON rel_tag(rel_id)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__rel_tag__tag_id ON rel_tag(tag_id)",0,0,0);
	sqlite3_exec(db, "CREATE TABLE rel_node (rel_id INTEGER NOT NULL,node_id INTEGER NOT NULL, role_id INTEGER NOT NULL)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__rel_node__node_id ON rel_tag(node_id)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__rel_node__role_id ON rel_tag(role_id)",0,0,0);
	sqlite3_exec(db, "CREATE TABLE rel_way (rel_id INTEGER NOT NULL,way_id INTEGER NOT NULL, role_id INTEGER NOT NULL, object INTEGER, segment INTEGER,idx INTEGER)",0,0,0);	
	sqlite3_exec(db, "CREATE INDEX i__rel_way__way_id ON rel_tag(way_id)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__rel_way__role_id ON rel_tag(role_id)",0,0,0);	
	sqlite3_exec(db, "CREATE TABLE rel_child (rel_id INTEGER NOT NULL,child_id INTEGER NOT NULL, role_id INTEGER NOT NULL)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__rel_rel__child_id ON rel_tag(child_id)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__rel_rel__role_id ON rel_tag(role_id)",0,0,0);		
	
	sqlite3_exec(db, "CREATE TABLE rel_outer (rel_id INTEGER NOT NULL,object INTEGER NOT NULL, segment INTEGER NOT NULL, way_id INTEGER NOT NULL)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__rel_outer__rel_id ON rel_outer(rel_id)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__rel_outer__way_id ON rel_outer(way_id)",0,0,0);
	sqlite3_exec(db, "CREATE TABLE rel_inner (rel_id INTEGER NOT NULL,object INTEGER NOT NULL, segment INTEGER NOT NULL, way_id INTEGER NOT NULL)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__rel_inner__rel_id ON rel_inner(rel_id)",0,0,0);
	sqlite3_exec(db, "CREATE INDEX i__rel_inner__way_id ON rel_inner(way_id)",0,0,0);		
	
	
	sqlite3_exec(db, "CREATE UNIQUE INDEX u__node_tag__node_id__tag_id ON node_tag(node_id,tag_id)",0,0,0);	
	sqlite3_exec(db, "CREATE UNIQUE INDEX u__way_tag__way_id__tag_id ON way_tag(way_id,tag_id)",0,0,0);	
	sqlite3_exec(db, "CREATE UNIQUE INDEX u__rel_tag__rel_id__tag_id ON rel_tag(rel_id,tag_id)",0,0,0);	
	
	sqlite3_exec(db, "CREATE UNIQUE INDEX u__rel_node__rel_id__node_id ON rel_node(rel_id,node_id)",0,0,0);	
	sqlite3_exec(db, "CREATE UNIQUE INDEX u__rel_way__rel_id__way_id ON rel_way(rel_id,way_id)",0,0,0);	
	sqlite3_exec(db, "CREATE UNIQUE INDEX u__rel_child__rel_id__child_id ON rel_child(rel_id,child_id)",0,0,0);	
	
	sqlite3_exec(db, "CREATE UNIQUE INDEX u__rel_outer__rel_id__way_id ON rel_outer(rel_id,way_id)",0,0,0);	
	sqlite3_exec(db, "CREATE UNIQUE INDEX u__rel_inner__rel_id__way_id ON rel_inner(rel_id,way_id)",0,0,0);	
	
	
	sqlite3_exec(db,
		"CREATE VIEW ntag AS "
		"  SELECT NT.node_id AS id, T.key AS k,T.value AS v "
		"  FROM node_tag NT "
		"  JOIN tag T ON T.id = NT.tag_id "		
	,0,0,0);
	
	sqlite3_exec(db,
		"CREATE VIEW wtag AS "
		"  SELECT WT.way_id AS id, T.key AS k,T.value AS v "
		"  FROM way_tag WT "
		"  JOIN tag T ON T.id = WT.tag_id "		
	,0,0,0);
	
	sqlite3_exec(db,
		"CREATE VIEW rtag AS "
		"  SELECT RT.rel_id AS id, T.key AS k,T.value AS v "
		"  FROM rel_tag RT "
		"  JOIN tag T ON T.id = RT.tag_id "		
	,0,0,0);
	
	sqlite3_exec(db,
		"CREATE VIEW rtag AS "
		"  SELECT RT.rel_id AS id, T.key AS k,T.value AS v "
		"  FROM rel_tag RT "
		"  JOIN tag T ON T.id = RT.tag_id "

	,0,0,0);
	
	//sqlite3_exec(db, "SELECT CreateSpatialIndex('node', 'geom');",0,0,0);
	//sqlite3_exec(db, "SELECT CreateSpatialIndex('way', 'geom');",0,0,0);	
	
	
	sqlite3_exec(db, "BEGIN TRANSACTION",0,0,0);	
	
	
	sqlite3_prepare_v2(db, nodeSql, strlen(nodeSql), &nodeStmt, 0);
	sqlite3_prepare_v2(db, insertTagSql, strlen(insertTagSql), &insertTagStmt, 0);
	sqlite3_prepare_v2(db, insertRoleSql, strlen(insertRoleSql), &insertRoleStmt, 0);
	sqlite3_prepare_v2(db, insertNodeTagSql, strlen(insertNodeTagSql), &insertNodeTagStmt, 0);
	sqlite3_prepare_v2(db, insertWayTagSql, strlen(insertWayTagSql), &insertWayTagStmt, 0);
	sqlite3_prepare_v2(db, insertRelTagSql, strlen(insertRelTagSql), &insertRelTagStmt, 0);
	sqlite3_prepare_v2(db, insertRelNodeSql, strlen(insertRelNodeSql), &insertRelNodeStmt, 0);
	sqlite3_prepare_v2(db, insertRelWaySql, strlen(insertRelWaySql), &insertRelWayStmt, 0);
	sqlite3_prepare_v2(db, insertRelChildSql, strlen(insertRelChildSql), &insertRelChildStmt, 0);
	sqlite3_prepare_v2(db, selectTagSql, strlen(selectTagSql), &selectTagStmt, 0);
	sqlite3_prepare_v2(db, selectRoleSql, strlen(selectRoleSql), &selectRoleStmt, 0);
	sqlite3_prepare_v2(db, waySql, strlen(waySql), &wayStmt, 0);
	sqlite3_prepare_v2(db, relSql, strlen(relSql), &relStmt, 0);
	sqlite3_prepare_v2(db, selectNodeSql,strlen(selectNodeSql),&selectNodeStmt,0);	
	
	sqlite3_prepare_v2(db, outerSql,strlen(outerSql),&outerStmt,0);	
	sqlite3_prepare_v2(db, innerSql,strlen(innerSql),&innerStmt,0);	
	
	char str[255];
	
	f = argc < 3 ? stdin : fopen(argv[2],"r");
	if ( !f ) {
		fprintf(stderr,str,"Failed to open file");
		return -1;
	}
	if ( !o5mreader_open(&reader,f) ) {
		fprintf(stderr,"Failed to make reader: %s",
			o5mreader_strerror(reader->errCode)
		);
		return -1;
	}
	
	nodes = 0;
	ways = 0;
	rels = 0;		
	
	while( (ret = o5mreader_iterateDataSet(reader, &ds)) == O5MREADER_ITERATE_RET_NEXT ) {
		switch ( ds.type ) {
			case O5MREADER_DS_NODE:
				hasTags = insertTag(reader,db,selectTagStmt,insertTagStmt,insertNodeTagStmt,ds.id);
				
				nodeCacheItem = malloc(sizeof(struct NodeCache));
				nodeCacheItem->id = ds.id;
				nodeCacheItem->lon = ds.lon;
				nodeCacheItem->lat = ds.lat;				
				HASH_ADD_INT( nodeCache, id, nodeCacheItem );								
				
				if ( hasTags ) {		
					sqlite3_bind_int(nodeStmt, 1, ds.id);
					geom = gaiaAllocGeomColl();
					geom->Srid = 4326;
					gaiaAddPointToGeomColl(geom,(double)ds.lon*1E-7,(double)ds.lat*1E-7);
					//gaiaMbrGeomColl(geom);
					gaiaToSpatiaLiteBlobWkb(geom,&blob,&blob_size);
					
					sqlite3_bind_blob(nodeStmt, 2, blob,blob_size,free);
					
					gaiaFreeGeomColl(geom);			
					
					if (sqlite3_step(nodeStmt) != SQLITE_DONE) {						
						fprintf(stderr,"Insert node failed: %s\n",sqlite3_errmsg(db));
					}
					sqlite3_reset(nodeStmt);
				}
				
				
				if ( ret == O5MREADER_ITERATE_RET_ERR ) {
					fprintf(stderr,
						"Failed with error: '%s' (code: %d) '%s' while parsing node tags'.",
						reader->errCode,
						o5mreader_strerror(reader->errCode),
						reader->errMsg
					);
					return -1;
				}
				
				if ( ++nodes % 50000 == 0) {					
					printf("Nodes %lldK  Ways %lldK  Rels %lld        \r",nodes/1000,ways/1000,rels);
					fflush(stdout);					
				}
								
				break;
			case O5MREADER_DS_WAY:
				j = 0;				
				
				wayCacheItem = malloc(sizeof(struct WayCache));
				wayCacheItem->id = ds.id;
				wayCacheItem->from = 0LL;				
				wayCacheItem->to = 0LL;				
				
				first = 1;	
							
				while ( o5mreader_iterateNds(reader, &wayNodeId) != O5MREADER_ITERATE_RET_DONE ) {
					HASH_FIND_INT(nodeCache, &wayNodeId, nodeCacheItem);
					if ( first ) {
						wayCacheItem->fromLL.lon = nodeCacheItem->lon;
						wayCacheItem->fromLL.lat = nodeCacheItem->lat;			
						first = 0;
					}
					
					if ( nodeCacheItem ) {
						if ( j == 0 || ( oldLon != nodeCacheItem->lon || oldLat != nodeCacheItem->lat ) ) {
							waypoints[j++] = nodeCacheItem->lon;
							waypoints[j++] = nodeCacheItem->lat;
							
						}						
						
					}
					
					oldLon = nodeCacheItem->lon;
					oldLat = nodeCacheItem->lat;
					//printf(".");
				}
				//printf("%d: %d - ",j,ds.id);
				if ( j/2 >= 2 ) {			
					//printf("%d\n",wayNodeId);
					wayCacheItem->toLL.lon = nodeCacheItem->lon;
					wayCacheItem->toLL.lat = nodeCacheItem->lat;		
								
					HASH_ADD_INT( wayCache, id, wayCacheItem );	
																			
					linestring = gaiaAllocLinestring(j/2);
					for ( jj = 0; jj < j/2; ++jj ) {
						gaiaSetPoint(linestring->Coords,jj,(double)waypoints[2*jj]*1E-07,(double)waypoints[2*jj+1]*1E-07);
					}																		
					
					gaiaMbrLinestring(linestring);
					
					hasTags = insertTag(reader,db,selectTagStmt,insertTagStmt,insertWayTagStmt,ds.id);
					
					sqlite3_bind_int(wayStmt, 1, ds.id);
					//printf("%d\n",Geometry_getSize(&geo));
					
					//geom = gaiaFromWkb((unsigned char*)&wkb,Geometry_getSize(&geo));
					geom = gaiaAllocGeomColl();
					geom->Srid = 4326;
					gaiaInsertLinestringInGeomColl(geom,linestring);
					//gaiaMbrGeomColl(geom);
					gaiaToSpatiaLiteBlobWkb(geom,&blob,&blob_size);
					
					sqlite3_bind_blob(wayStmt, 2, blob,blob_size,free);
					sqlite3_bind_int(wayStmt, 3, wayCacheItem->to == wayCacheItem->from && j/2 >= 4);
					
					gaiaFreeGeomColl(geom);
					//gaiaFreeLinestring(linestring);
					
					if (sqlite3_step(wayStmt) != SQLITE_DONE) {
						fprintf(stderr,"Insert way failed: %s\n",sqlite3_errmsg(db));
					}
					sqlite3_reset(wayStmt);
					
					if ( ++ways % 5000 == 0) {
						printf("Nodes %lldK  Ways %lldK  Rels %lld        \r",nodes/1000,ways/1000,rels);
						fflush(stdout);					
					}					
					//printf("to: %d - %d\n",ds.id,wayCacheItem->to);					
				}
				else {
					free(wayCacheItem);
				}
				break;
			case O5MREADER_DS_REL:								
				
				Groups_init(&outer);
				Groups_init(&inner);
				
				while ( o5mreader_iterateRefs(reader, &refId , &relType , &role) == O5MREADER_ITERATE_RET_NEXT ) {
					
					sqlite3_bind_text(selectRoleStmt, 1, role,-1,0);					
					if (sqlite3_step(selectRoleStmt) == SQLITE_DONE) {																
						sqlite3_bind_text(insertRoleStmt, 1, role,-1,0);						
						if (sqlite3_step(insertRoleStmt) != SQLITE_DONE) {
							fprintf(stderr,"Commit role failed!\n");
							sqlite3_reset(insertRoleStmt);							
							return -1;
						}
						else {
							roleId = sqlite3_last_insert_rowid(db);
						}
						
						sqlite3_reset(insertRoleStmt);
					}
					else {
						roleId = (int32_t)sqlite3_column_int(selectRoleStmt,0);					
					}
					
					sqlite3_reset(selectRoleStmt);
					
					switch ( relType ) {
						case O5MREADER_DS_NODE:
							stmt = insertRelNodeStmt;
							break;
						case O5MREADER_DS_WAY:
							if ( 0 == strcmp(role,"outer") || 0 == strcmp(role,"exclave") ) {								
								wayCacheItem = NULL;
								HASH_FIND_INT(wayCache, &refId, wayCacheItem);
								if ( wayCacheItem ) {									
									//printf("%lld: [%d,%d] - [%d,%d]\n",wayCacheItem->id,wayCacheItem->fromLL.lon,wayCacheItem->fromLL.lat,wayCacheItem->toLL.lon,wayCacheItem->toLL.lat);
									Groups_insertWay(&outer,wayCacheItem->from,wayCacheItem->to,wayCacheItem->id);									
								}
							}
							if ( 0 == strcmp(role,"inner") || 0 == strcmp(role,"inclave") ) {
								HASH_FIND_INT(wayCache, &refId, wayCacheItem);
								if ( wayCacheItem ) {
									Groups_insertWay(&inner,wayCacheItem->from,wayCacheItem->to,wayCacheItem->id);									
								}
							}
							
							stmt = insertRelWayStmt;
							break;
						case O5MREADER_DS_REL:
							stmt = insertRelChildStmt;
							break;
					}														
					
					sqlite3_bind_int(stmt, 1, ds.id);
					sqlite3_bind_int(stmt, 2, refId);
					sqlite3_bind_int(stmt, 3, roleId);										
					
					if (sqlite3_step(stmt) != SQLITE_DONE) {
						fprintf(stderr,"Commit rel member failed!\n");
						return -1;
					}
					sqlite3_reset(stmt);
					
				}								

				for ( i = 0; i < outer.groupsCount; ++i ) {
					for ( j = 0; j < outer.groups[i]->idsCount; ++j ) {							
						sqlite3_bind_int(outerStmt, 1, ds.id);
						sqlite3_bind_int(outerStmt, 2, i+1);
						sqlite3_bind_int(outerStmt, 3, j+1);				
						sqlite3_bind_int(outerStmt, 4, outer.groups[i]->ids[j]);
						if (sqlite3_step(outerStmt) != SQLITE_DONE) {
							fprintf(stderr,"Commit outer failed!\n");
							return -1;
						}
						sqlite3_reset(outerStmt);
					}
				}
				
				for ( i = 0; i < inner.groupsCount; ++i ) {
					for ( j = 0; j < inner.groups[i]->idsCount; ++j ) {							
						sqlite3_bind_int(innerStmt, 1, ds.id);
						sqlite3_bind_int(innerStmt, 2, i+1);
						sqlite3_bind_int(innerStmt, 3, j+1);				
						sqlite3_bind_int(innerStmt, 4, inner.groups[i]->ids[j]);
						if (sqlite3_step(innerStmt) != SQLITE_DONE) {
							fprintf(stderr,"Commit inner failed!\n");
							return -1;
						}
						sqlite3_reset(innerStmt);
					}
				}				

				Groups_delete(&inner);
				Groups_delete(&outer);

				hasTags = insertTag(reader,db,selectTagStmt,insertTagStmt,insertRelTagStmt,ds.id);
				
				sqlite3_bind_int(relStmt, 1, ds.id);
				if (sqlite3_step(relStmt) != SQLITE_DONE) {
					fprintf(stderr,"Commit Failed!\n");
				}
				sqlite3_reset(relStmt);
				
				if ( ++rels % 100 == 0) {					
					printf("Nodes %lldK  Ways %lldK  Rels %lld        \r",nodes/1000,ways/1000,rels);
					fflush(stdout);					
				}
				
				break;
		}
		
		
	}				

	if ( argc > 3 && strcmp(argv[3],"notfinal") == 0 ) {
	}
	else {

		sqlite3_exec(db,"DROP TABLE polygon",0,0,0);

		sqlite3_exec(db,		
		"CREATE TABLE polygon AS "
		"SELECT id,geom,0 AS is_rel FROM way WHERE closed = 1 AND IsValid(geom) AND NOT IsEmpty(geom) AND NumPoints(geom) > 3 "
		"UNION "
		"SELECT id,(CASE WHEN inner IS NULL THEN outer ELSE Difference(outer,inner) END) AS geom,is_rel FROM ( "
		"SELECT R.id, "		
		"  (SELECT Collect(geom) AS geom FROM "
		"	(SELECT Coalesce(Polygonize(geom),LineMerge(Collect(geom)),Collect(geom)) AS geom FROM rel_outer RO "
		"	JOIN way W ON W.id = RO.way_id AND IsValid(W.geom) AND NOT IsEmpty(W.geom) AND NumPoints(W.geom) > 1 "
		"	WHERE RO.rel_id = R.id "
		"	GROUP BY rel_id, object "
		"	ORDER BY segment) "
		" ) AS outer, "
		"  (SELECT Coalesce(Polygonize(geom),LineMerge(Collect(geom)),Collect(geom)) AS geom FROM "
		"	(SELECT Polygonize(geom) AS geom FROM rel_inner RI "
		"	JOIN way W ON W.id = RI.way_id AND IsValid(W.geom) AND NOT IsEmpty(W.geom) AND NumPoints(W.geom) > 1 "
		"	WHERE RI.rel_id = R.id "
		"	GROUP BY rel_id, object "
		"	ORDER BY segment) "
		"  ) AS inner, "
		"  1 AS is_rel  "
		"FROM rel R) "
		,0,0,0);
	}

	sqlite3_exec(db,"COMMIT TRANSACTION",0,0,0);	
	sqlite3_exec(db, "VACUUM",0,0,0);
	sqlite3_finalize(outerStmt);
	sqlite3_finalize(innerStmt);	
	sqlite3_finalize(nodeStmt);	
	sqlite3_finalize(wayStmt);
	sqlite3_finalize(relStmt);
	sqlite3_finalize(selectNodeStmt);
	sqlite3_finalize(insertTagStmt);
	sqlite3_finalize(insertRoleStmt);
	sqlite3_finalize(insertNodeTagStmt);
	sqlite3_finalize(insertWayTagStmt);
	sqlite3_finalize(insertRelTagStmt);
	sqlite3_finalize(insertRelNodeStmt);
	sqlite3_finalize(insertRelWayStmt);
	sqlite3_finalize(insertRelChildStmt);
	sqlite3_finalize(selectTagStmt);
	sqlite3_finalize(selectRoleStmt);
	sqlite3_close(db);	
	
	delete_nodeCache(nodeCache);
	delete_wayCache(wayCache);		
	
	if ( f != stdin ) {
		fclose(f);
	}
}
