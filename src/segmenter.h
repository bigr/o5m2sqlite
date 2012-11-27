#ifndef __SEGMENTER_H__
#define __SEGMENTER_H__

#include <stdint.h>

typedef struct {
	uint64_t start;
	uint64_t end;
	unsigned int idsCount;
	uint64_t* ids;
} Segmenter_Group;

typedef struct {
	unsigned int groupsCount;
	unsigned int _allocatedGroupsCount;
	Segmenter_Group** groups; 	
} Segmenter_Groups;


void Groups_init(Segmenter_Groups *pGroups);
void Groups_delete(Segmenter_Groups *pGroups);
void Groups_insertWay(Segmenter_Groups *pGroups, uint64_t start, uint64_t  end,uint64_t id);

#endif //__SEGMENTER_H__
