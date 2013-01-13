
#include "segmenter.h"

#include <stdlib.h>
#include <memory.h>

void Groups_init(Segmenter_Groups *pGroups) {
	pGroups->groupsCount = 0;
	pGroups->_allocatedGroupsCount = 0;		
	pGroups->groups = 0;
}

void Groups_delete(Segmenter_Groups *pGroups) {
	unsigned int i;
	if ( pGroups->_allocatedGroupsCount ) {
		for ( i = 0; i < pGroups->groupsCount; ++i ) {
			free(pGroups->groups[i]);
		}
		free(pGroups->groups);
	}
}

void Group_init(Segmenter_Group **ppGroup, uint64_t start, uint64_t end,uint64_t id) {
	(*ppGroup) = malloc(sizeof(Segmenter_Group));
	(*ppGroup)->start = start;
	(*ppGroup)->end = end;
	(*ppGroup)->idsCount = 1;
	(*ppGroup)->ids = malloc(sizeof(uint64_t));
	(*ppGroup)->ids[0] = id;
}

Segmenter_Group* Groups_addNew(Segmenter_Groups *pGroups, uint64_t start, uint64_t  end,uint64_t id) {
	Segmenter_Group* group;
	Group_init(&group,start,end,id);
	++pGroups->groupsCount;
	if ( pGroups->_allocatedGroupsCount < pGroups->groupsCount ) {
		pGroups->_allocatedGroupsCount = pGroups->groupsCount;
		pGroups->groups = realloc(pGroups->groups,sizeof(Segmenter_Group*)*pGroups->_allocatedGroupsCount);
	}
	pGroups->groups[pGroups->groupsCount-1] = group;
	return group;
}

void Group_delete(Segmenter_Group* group) {	
	free(group->ids);	
	free(group);
}

void Groups_remove(Segmenter_Groups *pGroups, unsigned int i) {	
	unsigned int j;	
	Group_delete(pGroups->groups[i]);	
	--pGroups->groupsCount;
	if ( i < pGroups->groupsCount ) {		
		for ( j = i; j < pGroups->groupsCount; ++j ) {
			pGroups->groups[j] = pGroups->groups[j+1];			
		}
		//memmove(pGroups->groups + i,pGroups->groups + i + 1,sizeof(Segmenter_Group*) * (pGroups->groupsCount - i));	
	}	
}



void Group_append(Segmenter_Group *pGroup, uint64_t  end,uint64_t id) {		
	pGroup->end = end;
	++pGroup->idsCount;	
	pGroup->ids = realloc(pGroup->ids, sizeof(uint64_t)*pGroup->idsCount);	
	pGroup->ids[pGroup->idsCount-1] = id;
}

void Group_appendGroup(Segmenter_Group *dest, Segmenter_Group *src) {		
	dest->end = src->end;
	dest->idsCount += src->idsCount;
	dest->ids = realloc(dest->ids, sizeof(uint64_t)*dest->idsCount);
	memcpy(dest->ids + dest->idsCount - src->idsCount,src->ids,sizeof(uint64_t)*src->idsCount);
}


void Groups_insertWay(Segmenter_Groups *pGroups, uint64_t start, uint64_t  end,uint64_t id) {	
	Segmenter_Group* currentGroup = 0;
	unsigned int i;
	for ( i = 0; i < pGroups->groupsCount; ++i ) {	
		if ( start == pGroups->groups[i]->end ) {
			Group_append(pGroups->groups[i],end,id);
			currentGroup = pGroups->groups[i];			
			break;
		}
	}	
	if ( !currentGroup ) {		
		currentGroup = Groups_addNew(pGroups, start, end, id);		
	}	
	for ( i = 0; i < pGroups->groupsCount; ++i ) {	
		//printf("%lld %lld -- %lld %lld\n",currentGroup->start,currentGroup->end,pGroups->groups[i]->start,pGroups->groups[i]->end);		
		if ( currentGroup->end == pGroups->groups[i]->start && pGroups->groups[i] != currentGroup ) {
			Group_appendGroup(currentGroup,pGroups->groups[i]);
			//printf("%lld %lld\n",currentGroup->start,currentGroup->end);		
			Groups_remove(pGroups,i);
			break;
		}		
	}	
	//printf("\n\n");
}
