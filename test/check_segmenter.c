#include <check.h>
#include <stdlib.h>
#include <stdio.h>
#include "../src/segmenter.h"


START_TEST (check_segmenter) {
	Segmenter_Groups x;
	int i, j;
	char str[255];
	
	
	
	Groups_init(&x);
	Groups_insertWay(&x,300,400,1);
	Groups_insertWay(&x,400,500,2);
	Groups_insertWay(&x,500,600,3);
	
	if ( x.groupsCount != 1 ) {
		sprintf(str,
			"Wrong count of groups. (%d <> 1) ",
			x.groupsCount
		);
		fail(str);
	}
	
	if ( x.groups[0]->idsCount != 3 ) {
		sprintf(str,
			"Wrong count of ways in group. (%d <> 3) ",
			x.groups[0]->idsCount
		);
		fail(str);
	}
	
	if ( x.groups[0]->ids[0] != 1 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 3) ",
			x.groups[0]->ids[0]
		);
		fail(str);
	}
	
	if ( x.groups[0]->ids[1] != 2 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 2) ",
			x.groups[0]->ids[1]
		);
		fail(str);
	}
	
	if ( x.groups[0]->ids[2] != 3 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 1) ",
			x.groups[0]->ids[2]
		);
		fail(str);
	}
	
	Groups_delete(&x);
	
	
	
	Groups_init(&x);
	Groups_insertWay(&x,500,600,1);
	Groups_insertWay(&x,400,500,2);
	Groups_insertWay(&x,300,400,3);
	
	if ( x.groupsCount != 1 ) {
		sprintf(str,
			"Wrong count of groups. (%d <> 1) ",
			x.groupsCount
		);
		fail(str);
	}
	
	if ( x.groups[0]->idsCount != 3 ) {
		sprintf(str,
			"Wrong count of ways in group. (%d <> 3) ",
			x.groups[0]->idsCount
		);
		fail(str);
	}
	
	if ( x.groups[0]->ids[0] != 3 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 3) ",
			x.groups[0]->ids[0]
		);
		fail(str);
	}
	
	if ( x.groups[0]->ids[1] != 2 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 2) ",
			x.groups[0]->ids[1]
		);
		fail(str);
	}
	
	if ( x.groups[0]->ids[2] != 1 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 1) ",
			x.groups[0]->ids[2]
		);
		fail(str);
	}
	
	Groups_delete(&x);
	
	
	
	Groups_init(&x);
	Groups_insertWay(&x,500,600,1);
	Groups_insertWay(&x,400,500,2);
	Groups_insertWay(&x,300,400,3);
	Groups_insertWay(&x,300,600,4); //Oposit direction
	
	if ( x.groupsCount != 1 ) {
		sprintf(str,
			"Wrong count of groups. (%d <> 1) ",
			x.groupsCount
		);
		fail(str);
	}
	
	if ( x.groups[0]->idsCount != 4 ) {
		sprintf(str,
			"Wrong count of ways in group. (%d <> 4) ",
			x.groups[0]->idsCount
		);
		fail(str);
	}
	
	if ( x.groups[0]->ids[0] != 3 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 3) ",
			x.groups[0]->ids[0]
		);
		fail(str);
	}
	
	if ( x.groups[0]->ids[1] != 2 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 2) ",
			x.groups[0]->ids[1]
		);
		fail(str);
	}
	
	if ( x.groups[0]->ids[2] != 1 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 1) ",
			x.groups[0]->ids[2]
		);
		fail(str);
	}
	
	if ( x.groups[0]->ids[3] != 4 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 4) ",
			x.groups[0]->ids[3]
		);
		fail(str);
	}
	
	Groups_delete(&x);
	
	
	
	
	Groups_init(&x);
	Groups_insertWay(&x,500,600,1);	
	Groups_insertWay(&x,400,500,2);
	Groups_insertWay(&x,800,900,3);
	Groups_insertWay(&x,900,1000,4);
	Groups_insertWay(&x,700,800,5);
	Groups_insertWay(&x,300,400,6);
	
	if ( x.groupsCount != 2 ) {
		sprintf(str,
			"Wrong count of groups. (%d <> 2) ",
			x.groupsCount
		);
		fail(str);
	}
	
	if ( x.groups[0]->idsCount != 3 ) {
		sprintf(str,
			"Wrong count of ways in group. (%d <> 3) ",
			x.groups[0]->idsCount
		);
		fail(str);
	}
	
	if ( x.groups[1]->ids[0] != 6 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 6) ",
			x.groups[0]->ids[0]
		);
		fail(str);
	}
	
	if ( x.groups[1]->ids[1] != 2 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 2) ",
			x.groups[0]->ids[1]
		);
		fail(str);
	}
	
	if ( x.groups[1]->ids[2] != 1 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 1) ",
			x.groups[0]->ids[2]
		);
		fail(str);
	}
	
	
	if ( x.groups[0]->ids[0] != 5 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 5) ",
			x.groups[1]->ids[0]
		);
		fail(str);
	}
	
	if ( x.groups[0]->ids[1] != 3 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 3) ",
			x.groups[1]->ids[1]
		);
		fail(str);
	}
	
	if ( x.groups[0]->ids[2] != 4 ) { 
		sprintf(str,
			"Wrong way id. (%d <> 4) ",
			x.groups[1]->ids[2]
		);
		fail(str);
	}
	
	Groups_delete(&x);
	
}
END_TEST


Suite *segmenter_suite (void) {
	Suite *s = suite_create ("segmenter");
	TCase *tc_core = tcase_create ("Core");	
	tcase_add_test (tc_core, check_segmenter);		
	suite_add_tcase (s, tc_core);

	return s;
}

main (void) {
	int number_failed;
	Suite *s = segmenter_suite ();
	SRunner *sr = srunner_create (s);
	srunner_run_all (sr, CK_NORMAL);
	number_failed = srunner_ntests_failed (sr);
	srunner_free (sr);
	return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
