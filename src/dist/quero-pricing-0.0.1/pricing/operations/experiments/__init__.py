from pricing.operations.experiments.helper import *

from pricing.operations.experiments import level_1_kind_1_courses_1, \
	 level_1_kind_1_courses_2,level_1_kind_3_courses_1,level_1_kind_3_courses_2, \
	 level_1_kind_8,level_7_kind_1,level_7_kind_3_8_offered_1,\
	 level_7_kind_3_8_offered_2,qap

experiments_list = [
	level_1_kind_1_courses_1.get_experiment(),
	level_1_kind_1_courses_2.get_experiment(),
	level_1_kind_3_courses_1.get_experiment(),
	level_1_kind_3_courses_2.get_experiment(),
	level_1_kind_8.get_experiment(),
	level_7_kind_1.get_experiment(),
	level_7_kind_3_8_offered_1.get_experiment(),
	level_7_kind_3_8_offered_2.get_experiment(),
	qap.get_experiment()
]	 
