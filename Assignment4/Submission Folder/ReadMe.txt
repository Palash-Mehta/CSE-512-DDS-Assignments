Name : Palash Mehta
ASU ID : 1217154913

Descriptions of Map and Reduce functions.

Map:
1) This function takes a file as input parameter.
2) Each line in the file is a row of either relation1 or relation2.
3) Each field in the row is split on delimeter comma ",".
4) Field 1 -> relation name
5) Field 2 -> join column
6) <key, value> -> <join column value, row>

Reduce:
1) The <key,value> pairs created in the map is passed as input to the reducer.
2) Two lists are created to store the tuples of relation1 and relation2 respectively.
3) If value of joining column is equal, reducer combines the tuples from both list corresponding to that value.
4) It writes the joined tuple in the output directory. 

Driver:
1) Driver/main function calls the map and reduce funtions and creates a job.
2) Gets the i/p and o/p directory from the i/p arguments.
3) The driver exits when the job is completed.             