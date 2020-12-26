Python Type     	        BSON Type 	    Supported Direction
None 	                    null 	        both
bool 	                    boolean 	    both
int [1] 	                int32 / int64 	py -> bson
long 	                    int64 	        py -> bson
bson.int64.Int64 	        int64 	        both
float 	                    number (real) 	both
string 	                    string 	        py -> bson
unicode 	                string 	        both
list 	                    array 	        both
dict / SON 	                object 	        both
datetime.datetime [2] [3] 	date 	        both
bson.regex.Regex 	        regex 	        both
compiled re [4] 	        regex 	        py -> bson
bson.binary.Binary 	        binary 	        both
bson.objectid.ObjectId 	    oid 	        both
bson.dbref.DBRef 	        dbref 	        both
None 	                    ndefined 	    bson -> py
unicode 	                code 	        bson -> py
bson.code.Code 	            code 	        py -> bson
unicode 	s               ymbol 	        bson -> py
bytes (Python 3) [5] 	    binary 	b       oth

Note that, when using Python 2.x, to save binary data it must be wrapped as an instance of bson.binary.Binary. Otherwise it will be saved as a BSON string and retrieved as unicode. Users of Python 3.x can use the Python bytes type.
[1]	A Python int will be saved as a BSON int32 or BSON int64 depending on its size. A BSON int32 will always decode to a Python int. A BSON int64 will always decode to a Int64.
[2]	datetime.datetime instances will be rounded to the nearest millisecond when saved
[3]	all datetime.datetime instances are treated as naive. clients should always use UTC.
[4]	Regex instances and regular expression objects from re.compile() are both saved as BSON regular expressions. BSON regular expressions are decoded as Regex instances.
[5]	The bytes type from Python 3.x is encoded as BSON binary with subtype 0. In Python 3.x it will be decoded back to bytes. In Python 2.x it will be decoded to an instance of Binary with subtype 0.

fonte: https://pymongo.readthedocs.io/en/stable/api/bson/index.html