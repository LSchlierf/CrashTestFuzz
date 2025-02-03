############################
# GENERAL TEST INFORMATION #
############################

SUT = "umbra"
DEBUG_LEVEL = 0
CONCURRENT_TESTS = 10
DB_TABLENAME = "lazytest"

#####################
# CRASH INFORMATION #
#####################

FILE = "umbra.db.wal"
OP = "fsync"
TIMING = "after"
STEPS = 20

##########################
# WORKFLOW SPECIFICATION #
##########################

NUM_TRANSACTIONS = 100
CONCURRENT_TRANSACTIONS = (3.0, 1.0)
TRANSACTION_SIZE = (5.0, 1.0)
STATEMENT_SIZE = (100.0, 5.0)
P_COMMIT = 0.8
P_INSERT = 0.7
P_UPDATE = 0.2
P_SERIALIZATION_FAILURE = 0.01
