#-*- coding: utf-8 -*-
import logging
import random

from kvstore import DBMStore, InMemoryKVStore

LOG_LEVEL = logging.WARNING

KVSTORE_CLASS = InMemoryKVStore

"""
Possible abort modes.
"""
USER = 0
DEADLOCK = 1

"""
Part I: Implementing request handling methods for the transaction handler

The transaction handler has access to the following objects:

self._lock_table: the global lock table. More information in the README.

self._acquired_locks: a list of locks acquired by the transaction. Used to
release locks when the transaction commits or aborts. This list is initially
empty.

self._desired_lock: the lock that the transaction is waiting to acquire as well
as the operation to perform. This is initialized to None.

self._xid: this transaction's ID. You may assume each transaction is assigned a
unique transaction ID.

self._store: the in-memory key-value store. You may refer to kvstore.py for
methods supported by the store.

self._undo_log: a list of undo operations to be performed when the transaction
is aborted. The undo operation is a tuple of the form (@key, @value). This list
is initially empty.

You may assume that the key/value inputs to these methods are already type-
checked and are valid.
"""

class TransactionHandler:

    def __init__(self, lock_table, xid, store):
        self._lock_table = lock_table
        self._acquired_locks = []
        self._desired_lock = None
        self._xid = xid
        self._store = store
        self._undo_log = []

    def perform_put(self, key, value):


        """
        Handles the PUT request. You should first implement the logic for
        acquiring the exclusive lock. If the transaction can successfully
        acquire the lock associated with the key, insert the key-value pair
        into the store.

        Hint: if the lock table does not contain the key entry yet, you should
        create one.
        Hint: be aware that lock upgrade may happen.
        Hint: remember to update self._undo_log so that we can undo all the
        changes if the transaction later gets aborted. See the code in abort()
        for the exact format.

        @param self: the transaction handler.
        @param key, value: the key-value pair to be inserted into the store.

        @return: if the transaction successfully acquires the lock and performs
        the insertion/update, returns 'Success'. If the transaction cannot
        acquire the lock, returns None, and saves the lock that the transaction
        is waiting to acquire in self._desired_lock.
        """
        # Part 1.1: your code here!
        xid = self._xid
        if not self._lock_table.has_key(key):
            q = []
            granted_group = []
            mode = "X"
            self._lock_table[key] = (granted_group, q, mode)

        if (self._lock_table[key][0] == []) or self._lock_table[key][0] == [[xid, "S"]]:
            self._acquired_locks.append([key, "X"])
            granted_group = [[xid, "X"]]
            q = self._lock_table[key][1]
            self._lock_table[key] = (granted_group, q, "X")
            for i in range(len(self._acquired_locks)):
                    if self._acquired_locks[i][0] == key:
                        self._acquired_locks.pop(i)
                        break

            self._undo_log.append((key, self._store.get(key)))
            self._store.put(key, value)
            return 'Success'
        elif len(self._lock_table[key][0]) > 1 and [xid, "S"] in self._lock_table[key][0]:
            
            self._desired_lock =[key, "X", value]
            granted_group = self._lock_table[key][0]
            granted_group.remove([xid, "S"])
            q = self._lock_table[key][1]
            q.insert(0, [xid, "X", value])
            self._lock_table[key] = (granted_group, q, "S")

        else:
            self._desired_lock = [key, "X", value]
            q = self._lock_table[key][1]
            q.append([xid, "X", value])
            granted_group = self._lock_table[key][0]
            mode = self._lock_table[key][2]
            self._lock_table[key] = (granted_group, q, mode)
            return None

    def helper(self, xid, key):
        for item in self._lock_table[key][0]:
            if item[0] == xid and item[1] == "X":
                return True
        return False

    def perform_get(self, key):
        """
        Handles the GET request. You should first implement the logic for
        acquiring the shared lock. If the transaction can successfully acquire
        the lock associated with the key, read the value from the store.

        Hint: if the lock table does not contain the key entry yet, you should
        create one.

        @param self: the transaction handler.
        @param key: the key to look up from the store.

        @return: if the transaction successfully acquires the lock and reads
        the value, returns the value. If the key does not exist, returns 'No
        such key'. If the transaction cannot acquire the lock, returns None,
        and saves the lock that the transaction is waiting to acquire in
        self._desired_lock.
        """
        # Part 1.1: your code here!
        xid = self._xid


        if not self._lock_table.has_key(key):
            q = []
            granted_group = []
            mode = "S"
            self._lock_table[key] = (granted_group, q, mode)

        if self._lock_table[key][0] == []:
            self._acquired_locks += [[key, "S"]]
            granted_group = [[xid, "S"]]
            q = self._lock_table[key][1]
            self._lock_table[key] = (granted_group, q, "S")
            value = self._store.get(key)
            if value is None:
                return 'No such key'
            else:
                return value
        else:
            if  [xid, "S"] in self._lock_table[key][0] or self.helper(xid, key):


                # print 110
                value = self._store.get(key)
                if value is None:
                    return 'No such key'
                else:
                    return value
            else:
                
                if self._lock_table[key][2] == "S":
                    self._acquired_locks += [[key, "S"]]
                    granted_group = self._lock_table[key][0]
                    granted_group.append([xid, "S"])
                    q = self._lock_table[key][1]
                    self._lock_table[key] = (granted_group, q, "S")
                    value = self._store.get(key)
                    if value is None:
                        return 'No such key'
                    else:
                        return value

                else:

                    # print 911
                    self._desired_lock = [key, "S"]
                    q = self._lock_table[key][1]
                    q.append([xid, "S"])
                    granted_group = self._lock_table[key][0]
                    self._lock_table[key] = (granted_group, q, "X")
                    return None



    def release_and_grant_locks(self):
        """
        Releases all locks acquired by the transaction and grants them to the
        next transactions in the queue. This is a helper method that is called
        during transaction commits or aborts. 

        Hint: you can use self._acquired_locks to get a list of locks acquired
        by the transaction.
        Hint: be aware that lock upgrade may happen.

        @param self: the transaction handler.
        """

        xid = self._xid
        for l in self._acquired_locks:
            a = 1
             # Part 1.2: your code here!
            key = l[0]
            mode = l[1]
            # print(self._lock_table[key][0])
            size = len(self._lock_table[key][0])
            # print self._lock_table[key][0]
            q = self._lock_table[key][1]
            granted_group = self._lock_table[key][0]

            if size == 1:
                # print 7777777
                if q == []:
                    granted_group = []
                    mmode = mode
                else:
                    next = q.pop()
                    mmode = next[1]
                    granted_group = [next]
                    
                    while mmode != "X" and q != []:
                        
                        nextmode = q[0][1]
                        if nextmode == "S":
                            next = q.pop()
                            mmode = "S"
                            granted_group += [next]
                        else:
                            mmode = "X"
                self._lock_table[key] = (granted_group, q, mmode)
            else:
                # print 666666666
                # granted_group.remove((xid, "S"))
                granted_group = self._lock_table[key][0]
                # if granted_group != []:
                for i in range(len(granted_group)):
                    # if granted_group[i] != []:
                    if granted_group[i][0] == xid:
                        granted_group.pop(i)
                        break
                self._lock_table[key] = (granted_group, q, "S")

                if len(q) >= 1:

                    if q[0][1] == "X" and [q[0][0], "S"] in granted_group and len(granted_group) >= 2:

                        newid = q[0][0]
                        for i in range(len(granted_group)):
                            if granted_group[i][0] == newid:
                                granted_group.pop(i)
                                break
                        self._lock_table[key] = (granted_group, q, "S")        
                    if q[0][1] == "X" and [q[0][0], "S"] in granted_group and len(granted_group) == 1:
                        granted_group = [[q[0][0], "X"]]
                        q.pop()
                        self._lock_table[key] = (granted_group, q, "X")

                #     for i in range(len(self._acquired_locks)):
                #     if granted_group[i][0] == newid:
                #         granted_group.pop(i)
                #         break



                # self._lock_table[key] = (granted_group, q, "S")



        self._acquired_locks = []

    def commit(self):
        """
        Commits the transaction.

        Note: This method is already implemented for you, and you only need to
        implement the subroutine release_locks().

        @param self: the transaction handler.

        @return: returns 'Transaction Completed'
        """
        self.release_and_grant_locks()
        return 'Transaction Completed'

    def abort(self, mode):
        """
        Aborts the transaction.

        Note: This method is already implemented for you, and you only need to
        implement the subroutine release_locks().

        @param self: the transaction handler.
        @param mode: mode can either be USER or DEADLOCK. If mode == USER, then
        it means that the abort is issued by the transaction itself (user
        abort). If mode == DEADLOCK, then it means that the transaction is
        aborted by the coordinator due to deadlock (deadlock abort).

        @return: if mode == USER, returns 'User Abort'. If mode == DEADLOCK,
        returns 'Deadlock Abort'.
        """
        while (len(self._undo_log) > 0):
            k,v = self._undo_log.pop()
            self._store.put(k, v)
        self.release_and_grant_locks()
        if (mode == USER):
            return 'User Abort'
        else:
            return 'Deadlock Abort'

    def check_lock(self):
        """
        If perform_get() or perform_put() returns None, then the transaction is
        waiting to acquire a lock. This method is called periodically to check
        if the lock has been granted due to commit or abort of other
        transactions. If so, then this method returns the string that would 
        have been returned by perform_get() or perform_put() if the method had
        not been blocked. Otherwise, this method returns None.

        As an example, suppose Joe is trying to perform 'GET a'. If Nisha has an
        exclusive lock on key 'a', then Joe's transaction is blocked, and
        perform_get() returns None. Joe's server handler starts calling
        check_lock(), which keeps returning None. While this is happening, Joe
        waits patiently for the server to return a response. Eventually, Nisha
        decides to commit his transaction, releasing his exclusive lock on 'a'.
        Now, when Joe's server handler calls check_lock(), the transaction
        checks to make sure that the lock has been acquired and returns the
        value of 'a'. The server handler then sends the value back to Joe.

        Hint: self._desired_lock contains the lock that the transaction is
        waiting to acquire.
        Hint: remember to update the self._acquired_locks list if the lock has
        been granted.
        Hint: if the transaction has been granted an exclusive lock due to lock
        upgrade, remember to clean up the self._acquired_locks list.
        Hint: remember to update self._undo_log so that we can undo all the
        changes if the transaction later gets aborted.

        @param self: the transaction handler.

        @return: if the lock has been granted, then returns whatever would be
        returned by perform_get() and perform_put() when the transaction
        successfully acquired the lock. If the lock has not been granted,
        returns None.
        """
        key = self._desired_lock[0]
        mode = self._desired_lock[1]
        granted_group = self._lock_table[key] 

        granted = 0
        for i in granted_group[0]:
            if i[0] == self._xid and i[1] == mode:
                granted = 1
                if i[1] == "X":
                    value = i[2]
                    break

        if granted == 0:
            return None
        else:
            if mode == "S":
                value = self._store.get(key)
                if value is None:
                    return 'No such key'
                else:
                    return value
            else:
                self._acquired_locks.append([key, self._xid, "X"])
                self._undo_log.append((key, self._store.get(key)))
                self._store.put(key, value)
                return 'Success'











"""
Part II: Implement deadlock detection method for the transaction coordinator

The transaction coordinator has access to the following object:

self._lock_table: see description from Part I

"""

class TransactionCoordinator:

    def __init__(self, lock_table):
        self._lock_table = lock_table


    def dfs(self, graph, start):
        visited = []
        stack = [start]
        while stack:
            node = stack.pop()
            if node not in visited:
                visited.append(node)
                # stack.extend([x for x in graph[node] if x not in visited])
                stack.extend([x for x in graph[node]])
            else:
                return [visited, False]
        return [visited, True]

        

    def detect_deadlocks(self):
        """
        Constructs a waits-for graph from the lock table, and runs a cycle
        detection algorithm to determine if a transaction needs to be aborted.
        You may choose which one transaction you plan to abort, as long as your
        choice is deterministic. For example, if transactions 1 and 2 form a
        cycle, you cannot return transaction 1 sometimes and transaction 2 the
        other times.

        This method is called periodically to check if any operations of any
        two transactions conflict. If this is true, the transactions are in
        deadlock - neither can proceed. If there are multiple cycles of
        deadlocked transactions, then this m=f the cycles, until it returns None
        to indicate that there are no more cycles. Afterward, the surviving
        transactions will continue to run as normal.

        Note: in this method, you only need to find and return the xid of a
        transaction that needs to be aborted. You do not have to perform the
        actual abort.

        @param self: the transaction coordinator.

        @return: If there are no cycles in the waits-for graph, returns None.
        Otherwise, returns the xid of a transaction in a cycle.
        """
        lock_table = self._lock_table
        graph = {}
        waiting_list = []
        # modify waiting_list 
        for key in lock_table.keys():
            for q_item in lock_table[key][1]:
                xid = q_item[0]
                if xid not in waiting_list:
                    waiting_list.append(xid)
            for g_item in lock_table[key][0]:
                xid_1 = g_item[0]
                if xid_1 not in waiting_list:
                    waiting_list.append(xid_1)

        # set up graph
        for waiting_item in waiting_list:
            graph[waiting_item] = []

        # add edges
        for key in lock_table.keys():
            for q_item in lock_table[key][1]:
                for granted_item in lock_table[key][0]:
                    if not granted_item[0] in graph[q_item[0]]:
                        graph[q_item[0]].append(granted_item[0]) 

        # run dfs
        for item in waiting_list:
            visited, gg = self.dfs(graph, item)
            if gg == False:
                return random.choice(visited)


        return None






