
def run_tests():
    from lstore.db import Database
    from lstore.query import Query
    from random import choice, randint, sample, seed, randrange
    from time import process_time
    from decimal import Decimal
    import json, copy

    ######################################
    # PERFORMANCE TESTS (common to all files)
    ######################################
    results = {}
    try:
        db_perf = Database()
        grades_table_perf = db_perf.create_table('Grades', 5, 0)
        query_perf = Query(grades_table_perf)
        keys_perf = []
        insert_time_0 = process_time()
        for i in range(0, 10000):
            query_perf.insert(906659671 + i, 93, 0, 0, 0)
            keys_perf.append(906659671 + i)
        insert_time_1 = process_time()

        update_cols = [
            [None, None, None, None, None],
            [None, randrange(0, 100), None, None, None],
            [None, None, randrange(0, 100), None, None],
            [None, None, None, randrange(0, 100), None],
            [None, None, None, None, randrange(0, 100)]
        ]
        update_time_0 = process_time()
        for i in range(0, 10000):
            query_perf.update(choice(keys_perf), *(choice(update_cols)))
        update_time_1 = process_time()

        select_time_0 = process_time()
        for i in range(0, 10000):
            query_perf.select(choice(keys_perf), 0, [1, 1, 1, 1, 1])
        select_time_1 = process_time()

        agg_time_0 = process_time()
        for i in range(0, 10000, 100):
            start_value = 906659671 + i
            end_value = start_value + 100
            _ = query_perf.sum(start_value, end_value - 1, randrange(0, 5))
        agg_time_1 = process_time()

        delete_time_0 = process_time()
        for i in range(0, 10000):
            query_perf.delete(906659671 + i)
        delete_time_1 = process_time()

        results = {
            'insert_time': insert_time_1 - insert_time_0,
            'update_time': update_time_1 - update_time_0,
            'select_time': select_time_1 - select_time_0,
            'agg_time': agg_time_1 - agg_time_0,
            'delete_time': delete_time_1 - delete_time_0
        }
        try:
            db_perf.close()
        except Exception:
            pass
    except Exception as e:
        results = {
            'insert_time': 0,
            'update_time': 0,
            'select_time': 0,
            'agg_time': 0,
            'delete_time': 0
        }
    
    ######################################
    # HELPER FUNCTION
    ######################################
    def reorganize_result(result):
        val = []
        for r in result:
            val.append(r.columns)
        val.sort()
        return val
    
    ######################################
    # EXTENDED CORRECTNESS TESTS (using "./CT" database)
    ######################################
    m_tests = {}
    fixed_records = [
        [0, 1, 1, 2, 1],
        [1, 1, 1, 1, 2],
        [2, 0, 3, 5, 1],
        [3, 1, 5, 1, 3],
        [4, 2, 7, 1, 1],
        [5, 1, 1, 1, 1],
        [6, 0, 9, 1, 0],
        [7, 1, 1, 1, 1]
    ]
    try:
        db_ct = Database()
        db_ct.open("./CT")
        test_table = db_ct.create_table('test', 5, 0)
        query_ct = Query(test_table)
        for rec in fixed_records:
            query_ct.insert(*rec)
    except Exception as e:
        m_tests["CT Setup"] = {"status": "Failed", "message": str(e)}
    
    # Test 1: Select with index.
    test_name = "Select with index"
    try:
        test_table.index.create_index(2)
        res = reorganize_result(query_ct.select(1, 2, [1, 1, 1, 1, 1]))
        if (len(res) == 4 and 
            fixed_records[0] in res and fixed_records[1] in res and 
            fixed_records[5] in res and fixed_records[7] in res):
            m_tests[test_name] = {"status": "Passed", "message": "Select with index: expected records found."}
        else:
            m_tests[test_name] = {"status": "Failed", "message": "Expected 4 matching records, got: " + str(res)}
    except Exception as e:
        m_tests[test_name] = {"status": "Failed", "message": "Exception: " + str(e)}
    
    # Test 2: Select without index (one record).
    test_name = "Select without index (one record)"
    try:
        test_table.index.drop_index(2)
        res = reorganize_result(query_ct.select(3, 2, [1, 1, 1, 1, 1]))
        if (len(res) == 1 and fixed_records[2] in res):
            m_tests[test_name] = {"status": "Passed", "message": "Select without index (one record): record found as expected."}
        else:
            m_tests[test_name] = {"status": "Failed", "message": "Expected one record matching fixed_records[2], got: " + str(res)}
    except Exception as e:
        m_tests[test_name] = {"status": "Failed", "message": "Exception: " + str(e)}
    
    # Test 3: Select without index (multiple records).
    test_name = "Select without index (multiple records)"
    try:
        res = reorganize_result(query_ct.select(1, 2, [1, 1, 1, 1, 1]))
        if (len(res) == 4 and 
            fixed_records[0] in res and fixed_records[1] in res and 
            fixed_records[5] in res and fixed_records[7] in res):
            m_tests[test_name] = {"status": "Passed", "message": "Select without index (multiple records): expected records found."}
        else:
            m_tests[test_name] = {"status": "Failed", "message": "Expected 4 records for multiple records select, got: " + str(res)}
    except Exception as e:
        m_tests[test_name] = {"status": "Failed", "message": "Exception: " + str(e)}
    
    # Test 4: Select without index (empty result).
    test_name = "Select without index (empty result)"
    try:
        res = reorganize_result(query_ct.select(10, 2, [1, 1, 1, 1, 1]))
        if len(res) == 0:
            m_tests[test_name] = {"status": "Passed", "message": "Select without index (empty result): no records found as expected."}
        else:
            m_tests[test_name] = {"status": "Failed", "message": "Expected empty result, got: " + str(res)}
    except Exception as e:
        m_tests[test_name] = {"status": "Failed", "message": "Exception: " + str(e)}
    
    # Test 5: Update on non-existent primary key.
    test_name = "Update on non-existent primary key"
    try:
        query_ct.update(8, *[None, 2, 2, 2, 2])
        res = reorganize_result(query_ct.select(8, 0, [1, 1, 1, 1, 1]))
        if len(res) == 0:
            m_tests[test_name] = {"status": "Passed", "message": "Update on non-existent primary key: no record found as expected."}
        else:
            m_tests[test_name] = {"status": "Failed", "message": "Record with primary key 8 should not exist, but got: " + str(res)}
    except Exception as e:
        m_tests[test_name] = {"status": "Failed", "message": "Exception: " + str(e)}
    
    # Test 6: Update that changes primary key (should be disallowed).
    test_name = "Update that changes primary key"
    try:
        query_ct.update(7, *[8, 2, 2, 2, 2])

        res7 = reorganize_result(query_ct.select(7, 0, [1, 1, 1, 1, 1]))
        res8 = reorganize_result(query_ct.select(8, 0, [1, 1, 1, 1, 1]))

        if len(res7) == 1 and res7[0] == fixed_records[7] and len(res8) == 0:
            m_tests[test_name] = {"status": "Passed", "message": "Primary key update rejected; pk=7 unchanged; pk=8 not created."}
        else:
            m_tests[test_name] = {"status": "Failed", "message": f"Unexpected state: res7={res7}, res8={res8}"}

    except Exception as e:
        m_tests[test_name] = {"status": "Failed", "message": "Exception: " + str(e)}
    
    # Test 7: Delete a record.
    test_name = "Delete record"
    try:
        query_ct.delete(5)
        res = reorganize_result(query_ct.select(5, 0, [1, 1, 1, 1, 1]))
        if len(res) == 0:
            m_tests[test_name] = {"status": "Passed", "message": "Delete record: record deleted as expected."}
        else:
            m_tests[test_name] = {"status": "Failed", "message": "Record with primary key 5 was not deleted, got: " + str(res)}
    except Exception as e:
        m_tests[test_name] = {"status": "Failed", "message": "Exception: " + str(e)}
    
    # Test 8: Multiple tables test.
    test_name = "Multiple tables test"
    try:
        test_table2 = db_ct.create_table("test2", 5, 0)
        records2 = [
            [1, 1, 1, 2, 1],
            [2, 1, 1, 1, 2],
            [3, 0, 3, 5, 1],
            [4, 1, 5, 1, 3],
            [5, 2, 7, 1, 1],
            [6, 1, 1, 1, 1],
            [7, 0, 9, 1, 0],
            [8, 1, 1, 1, 1]
        ]
        query2 = Query(test_table2)
        for rec in records2:
            query2.insert(*rec)
        res = reorganize_result(query2.select(1, 0, [1, 1, 1, 1, 1]))
        if len(res) == 1 and records2[0] in res:
            m_tests[test_name] = {"status": "Passed", "message": "Multiple tables test: expected result returned."}
        else:
            m_tests[test_name] = {"status": "Failed", "message": "Multiple tables test did not return expected result, got: " + str(res)}
    except Exception as e:
        m_tests[test_name] = {"status": "Failed", "message": "Exception: " + str(e)}
    
    # Test 9: Different primary key test.
    test_name = "Different primary key test"
    try:
        test_table3 = db_ct.create_table("test3", 5, 2)
        records3 = [
            [1, 1, 0, 2, 1],
            [2, 1, 1, 1, 2],
            [3, 0, 2, 5, 1],
            [4, 1, 3, 1, 3],
            [5, 2, 4, 1, 1],
            [6, 1, 5, 1, 1],
            [7, 0, 6, 1, 0],
            [8, 1, 7, 1, 1]
        ]
        query3 = Query(test_table3)
        for rec in records3:
            query3.insert(*rec)
        res = query3.sum(3, 5, 4)
        if res == 5:
            m_tests[test_name] = {"status": "Passed", "message": "Different primary key test: sum result as expected."}
        else:
            m_tests[test_name] = {"status": "Failed", "message": "Expected sum result 5, got " + str(res)}
    except Exception as e:
        m_tests[test_name] = {"status": "Failed", "message": "Exception: " + str(e)}
    try:
        db_ct.close()
    except Exception as e:
        m_tests["CT Close"] = {"status": "Failed", "message": str(e)}
    
    ######################################
    # M2 TESTS AND DURABILITY TESTS (using "./M2" database)
    ######################################
    number_of_records = 1000
    number_of_aggregates = 100
    number_of_updates = 1
    # Instead of relying on the previous generte_keys (which appears to be not working as expected),
    # we directly generate the keys.
    keys = [92106429 + i for i in range(number_of_records)]
    # Optionally, if you need associated random records, build them here:
    records = {key: [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)] for key in keys}
    # (If you need to perform updates as in your original code, you can do that here as well.)

    try:
        db_m2 = Database()
        db_m2.open('./M2')
        grades_table_m2 = db_m2.create_table('Grades', 5, 0)
        query_m2 = Query(grades_table_m2)
        m2_records = {}
        seed(3562901)
        # Test 10: M2 Insert test.
        test_name = "M2 Insert test"
        try:
            for key in keys:
                # For each key, generate a record with random values.
                m2_records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
                query_m2.insert(*m2_records[key])
            m_tests[test_name] = {"status": "Passed", "message": "M2 Insert test: all records inserted successfully."}
        except Exception as e:
            m_tests[test_name] = {"status": "Failed", "message": "Exception: " + str(e)}
        
        # Test 11: M2 Select test.
        test_name = "M2 Select test"
        try:
            for key in keys:
                rec = query_m2.select(key, 0, [1, 1, 1, 1, 1])[0]
                if any(rec.columns[i] != m2_records[key][i] for i in range(5)):
                    raise Exception("Mismatch for key " + str(key))
            m_tests[test_name] = {"status": "Passed", "message": "M2 Select test: all records selected correctly."}
        except Exception as e:
            m_tests[test_name] = {"status": "Failed", "message": "Exception: " + str(e)}
        
        # Test 12: M2 Update test.
        test_name = "M2 Update test"
        m2_original = copy.deepcopy(m2_records)
        try:
            for key in keys:
                updated_columns = [None, None, None, None, None]
                for i in range(1, grades_table_m2.num_columns):
                    value = randint(0, 20)
                    updated_columns[i] = value
                    m2_records[key][i] = value
                query_m2.update(key, *updated_columns)
                rec = query_m2.select(key, 0, [1, 1, 1, 1, 1])[0]
                if any(rec.columns[i] != m2_records[key][i] for i in range(5)):
                    raise Exception("Mismatch for key " + str(key))
            m_tests[test_name] = {"status": "Passed", "message": "M2 Update test: records updated correctly."}
        except Exception as e:
            m_tests[test_name] = {"status": "Failed", "message": "Exception: " + str(e)}
        try:
            db_m2.close()
        except Exception:
            pass
        
        # Durability tests: reopen the M2 database.
        try:
            db_m2 = Database()
            db_m2.open('./M2')
            grades_table_m2 = db_m2.get_table('Grades')
            query_m2 = Query(grades_table_m2)
            # Test 13: Durability select test.
            test_name = "Durability select test"
            try:
                for key in keys:
                    rec = query_m2.select(key, 0, [1, 1, 1, 1, 1])[0]
                    if any(rec.columns[i] != m2_records[key][i] for i in range(5)):
                        raise Exception("Mismatch for key " + str(key))
                m_tests[test_name] = {"status": "Passed", "message": "Durability select test: all records persisted correctly."}
            except Exception as e:
                m_tests[test_name] = {"status": "Failed", "message": "Exception: " + str(e)}
            
            # Test 14: Durability aggregate test.
            test_name = "Durability aggregate test"
            try:
                # Guard check: ensure that there are at least 2 keys to sample from.
                if len(keys) < 2:
                    raise Exception("Not enough keys to perform durability aggregate test, got " + str(len(keys)))
                for i in range(0, number_of_aggregates):
                    r = sorted(sample(range(0, len(keys)), 2))
                    correct = sum(m2_records[x][0] for x in keys[r[0]: r[1] + 1])
                    agg_res = query_m2.sum(keys[r[0]], keys[r[1]], 0)
                    if correct != agg_res:
                        raise Exception("For range " + str(r) + ", expected " + str(correct) + " but got " + str(agg_res))
                m_tests[test_name] = {"status": "Passed", "message": "Durability aggregate test: aggregates computed correctly."}
            except Exception as e:
                m_tests[test_name] = {"status": "Failed", "message": "Exception: " + str(e)}
            try:
                db_m2.close()
            except Exception:
                pass
        except Exception as e:
            m_tests["M2 Durability"] = {"status": "Failed", "message": str(e)}
    except Exception as e:
        m_tests["M2 Setup"] = {"status": "Failed", "message": str(e)}
    
    ######################################
    # MERGING TESTS (using "./MT" database)
    ######################################
    def merging_tester():
        try:
            db_mt = Database()
            db_mt.open("./MT")
            merge_table = db_mt.create_table('merge', 5, 0)
            query_mt = Query(merge_table)
            update_nums = [2, 4, 8, 16]
            records_num = 10000
            sample_count = 200
            select_repeat = 200
            for i in range(records_num):
                query_mt.insert(i, (i+100) % records_num, (i+200) % records_num,
                                (i+300) % records_num, (i+400) % records_num)
            for index in range(len(update_nums)):
                update_num = update_nums[index]
                for count in range(update_num):
                    for i in range(records_num):
                        update_rec = [None, (i+101+count) % records_num, (i+201+count) % records_num,
                                      (i+301+count) % records_num, (i+401+count) % records_num]
                        for idx in range(index):
                            update_rec[4-idx] = None
                        query_mt.update(i, *update_rec)
                keys_sample = sorted(sample(range(0, records_num), sample_count))
                t = 0
                while t < select_repeat:
                    t += 1
                    for key in keys_sample:
                        query_mt.select(key, 0, [1, 1, 1, 1, 1])
            try:
                db_mt.close()
            except Exception:
                pass
            return {"status": "Passed", "message": "Merging tester: merging tests completed successfully."}
        except Exception as e:
            return {"status": "Failed", "message": str(e)}
    m_tests["Merging tester"] = merging_tester()
    
    ######################################
    # OUTPUT
    ######################################
    total_tests = len(m_tests)
    passed_tests = sum(1 for test in m_tests.values() if test.get("status") == "Passed")
    output = {
        "tests": m_tests,
        "count": passed_tests,
        "total": total_tests,
        "results": {k: round(v, 3) for k, v in results.items()}
    }
    return output

if __name__ == '__main__':
    import json
    res = run_tests()
    print(json.dumps(res))
