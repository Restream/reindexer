import argparse
import json
import re
import sys

import delegator


def get_diff(l1: list, l2: list):
    diff1 = []
    diff2 = []
    for i, ns1 in enumerate(l1):
        ns2 = l2[i]
        if ns1 != ns2:
            diff1.append(ns1)
            diff2.append(ns2)
    return diff1, diff2


def parse_namespace_indexes(row, ns_name):
    json_str = row.split(f"\\NAMESPACES ADD {ns_name} ")[1]
    ns_config = json.loads(json_str)
    indexes = ns_config.get("indexes", [])
    indexes_str = [json.dumps(idx) for idx in indexes]
    return indexes_str


def get_namespace_data_from_file(filename, ns_name):
    """ process data by lines from file """
    items = []
    collecting_data = False
    with open(filename) as f:
        for line in f:
            line = line.strip()

            if f"\\NAMESPACES ADD {ns_name} " in line:
                indexes = parse_namespace_indexes(line, ns_name)
                collecting_data = True

            elif collecting_data and line.startswith(f"\\UPSERT {ns_name} "):
                items.append(line)

            elif collecting_data and line.startswith("-- Dumping namespace"):
                break

    return indexes, items


def get_namespace_data_from_text(text, ns_name):
    indexes = []
    for line in text.split("\n"):
        if f"\\NAMESPACES ADD {ns_name} " in line:
            indexes = parse_namespace_indexes(line, ns_name)

    items_pattern = rf"\\UPSERT {ns_name} {{.*?}}(?=\s*(?:\\UPSERT|\\NAMESPACES|\\META|--|\Z))"
    items = re.findall(items_pattern, text, re.DOTALL)

    return indexes, items


def check_dumps(diffs: list):
    for diff in diffs:
        ns_name = diff["name"]

        expected_indexes, expected_items = get_namespace_data_from_file("frontapi_demo_v2.rxdump", ns_name)

        actual_ns_dump = delegator.run(fr"reindexer_tool --dsn {args.addr}{args.db} --command '\dump {ns_name}'").out
        actual_indexes, actual_items = get_namespace_data_from_text(actual_ns_dump, ns_name)

        # check indexes
        if set(actual_indexes) != set(expected_indexes):
            print("❌ indexes are different")
            print("expected:", expected_indexes)
            print("actual:", actual_indexes)
            return False

        # check items
        if set(actual_items) != set(expected_items):
            print("❌ items are different")
            print("expected:", expected_items)
            print("actual:", actual_items)
            return False

    return True


parser = argparse.ArgumentParser(description="Compare memstats")
parser.add_argument("--expected", type=str)
parser.add_argument("--addr", type=str)
parser.add_argument("--db", type=str)
args = parser.parse_args()

cmd = fr"reindexer_tool --dsn {args.addr}{args.db} --command 'select name, replication.data_hash, replication.data_count from #memstats order by name'"
memstats_actual = delegator.run(cmd).out

actual_json_str = memstats_actual.split("]")[0] + "]"
expected_json_str = args.expected.split("]")[0] + "]"
actual_list = json.loads(actual_json_str)
expected_list = json.loads(expected_json_str)

# get memstats diffs
diff_actual, diff_expected = get_diff(actual_list, expected_list)
if not diff_actual:
    print("✅ memstats was not changed")
else:
    print("⚠️ memstats was changed")
    expected_str = ',\n'.join([json.dumps(item) for item in diff_expected])
    print(f"expected: [{expected_str}]")
    actual_str = ',\n'.join([json.dumps(item) for item in diff_actual])
    print(f"actual: [{actual_str}]")
    if not check_dumps(diff_actual):
        sys.exit(1)
    print("✅ content is the same")
