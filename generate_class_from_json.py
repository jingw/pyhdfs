"""
Convert a JSON schema from the WebHDFS docs into a class

https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#ContentSummary_JSON_Schema
"""
import sys
from typing import Any
from typing import Dict

import simplejson as json

TYPE_MAPPING = {
    "integer": "int",
    "string": "str",
}


def to_py_type(v: Dict[str, Any]) -> str:
    if "type" in v:
        t = TYPE_MAPPING.get(v["type"], v["type"])
        return t
    if "enum" in v:
        return "str"
    raise AssertionError(v)


def main() -> None:
    js = json.loads(sys.stdin.read())
    name = js["name"]
    print("class {}(_BoilerplateClass):".format(js["name"]))
    print('    """')
    for k, v in js["properties"][name]["properties"].items():
        print("    :param {}: {}".format(k, v.get("description", "")))
        print("    :type {}: {}".format(k, to_py_type(v)))
    print('    """')
    print()
    for k, v in js["properties"][name]["properties"].items():
        print("    {}: {}".format(k, to_py_type(v)))


if __name__ == "__main__":
    main()
