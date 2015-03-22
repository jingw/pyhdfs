"""
Convert a JSON schema from the WebHDFS docs into a class

https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#ContentSummary_JSON_Schema
"""
from __future__ import absolute_import, print_function, unicode_literals
import sys

import simplejson as json


TYPE_MAPPING = {
    'integer': 'int',
    'string': 'str',
}


def main():
    js = json.loads(sys.stdin.read())
    name = js['name']
    print('class {}(_BoilerplateClass):'.format(js['name']))
    print('    """')
    for k, v in js['properties'][name]['properties'].items():
        print('    :param {}: {}'.format(k, v['description']))
        print('    :type {}: {}'.format(k, TYPE_MAPPING.get(v['type'], v['type'])))
    print('    """')


if __name__ == '__main__':
    main()
