#!/usr/bin/env python
"""Run the exposure scenarios and report what each one did.

    python test/runTests.py [-v] [namePattern ...]
"""

import argparse
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def collect(module, patterns):
    tests = [(name, func) for name, func in sorted(vars(module).items())
             if name.startswith('test_') and callable(func)]

    if patterns:
        tests = [(name, func) for name, func in tests if any(pattern in name for pattern in patterns)]

    return tests


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('patterns', nargs='*', help='only run scenarios whose name contains this')
    parser.add_argument('-v', '--verbose', action='store_true', help='print the full traceback')
    args = parser.parse_args()

    import testExposure

    tests = collect(testExposure, args.patterns)
    failed = []
    gaps = []

    for name, func in tests:
        label = name[len('test_'):].replace('_', ' ')
        gap = getattr(func, 'knownGap', None)

        try:
            func()
        except Exception as e:
            if gap:
                gaps.append(name)
                print(f'  gap   {label}\n          {gap}')
            else:
                failed.append(name)
                print(f'  FAIL  {label}\n          {type(e).__name__}: {e}')

            if args.verbose:
                traceback.print_exc()
        else:
            if gap:
                failed.append(name)
                print(f'  FIXED {label}\n          known gap now passes, drop the knownGap mark')
            else:
                print(f'  ok    {label}')

    passed = len(tests) - len(failed) - len(gaps)
    print(f'\n{passed} passed, {len(failed)} failed, {len(gaps)} known gaps')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
