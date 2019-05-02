#!/usr/bin/env python
import sys
import pytest
import os

if __name__ == "__main__":
    # show output results from every test function
    args = ["-vvxs"]
    # show the message output for skipped and expected failure tests
    if len(sys.argv) > 1:
        args.extend(sys.argv[1:])
    print("pytest arguments: {}".format(args))
    # # compute coverage stats for xpdAcq
    # args.extend(['--cov', 'xpdAcq'])
    # call pytest and exit with the return code from pytest so that
    # travis will fail correctly if tests fail
    for f in os.listdir("xpdan/tests"):
        a = args.copy() + [f"xpdan/tests/{f}"]
        exit_res = pytest.main(a)
    sys.exit(exit_res)
