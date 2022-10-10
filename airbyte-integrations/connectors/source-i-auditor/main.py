#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_i_auditor import SourceIAuditor

if __name__ == "__main__":
    source = SourceIAuditor()
    launch(source, sys.argv[1:])
