#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#

from curie.curie_error_pb2 import CurieError
from curie.log import CHECK_EQ, CHECK

# Mapping from curie error code to HTTP status code.
ERROR_CODE_HTTP_STATUS_MAP = {
  CurieError.kNoError: 200,
  CurieError.kRetry: 400,
  CurieError.kTimeout: 500,
  CurieError.kInvalidParameter: 400,
  CurieError.kManagementServerApiAuthenticationError: 500,
  CurieError.kManagementServerApiError: 500,
  CurieError.kClusterApiAuthenticationError: 500,
  CurieError.kClusterApiError: 500,
  CurieError.kInternalError: 500,
  CurieError.kOobAuthenticationError: 500,
  CurieError.kAuthenticationError: 401
}

CHECK_EQ(len(ERROR_CODE_HTTP_STATUS_MAP), CurieError.kNumErrors)


def curie_error_to_http_status(error_code):
  """
  Returns an HTTP status code corresponding to the curie error code
  'error_code'.
  """
  CHECK(error_code in ERROR_CODE_HTTP_STATUS_MAP, msg=error_code)
  return ERROR_CODE_HTTP_STATUS_MAP[error_code]
