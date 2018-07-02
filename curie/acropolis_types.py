#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
from curie.json_util import Field, RepeatedField, JsonData


class AcropolisTaskEntity(JsonData):
  entity_name = Field(str, default="")
  entity_type = Field(str, default="")
  uuid = Field(str, required=True)


class AcropolisTaskMetaResponse(JsonData):
  error = Field(str, default="")
  error_detail = Field(str, default="")


class AcropolisTaskInfo(JsonData):
  progress_status = Field(str, default="Unknown")
  percentage_complete = Field(int, default=0)
  message = Field(str, default="")
  meta_response = Field(AcropolisTaskMetaResponse,
                        default=AcropolisTaskMetaResponse())
  operation_type = Field(str, default="")
  entity_list = RepeatedField(AcropolisTaskEntity)
  uuid = Field(str, default="")
  create_time = Field(int, default=-1)
