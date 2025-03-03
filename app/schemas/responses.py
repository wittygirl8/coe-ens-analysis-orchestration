from typing import Any, Dict
from pydantic import BaseModel, ConfigDict, EmailStr


class BaseResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class AccessTokenResponse(BaseResponse):
    token_type: str = "Bearer"
    access_token: str
    expires_at: int
    refresh_token: str
    refresh_token_expires_at: int


class UserResponse(BaseResponse):
    user_id: str
    email: EmailStr

class AnalysisResult(BaseModel):
    """
    Schema for the result of a single analysis module.
    """

    module: str
    status: str
    result: Dict

class AnalysisResponse(BaseResponse):
    """
    Schema for the Phase 1 Analysis results.
    """ 

    results: AnalysisResult

class ResponseMessage(BaseModel):
    status: str
    data: Dict # data is now a dictionary
    message: str

class BulkAnalysisResult(BaseModel):
    """
    Schema for the result of a bulk analysis.
    """

    entity: str
    analysis_results: list[AnalysisResult]


class BulkAnalysisResponse(BaseResponse):
    """
    Schema for bulk analysis results.
    """

    bulk_results: list[BulkAnalysisResult]

################################
# For report generation testing purpose 
class details(BaseModel):
    # ens_id: str
    session_id: str
    # name: str
    # country: str

class ReportDetails(BaseModel):
    status: str
    data: details  
    message: str

class ReportStatus(BaseModel):
    api_details: dict 
################################

class TriggerTaskResponse(BaseResponse):
    status: bool
    message: str
