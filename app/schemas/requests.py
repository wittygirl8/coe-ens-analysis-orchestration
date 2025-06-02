from pydantic import BaseModel, EmailStr


class BaseRequest(BaseModel):
    # may define additional fields or config shared across requests
    pass


class RefreshTokenRequest(BaseRequest):
    refresh_token: str


class UserUpdatePasswordRequest(BaseRequest):
    password: str


class UserCreateRequest(BaseRequest):
    email: EmailStr
    password: str


class AnalysisRequest(BaseRequest):
    """
    Schema for input data required for Phase 1 Analysis.
    """

    session_id: str

class AnalysisRequestSingle(BaseRequest):
    """
    Schema for input data required for Phase 1 Analysis.
    """
    ens_id: str
    session_id: str

class GraphRequest(BaseRequest):
    """
    Schema for input data required for Phase 1 Analysis.
    """
    ens_id: str
    session_id: str



class BulkAnalysisRequest(BaseRequest):
    """
    Schema for handling multiple analysis requests in bulk.
    """
    # TODO should be deprecated
    requests: list[AnalysisRequest]


class StreamingENSIdRequest(BaseRequest):
    """
    Schema for input data required for streaming request for ens_ids (given as a list of ids in req body)
    """

    ens_id_list: list[str]
    session_id: str


class StreamingSessionIdRequest(BaseRequest):
    """
    Schema for input data required for streaming request for session_ids
    """

    session_id: str

class GraphRequest(BaseRequest):
    """
    Schema for input data required for streaming request for session_ids
    """

    client_id: str
    session_id: str