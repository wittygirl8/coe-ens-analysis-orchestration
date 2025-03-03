from collections.abc import AsyncGenerator
from typing import Annotated
from fastapi import Depends, HTTPException, status, Security
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.security import APIKeyHeader

from app.api import api_messages
from app.core import database_session
from app.core.security.jwt import verify_jwt_token
from app.models import User, Base

# Accept Bearer Token directly in headers
api_key_header = APIKeyHeader(name="Authorization", auto_error=False)

async def get_session() -> AsyncGenerator[AsyncSession]:
    async with database_session.get_async_session() as session:
        yield session

async def get_current_user(
    authorization: str = Security(api_key_header),
    session: AsyncSession = Depends(get_session),
) -> User:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing token",
        )

    token = authorization.split("Bearer ")[1]  # Extract the actual token

    # Verify the JWT token
    token_payload = verify_jwt_token(token)
    print("token_payload", token_payload)

    if token_payload.sub != 'application_backend':
        table_class = Base.metadata.tables.get("users_table")
        if table_class is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Table 'users_table' does not exist in the database schema."
                )

        # Execute async query to fetch user with matching user_id and user_grp
        query = select(table_class).where(
            table_class.c.user_id == token_payload.sub,  # Match user_id
            table_class.c.user_group == token_payload.ugr  # Match user_grp
        )
        result = await session.execute(query)
        user = result.scalars().first()  # Extract user from result

        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=api_messages.JWT_ERROR_USER_REMOVED,
            )
        
        return user
    else :
         return True
