from fastapi import Depends
from sqlalchemy import and_, update, desc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models import Base
from app.api import deps
from sqlalchemy.dialects.postgresql import insert

from app.core.database_session import _ASYNC_ENGINE, SessionFactory

from app.schemas.logger import logger

# Update KPI to Dtabase
# columns_data = {"kpi_area": "ESG","kpi_code": "ESG1C", "kpi_flag": False,"kpi_value": None,"kpi_details": ""}
# async def update_dynamic_ens_data(table_name: str, columns_data: dict, ens_id: str):


async def get_dynamic_ens_data(
    table_name: str,
    required_columns: list,
    ens_id: str,
    session_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        session = SessionFactory()
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(
                f"Table '{table_name}' does not exist in the database schema."
            )

        # If "*" is passed, select all columns
        if required_columns == ["all"]:
            columns_to_select = [table_class.c[column] for column in table_class.c.keys()]
        else:
            columns_to_select = [getattr(table_class.c, column) for column in required_columns]

        query = select(*columns_to_select)

        if ens_id:
            query = query.where(table_class.c.ens_id == str(ens_id)).distinct()
        
        if session_id:
            query = query.where(table_class.c.session_id == str(session_id))

        result = await session.execute(query)

        columns = result.keys()
        rows = result.all()

        formatted_res = [
            dict(zip(columns, row)) for row in rows
        ]

        await session.close()
        return formatted_res

    except ValueError as ve:
        print(f"Error: {ve}")
        return []

    except SQLAlchemyError as sa_err:
        print(f"Database error: {sa_err}")
        return []

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []



async def update_dynamic_ens_data(
    table_name: str,
    columns_data: dict,
    ens_id: str,
    session_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    """
    Update the specified table dynamically with the provided columns_data based on ens_id.

    :param session: AsyncSession = Depends(deps.get_session) - The database session.
    :param table_name: str - The name of the table to update.
    :param columns_data: dict - The dictionary of KPI data to update.
    :param ens_id: str - The ID to filter the record that needs to be updated.
    :return: dict - The result of the update operation.
    """
    try:
        session = SessionFactory()

        # Get the table class dynamically
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")
        
        # Prepare the update values
        update_values = {key: value for key, value in columns_data.items() if value is not None}
        

        # Initialize the base update query
        query = update(table_class).values(update_values)

        # Define conditions dynamically
        conditions = []
        if ens_id:
            conditions.append(table_class.c.ens_id == str(ens_id))
        if session_id:
            conditions.append(table_class.c.session_id == str(session_id))

        # Apply conditions if there are any
        if conditions:
            query = query.where(and_(*conditions))
        
        # Execute the query
        result = await session.execute(query)
        
        # Commit the transaction
        await session.commit()
        
        # Return success response
        await session.close()
        return {"status": "success", "message": "Data updated successfully."}

    except ValueError as ve:
        # Handle the case where the table does not exist
        print(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}
    
    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        print(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}
    
    except Exception as e:
        # Catch any other exceptions
        print(f"An unexpected error occurred: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}

async def insert_dynamic_ens_data(
    table_name: str,
    columns_data: list,
    ens_id: str,
    session_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        session = SessionFactory()

        # Get the table class dynamically
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")
        
        # Add `ens_id` and `session_id` to each row in `columns_data`
        rows_to_insert = [
            {**row, "ens_id": ens_id, "session_id": session_id}
            for row in columns_data
        ]
        
        # Build the insert query
        query = insert(table_class).values(rows_to_insert)
        
        # Execute the query
        await session.execute(query)
        
        # Commit the transaction
        await session.commit()
        
        # Return success response
        await session.close()
        return {"status": "success", "message": f"Inserted {len(rows_to_insert)} rows successfully."}

    except ValueError as ve:
        # Handle the case where the table does not exist
        print(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}
    
    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        print(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}
    
    except Exception as e:
        # Catch any other exceptions
        print(f"An unexpected error occurred: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}

async def upsert_dynamic_ens_data(
    table_name: str,
    columns_data: list,
    ens_id: str,
    session_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        session = SessionFactory()

        # Get the table class dynamically
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")

        # Add `ens_id` and `session_id` to each row in `columns_data`
        rows_to_insert = [
            {**row, "ens_id": ens_id, "session_id": session_id}
            for row in columns_data
        ]

        # Build the UPSERT query (Insert with conflict handling)
        query = insert(table_class).values(rows_to_insert).on_conflict_do_update(
            index_elements=["ens_id", "session_id"],  # Conflict columns
            set_={col: getattr(insert(table_class).excluded, col) for col in rows_to_insert[0].keys() if col not in ["ens_id", "session_id"]}
        )

        # Execute the query
        await session.execute(query)

        # Commit the transaction
        await session.commit()
        await session.close()

        return {"status": "success", "message": f"Upserted {len(rows_to_insert)} rows successfully."}

    except ValueError as ve:
        print(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}

    except SQLAlchemyError as sa_err:
        print(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}

async def upsert_dynamic_ens_data_summary(
    table_name: str,
    columns_data: list,
    ens_id: str,
    session_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        session = SessionFactory()

        # Get the table class dynamically
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")

        # Add `ens_id` and `session_id` to each row in `columns_data`
        rows_to_insert = [
            {**row, "ens_id": ens_id, "session_id": session_id}
            for row in columns_data
        ]

        # Build the UPSERT query (Insert with conflict handling)
        query = insert(table_class).values(rows_to_insert).on_conflict_do_update(
            index_elements=["ens_id", "session_id", "area"],  # Conflict columns
            set_={col: getattr(insert(table_class).excluded, col) for col in rows_to_insert[0].keys() if col not in ["ens_id", "session_id", "area"]}
        )

        # Execute the query
        await session.execute(query)

        # Commit the transaction
        await session.commit()
        await session.close()

        return {"status": "success", "message": f"Upserted {len(rows_to_insert)} rows successfully."}

    except ValueError as ve:
        print(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}

    except SQLAlchemyError as sa_err:
        print(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}

async def upsert_kpi(
    table_name: str,
    columns_data: list,
    ens_id: str,
    session_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        session = SessionFactory()

        # kpi_code
        
        # Get the table class dynamically
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")

        # Add ens_id and session_id to each record
        for record in columns_data:
            record["ens_id"] = ens_id
            record["session_id"] = session_id

        # Extract column names dynamically
        columns = list(columns_data[0].keys())

        # Prepare bulk insert statement using PostgreSQL ON CONFLICT
        stmt = insert(table_class).values(columns_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["ens_id", "session_id", "kpi_code"],  # Unique constraint columns
            set_={col: stmt.excluded[col] for col in columns if col not in ["ens_id", "session_id", "kpi_code"]}
        ).returning(table_class)

        # Execute bulk upsert
        result = await session.execute(stmt)
        await session.commit()

        # Fetch the inserted/updated rows
        await session.close()
        return {"message": "Upsert completed", "data": result.fetchall(), "status": "success"}

    except ValueError as ve:
        # Handle the case where the table does not exist
        print(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}
    
    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        print(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}
    
    except Exception as e:
        # Catch any other exceptions
        print(f"An unexpected error occurred: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}
    


async def upsert_ensid_screening_status(
    columns_data: list,
    session_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        session = SessionFactory()

        # Get the table class dynamically
        table_class = Base.metadata.tables.get('ensid_screening_status')
        if table_class is None:
            raise ValueError(f"Table 'ensid_screening_status' does not exist in the database schema.")

        # Add ens_id and session_id to each record
        for record in columns_data:
            if 'ens_id' not in record:
                raise ValueError(f"Missing 'ens_id' in record: {record}")
            record["session_id"] = session_id

        # Extract column names dynamically
        columns = list(columns_data[0].keys())

        # Prepare bulk insert statement using PostgreSQL ON CONFLICT
        stmt = insert(table_class).values(columns_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=["ens_id", "session_id"],  # Unique constraint columns
            set_={col: stmt.excluded[col] for col in columns if col not in ["ens_id", "session_id"]}
        ).returning(table_class)
        # Execute bulk upsert
        result = await session.execute(stmt)
        await session.commit()

        # Fetch the inserted/updated rows
        await session.close()
        return {"message": "Upsert completed", "data": result.fetchall()}

    except ValueError as ve:
        # Handle the case where the table does not exist
        print(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}
    
    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        print(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}
    
    except Exception as e:
        # Catch any other exceptions
        print(f"An unexpected error occurred: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}
    

async def upsert_session_screening_status(
    columns_data: list,
    session_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    """
    :param columns_data: [{"overall_status":STATUS.COMPLETED}] list of one element TODO we can neaten this up later
    :param session_id:
    :param session:
    :return:
    """
    try:
        session = SessionFactory()

        # Get the table class dynamically
        table_class = Base.metadata.tables.get("session_screening_status")
        if table_class is None:
            raise ValueError(f"Table 'session_screening_status' does not exist in the database schema.")

        # Deduplicate the rows based on session_id
        unique_records = {}
        for record in columns_data:
            record["session_id"] = session_id
            # Use session_id as the key to deduplicate rows
            unique_records[record["session_id"]] = record

        # Convert the dictionary back to a list
        deduplicated_columns_data = list(unique_records.values())

        # Extract column names dynamically
        columns = list(deduplicated_columns_data[0].keys())

        # Prepare bulk insert statement using PostgreSQL ON CONFLICT
        stmt = insert(table_class).values(deduplicated_columns_data)

        # Modify ON CONFLICT to use session_id and update the non-unique fields
        stmt = stmt.on_conflict_do_update(
            index_elements=["session_id"],  # Index on session_id, no unique constraint
            set_={col: stmt.excluded[col] for col in columns if col != "session_id"}
        ).returning(table_class)

        # Execute bulk upsert
        result = await session.execute(stmt)
        await session.commit()

        # Fetch the inserted/updated rows
        await session.close()
        return {"message": "Upsert completed", "data": result.fetchall()}

    except ValueError as ve:
        # Handle the case where the table does not exist
        print(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}
    
    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        print(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}
    
    except Exception as e:
        # Catch any other exceptions
        print(f"An unexpected error occurred: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}


async def get_ensid_screening_status_static(
        ens_id: list,
        session_id: str,
        session: AsyncSession = Depends(deps.get_session)
):
    try:
        session = SessionFactory()

        required_columns = ["id", "session_id", "ens_id", "overall_status","orbis_retrieval_status","screening_modules_status","report_generation_status","create_time","update_time"]
        table_class = Base.metadata.tables.get("ensid_screening_status")
        if table_class is None:
            raise ValueError(
                f"Table ensid_screening_status does not exist in the database schema."
            )

        columns_to_select = [
            getattr(table_class.c, column) for column in required_columns
        ]

        query = select(*columns_to_select)

        # if ens_id:
        #     query = query.filter(table_class.c.ens_id.in(ens_id)).distinct()

        if session_id:
            query = query.where(table_class.c.session_id == str(session_id)).filter(table_class.c.ens_id.in_(ens_id))

        result = await session.execute(query)

        columns = result.keys()

        rows = result.all()

        formatted_res = [
            dict(
                zip(columns, row)
            )  # zip the column names with their corresponding row values
            for row in rows
        ]

        # Return the formatted result
        await session.close()
        return formatted_res

    except ValueError as ve:
        # Handle the case where the table does not exist
        print(f"Error: {ve}")
        return []

    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        print(f"Database error: {sa_err}")
        return []

    except Exception as e:
        # Catch any other exceptions
        print(f"An unexpected error occurred: {e}")
        return []


async def get_session_screening_status_static(
        session_id: str,
        session: AsyncSession = Depends(deps.get_session)
):
    try:
        session = SessionFactory()

        required_columns = ["id", "session_id","overall_status","list_upload_status","supplier_name_validation_status","screening_analysis_status","create_time","update_time"]
        table_class = Base.metadata.tables.get("session_screening_status")
        if table_class is None:
            raise ValueError(
                f"Table session_screening_status does not exist in the database schema."
            )

        columns_to_select = [
            getattr(table_class.c, column) for column in required_columns
        ]

        query = select(*columns_to_select)

        if session_id:
            query = query.where(table_class.c.session_id == str(session_id))

        result = await session.execute(query)

        columns = result.keys()

        rows = result.all()

        formatted_res = [
            dict(
                zip(columns, row)
            )  # zip the column names with their corresponding row values
            for row in rows
        ]

        # Return the formatted result
        await session.close()
        return formatted_res

    except ValueError as ve:
        # Handle the case where the table does not exist
        print(f"Error: {ve}")
        return []

    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        print(f"Database error: {sa_err}")
        return []

    except Exception as e:
        # Catch any other exceptions
        print(f"An unexpected error occurred: {e}")
        return []

async def check_and_update_unique_value(
    table_name: str,
    column_name: str,
    bvd_id_to_check: str,
    ens_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        session = SessionFactory()

        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")

        # Step 1: Check if the value already exists in the column, excluding the current row
        query = select(table_class.c.ens_id).where(
            (getattr(table_class.c, column_name) == bvd_id_to_check) &
            (table_class.c.ens_id != ens_id)  # Exclude the current row
        )
        query = query.order_by(desc(table_class.c.create_time))
        result = await session.execute(query)
        existing_rows = result.scalars().all()

        if existing_rows: # EXISTING ENS-IDs WITH SAME BVDID AS CURRENT ROW

            logger.info("----------- BVDID ALREADY EXISTS")
            latest_ens_id = existing_rows[0]
            logger.info("PRE-EXISTING ENS_ID ----> %s", latest_ens_id)

            # Step 2: If value exists, update pre_existing_bvdid to 'Yes' for all matching rows
            update_query = (
                update(table_class)
                .where(getattr(table_class.c, column_name) == bvd_id_to_check)
                .values(pre_existing_bvdid=True)
            )
            await session.execute(update_query)

            # Step 3: Also update pre_existing_bvdid and replace ens_id with pre-existing ens_id for the current row being updated
            update_self_query = (
                update(table_class)
                .where(table_class.c.ens_id == ens_id)
                .values(pre_existing_bvdid=True, ens_id=latest_ens_id)
            )
            await session.execute(update_self_query)

            await session.commit()
            await session.close()
            return latest_ens_id, {"status": "duplicate", "message": f"Value '{bvd_id_to_check}' already exists. pre_existing_bvdid set to 'Yes'."}

        else:
            # Step 4: If value does not exist, update the column for the given ens_id
            update_query = (
                update(table_class)
                .where(table_class.c.ens_id == ens_id)
                .values({column_name: bvd_id_to_check})
            )
            await session.execute(update_query)
            await session.commit()
            await session.close()
            return ens_id, {"status": "unique", "message": f"Value '{bvd_id_to_check}' successfully updated."}

    except ValueError as ve:
        print(f"Error: {ve}")
        return ens_id, {"error": str(ve), "status": "failure"}

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return ens_id, {"error": "An unexpected error occurred", "status": "failure"}


async def get_ens_ids_for_session_id(
    table_name: str,
    required_columns: list,
    session_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    """

    :param table_name: name of table in database to search
    :param session_id: session_id to pull all rows of that session_id
    :param session: database session object
    :return: list of dicts like this [{"ens_id": "ABC-123"},{"ens_id": "ABZ-122"},
                                        {"ens_id": "ABA-D23"},{"ens_id": "ABD-D23"}] ... etc
    """
    try:
        session = SessionFactory()

        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(
                f"Table '{table_name}' does not exist in the database schema."
            )

        columns_to_select = [
            getattr(table_class.c, column) for column in required_columns
        ]

        query = select(*columns_to_select)

        if session_id:
            query = query.where(table_class.c.session_id == str(session_id)).distinct()

        result = await session.execute(query)

        columns = result.keys()

        rows = result.all()

        formatted_res = [
            dict(
                zip(columns, row)
            )  # zip the column names with their corresponding row values
            for row in rows
        ]

        await session.close()
        # Return the formatted result
        return formatted_res

    except ValueError as ve:
        # Handle the case where the table does not exist
        print(f"Error: {ve}")
        return []

    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        print(f"Database error: {sa_err}")
        return []

    except Exception as e:
        # Catch any other exceptions
        print(str(e))
        print(f"An unexpected error occurred: {e}")
        return []

async def get_all_ensid_screening_status_static(
        session_id: str,
        session: AsyncSession = Depends(deps.get_session)
):
    try:
        session = SessionFactory()

        required_columns = ["session_id", "ens_id", "overall_status","orbis_retrieval_status","screening_modules_status","report_generation_status","create_time","update_time"]
        table_class = Base.metadata.tables.get("ensid_screening_status")
        if table_class is None:
            raise ValueError(
                f"Table ensid_screening_status does not exist in the database schema."
            )

        columns_to_select = [
            getattr(table_class.c, column) for column in required_columns
        ]

        query = select(*columns_to_select)

        # if ens_id:
        #     query = query.filter(table_class.c.ens_id.in(ens_id)).distinct()

        if session_id:
            query = query.where(table_class.c.session_id == str(session_id))

        result = await session.execute(query)

        columns = result.keys()

        rows = result.all()

        formatted_res = [
            dict(
                zip(columns, row)
            )  # zip the column names with their corresponding row values
            for row in rows
        ]

        # Return the formatted result
        await session.close()
        return formatted_res

    except ValueError as ve:
        # Handle the case where the table does not exist
        print(f"Error: {ve}")
        return []

    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        print(f"Database error: {sa_err}")
        return []

    except Exception as e:
        # Catch any other exceptions
        print(f"An unexpected error occurred: {e}")
        return []

async def update_for_ensid_svm_duplication(
        columns_data: dict,
        id: str,
        session_id: str,
        session: AsyncSession = Depends(deps.get_session)
        ):
    try:
        session = SessionFactory()

        # THIS FUNCTION IS ONLY FOR NAME VALIDATION PROCESS
        table_name = "upload_supplier_master_data"

        # Get the table class dynamically
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")

        # Prepare the update values
        update_values = {key: value for key, value in columns_data.items() if value is not None}

        # Initialize the base update query
        query = update(table_class).values(update_values)

        # Define conditions dynamically
        conditions = []
        if id:
            conditions.append(table_class.c.id == id)
        if session_id:
            conditions.append(table_class.c.session_id == str(session_id))

        # Apply conditions if there are any
        if conditions:
            query = query.where(and_(*conditions))

        # Execute the query
        result = await session.execute(query)

        # Commit the transaction
        await session.commit()

        # Return success response
        await session.close()
        return {"status": "success", "message": "Data updated successfully."}

    except ValueError as ve:
        # Handle the case where the table does not exist
        print(f"Error: {ve}")
        return []

    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        print(f"Database error: {sa_err}")
        return []

    except Exception as e:
        # Catch any other exceptions
        print(f"An unexpected error occurred: {e}")
        return []